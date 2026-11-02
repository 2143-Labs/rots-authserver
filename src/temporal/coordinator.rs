//! The fleet coordinator: one workflow per task queue (one per environment) that
//! knows which game-server slots are alive, which characters are on them, and which
//! slots were promised to recent `/play` calls.
//!
//! Every rule lives in the pure [`FleetState`] methods, which take `now` (unix
//! seconds) and push presence changes onto an outbox. The workflow handlers only
//! supply `now`, and the run loop delivers the outbox to session workflows in order.
//!
//! Presence is a lease, not a fence: a game server that loses the authserver keeps
//! playing, and after `stale_secs` the coordinator forgets its players, so for that
//! window the same character can be admitted on a second slot. Harmless while
//! characters carry no durable progress; once they do, the session-ending write must
//! be fenced by an epoch issued here.
//!
//! Ceiling: one workflow serializes every fleet event, and a run is capped at
//! [`MAX_HISTORY`] events or [`RUN_SECS`], so this sustains about 1 event/s
//! comfortably. Four servers produce about 0.4 events/s. Past roughly 100 reporting
//! servers, shard by slot range or move presence out of Temporal.
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    ContinueAsNewOptions, SignalWorkflowOptions, SyncWorkflowContext, WorkflowContext,
    WorkflowContextView, WorkflowResult,
};

use super::session::{PlayerSessionWorkflow, PresenceIn};
use super::{now_secs, session_id};

/// How long an assignment holds a slot for a player who has not joined yet: the
/// ticket lifetime plus jsonwebtoken's default 60 s leeway.
pub const RESERVATION_SECS: u64 = crate::tickets::TICKET_TTL_SECS + 60;
/// A run continues-as-new after this long, carrying the fleet state.
pub const RUN_SECS: u64 = 3600;
/// ... or after this many history events, whichever comes first.
pub const MAX_HISTORY: u32 = 2000;
/// Closes the coordinator of a deleted preview (no worker left to continue it).
pub const RUN_TIMEOUT: Duration = Duration::from_secs(3 * 3600);

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct CoordinatorArgs {
    pub task_queue: String,
    pub stale_secs: u64,
    pub carried: Option<FleetState>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct FleetState {
    pub slots: BTreeMap<u32, SlotState>,
    /// character_id -> the slot promised to it by the last `assign`.
    pub reservations: BTreeMap<i64, Reservation>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct SlotState {
    pub boot_id: String,
    pub last_report: u64,
    /// character_id -> sid of the login session that joined it.
    pub players: BTreeMap<i64, String>,
    /// Whether a heartbeat roster has been taken over for this boot. A slot first
    /// seen through a join or leave (for example after the coordinator lost its
    /// state) has not, so its first heartbeat may add players; later ones only remove.
    #[serde(default)]
    pub adopted: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Reservation {
    pub slot: u32,
    pub sid: String,
    pub expires: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct PlayerRef {
    pub character_id: i64,
    pub sid: String,
}

/// Outbox item, delivered to `rots-session-<queue>-<sid>` as a `presence` signal.
/// Per character, so one session may hold two characters on one server.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Presence {
    pub sid: String,
    pub slot: u32,
    pub character_id: i64,
    pub present: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct HeartbeatIn {
    pub slot: u32,
    pub boot_id: String,
    pub players: Vec<PlayerRef>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct JoinIn {
    pub slot: u32,
    pub boot_id: String,
    pub character_id: i64,
    pub sid: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct LeaveIn {
    pub slot: u32,
    pub boot_id: String,
    pub character_id: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct AssignIn {
    pub character_id: i64,
    pub sid: String,
    /// The session's unexpired assignment, or a slot it is already playing on.
    pub preferred: Option<u32>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum JoinOutcome {
    Admitted,
    AlreadyOnOtherServer { slot: u32 },
}

fn presence(sid: String, slot: u32, character_id: i64, present: bool) -> Presence {
    Presence {
        sid,
        slot,
        character_id,
        present,
    }
}

impl FleetState {
    /// The slot a character is recorded on, if any.
    fn slot_of(&self, character_id: i64) -> Option<u32> {
        self.slots
            .iter()
            .find(|(_, s)| s.players.contains_key(&character_id))
            .map(|(slot, _)| *slot)
    }

    /// Drops every slot that has not reported for `stale` seconds (its players get
    /// `present: false`) and every expired reservation. Called first by every event,
    /// so all slots left in `self.slots` are healthy.
    pub fn reap(&mut self, now: u64, stale: u64, out: &mut Vec<Presence>) {
        let dead: Vec<u32> = self
            .slots
            .iter()
            .filter(|(_, s)| now > s.last_report.saturating_add(stale))
            .map(|(slot, _)| *slot)
            .collect();
        for slot in dead {
            if let Some(state) = self.slots.remove(&slot) {
                for (character_id, sid) in state.players {
                    out.push(presence(sid, slot, character_id, false));
                }
            }
        }
        self.reservations.retain(|_, r| r.expires > now);
    }

    /// Records a report from `slot`. A new `boot_id` means the server restarted: its
    /// old players are gone and the slot starts empty and not yet adopted.
    pub fn register(&mut self, slot: u32, boot_id: &str, now: u64, out: &mut Vec<Presence>) {
        if let Some(state) = self.slots.get_mut(&slot) {
            if state.boot_id == boot_id {
                state.last_report = now;
                return;
            }
            for (character_id, sid) in std::mem::take(&mut state.players) {
                out.push(presence(sid, slot, character_id, false));
            }
        }
        self.slots.insert(
            slot,
            SlotState {
                boot_id: boot_id.to_owned(),
                last_report: now,
                players: BTreeMap::new(),
                adopted: false,
            },
        );
    }

    /// Recorded players missing from the roster have left. The first roster for a
    /// boot is also taken over (rebuilding state the coordinator lost), except for
    /// characters already on another healthy slot; after that, players are only ever
    /// added by `join`, because a roster can still list a join the coordinator refused.
    pub fn heartbeat(
        &mut self,
        input: &HeartbeatIn,
        now: u64,
        stale: u64,
        out: &mut Vec<Presence>,
    ) {
        self.reap(now, stale, out);
        self.register(input.slot, &input.boot_id, now, out);
        let roster: BTreeSet<i64> = input.players.iter().map(|p| p.character_id).collect();
        let others: BTreeSet<i64> = self
            .slots
            .iter()
            .filter(|(slot, _)| **slot != input.slot)
            .flat_map(|(_, s)| s.players.keys().copied())
            .collect();
        let Some(state) = self.slots.get_mut(&input.slot) else {
            return;
        };
        let gone: Vec<i64> = state
            .players
            .keys()
            .filter(|c| !roster.contains(c))
            .copied()
            .collect();
        for character_id in gone {
            if let Some(sid) = state.players.remove(&character_id) {
                out.push(presence(sid, input.slot, character_id, false));
            }
        }
        if !state.adopted {
            state.adopted = true;
            for p in &input.players {
                if others.contains(&p.character_id) || state.players.contains_key(&p.character_id) {
                    continue;
                }
                state.players.insert(p.character_id, p.sid.clone());
                out.push(presence(p.sid.clone(), input.slot, p.character_id, true));
            }
        }
    }

    /// Admits the character unless it is on another healthy slot.
    pub fn join(
        &mut self,
        input: &JoinIn,
        now: u64,
        stale: u64,
        out: &mut Vec<Presence>,
    ) -> JoinOutcome {
        self.reap(now, stale, out);
        self.register(input.slot, &input.boot_id, now, out);
        if let Some(slot) = self
            .slot_of(input.character_id)
            .filter(|s| *s != input.slot)
        {
            return JoinOutcome::AlreadyOnOtherServer { slot };
        }
        let Some(state) = self.slots.get_mut(&input.slot) else {
            unreachable!("register just inserted slot {}", input.slot);
        };
        if let Some(old_sid) = state.players.insert(input.character_id, input.sid.clone())
            && old_sid != input.sid
        {
            out.push(presence(old_sid, input.slot, input.character_id, false));
        }
        self.reservations.remove(&input.character_id);
        out.push(presence(
            input.sid.clone(),
            input.slot,
            input.character_id,
            true,
        ));
        JoinOutcome::Admitted
    }

    /// Only the boot that admitted a player can report it leaving; anything else is
    /// a late report from an old process and is ignored.
    pub fn leave(&mut self, input: &LeaveIn, now: u64, stale: u64, out: &mut Vec<Presence>) {
        self.reap(now, stale, out);
        let Some(state) = self.slots.get_mut(&input.slot) else {
            return;
        };
        if state.boot_id != input.boot_id {
            return;
        }
        state.last_report = now;
        if let Some(sid) = state.players.remove(&input.character_id) {
            out.push(presence(sid, input.slot, input.character_id, false));
        }
    }

    /// Picks a healthy slot: where the character already is (no reservation), else
    /// `preferred`, else the least players plus reservations (ties to the lowest
    /// slot). Reserves the pick for [`RESERVATION_SECS`]. `None` when no slot is healthy.
    pub fn assign(
        &mut self,
        input: &AssignIn,
        now: u64,
        stale: u64,
        out: &mut Vec<Presence>,
    ) -> Option<u32> {
        self.reap(now, stale, out);
        if let Some(slot) = self.slot_of(input.character_id) {
            return Some(slot);
        }
        let slot = match input.preferred.filter(|s| self.slots.contains_key(s)) {
            Some(slot) => slot,
            None => {
                self.slots
                    .iter()
                    .map(|(slot, s)| {
                        let reserved = self
                            .reservations
                            .iter()
                            .filter(|(c, r)| r.slot == *slot && **c != input.character_id)
                            .count();
                        (s.players.len() + reserved, *slot)
                    })
                    .min()?
                    .1
            }
        };
        self.reservations.insert(
            input.character_id,
            Reservation {
                slot,
                sid: input.sid.clone(),
                expires: now + RESERVATION_SECS,
            },
        );
        Some(slot)
    }
}

#[workflow]
pub struct CoordinatorWorkflow {
    args: CoordinatorArgs,
    fleet: FleetState,
    outbox: VecDeque<Presence>,
}

#[workflow_methods]
impl CoordinatorWorkflow {
    #[init]
    pub fn new(_ctx: &WorkflowContextView, args: CoordinatorArgs) -> Self {
        let fleet = args.carried.clone().unwrap_or_default();
        Self {
            args: CoordinatorArgs {
                carried: None,
                ..args
            },
            fleet,
            outbox: VecDeque::new(),
        }
    }

    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let deadline = now_secs(ctx.workflow_time()) + RUN_SECS;
        let queue = ctx.state(|s| s.args.task_queue.clone());
        loop {
            // The timer is load-bearing: `wait_condition` never wakes because the clock
            // moved, and nothing changes workflow state while the fleet is idle, so a
            // wait-only loop would never continue-as-new and the run timeout would
            // kill it instead, dropping the carried fleet state.
            let remaining = deadline
                .saturating_sub(now_secs(ctx.workflow_time()))
                .max(1);
            temporalio_sdk::workflows::select! {
                _ = ctx.timer(Duration::from_secs(remaining)) => {}
                _ = ctx.wait_condition(|s: &Self| !s.outbox.is_empty()) => {}
            }
            while let Some(p) = ctx.state_mut(|s| s.outbox.pop_front()) {
                // NotFound = the session ended or never started; nothing to tell.
                let _ = ctx
                    .external_workflow(session_id(&queue, &p.sid), None)
                    .signal(
                        PlayerSessionWorkflow::presence,
                        PresenceIn {
                            slot: p.slot,
                            character_id: p.character_id,
                            present: p.present,
                        },
                        SignalWorkflowOptions::default(),
                    )
                    .await;
            }
            let due = ctx.continue_as_new_suggested()
                || ctx.history_length() > MAX_HISTORY
                || now_secs(ctx.workflow_time()) >= deadline;
            if due && ctx.all_handlers_finished() && ctx.state(|s| s.outbox.is_empty()) {
                let args = ctx.state(|s| CoordinatorArgs {
                    carried: Some(s.fleet.clone()),
                    ..s.args.clone()
                });
                ctx.continue_as_new(
                    args,
                    ContinueAsNewOptions::builder()
                        .run_timeout(RUN_TIMEOUT)
                        .build(),
                )?;
            }
        }
    }

    #[signal]
    pub fn heartbeat(&mut self, ctx: &mut SyncWorkflowContext<Self>, input: HeartbeatIn) {
        let mut out = Vec::new();
        self.fleet.heartbeat(
            &input,
            now_secs(ctx.workflow_time()),
            self.args.stale_secs,
            &mut out,
        );
        self.outbox.extend(out);
    }

    #[update]
    pub fn join(&mut self, ctx: &mut SyncWorkflowContext<Self>, input: JoinIn) -> JoinOutcome {
        let mut out = Vec::new();
        let outcome = self.fleet.join(
            &input,
            now_secs(ctx.workflow_time()),
            self.args.stale_secs,
            &mut out,
        );
        self.outbox.extend(out);
        outcome
    }

    #[signal]
    pub fn leave(&mut self, ctx: &mut SyncWorkflowContext<Self>, input: LeaveIn) {
        let mut out = Vec::new();
        self.fleet.leave(
            &input,
            now_secs(ctx.workflow_time()),
            self.args.stale_secs,
            &mut out,
        );
        self.outbox.extend(out);
    }

    #[update]
    pub fn assign(&mut self, ctx: &mut SyncWorkflowContext<Self>, input: AssignIn) -> Option<u32> {
        let mut out = Vec::new();
        let slot = self.fleet.assign(
            &input,
            now_secs(ctx.workflow_time()),
            self.args.stale_secs,
            &mut out,
        );
        self.outbox.extend(out);
        slot
    }

    #[query]
    pub fn fleet(&self, _ctx: &WorkflowContextView) -> FleetState {
        self.fleet.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STALE: u64 = 30;

    fn hb(slot: u32, boot: &str, players: &[(i64, &str)]) -> HeartbeatIn {
        HeartbeatIn {
            slot,
            boot_id: boot.into(),
            players: players
                .iter()
                .map(|(c, s)| PlayerRef {
                    character_id: *c,
                    sid: (*s).into(),
                })
                .collect(),
        }
    }

    fn join(slot: u32, boot: &str, c: i64, sid: &str) -> JoinIn {
        JoinIn {
            slot,
            boot_id: boot.into(),
            character_id: c,
            sid: sid.into(),
        }
    }

    fn assign(c: i64, sid: &str, preferred: Option<u32>) -> AssignIn {
        AssignIn {
            character_id: c,
            sid: sid.into(),
            preferred,
        }
    }

    fn p(sid: &str, slot: u32, c: i64, present: bool) -> Presence {
        presence(sid.into(), slot, c, present)
    }

    /// Slots 0 and 1 alive at t=100, both adopted, slot 0 holding character 9.
    fn two_slots() -> FleetState {
        let mut f = FleetState::default();
        let mut out = Vec::new();
        f.heartbeat(&hb(0, "a", &[(9, "s9")]), 100, STALE, &mut out);
        f.heartbeat(&hb(1, "b", &[]), 100, STALE, &mut out);
        f
    }

    #[test]
    fn assign_prefers_presence_without_reserving() {
        let mut f = two_slots();
        let mut out = Vec::new();
        assert_eq!(
            f.assign(&assign(9, "s9", Some(1)), 101, STALE, &mut out),
            Some(0)
        );
        assert!(f.reservations.is_empty());
    }

    #[test]
    fn assign_keeps_healthy_preferred_and_skips_stale_preferred() {
        let mut f = two_slots();
        let mut out = Vec::new();
        assert_eq!(
            f.assign(&assign(5, "s5", Some(0)), 101, STALE, &mut out),
            Some(0)
        );
        // Slot 1 stops reporting; slot 0 keeps going.
        f.heartbeat(&hb(0, "a", &[(9, "s9")]), 120, STALE, &mut out);
        assert_eq!(
            f.assign(&assign(6, "s6", Some(1)), 140, STALE, &mut out),
            Some(0)
        );
    }

    #[test]
    fn assign_least_loaded_counts_reservations_and_ties_go_low() {
        let mut f = two_slots();
        let mut out = Vec::new();
        assert_eq!(
            f.assign(&assign(1, "s1", None), 101, STALE, &mut out),
            Some(1)
        );
        // Now both slots carry 1 (a player on 0, a reservation on 1): tie -> lowest.
        assert_eq!(
            f.assign(&assign(2, "s2", None), 101, STALE, &mut out),
            Some(0)
        );
        assert_eq!(
            f.assign(&assign(3, "s3", None), 101, STALE, &mut out),
            Some(1)
        );
        // Re-assigning the same character does not count its own reservation.
        assert_eq!(
            f.assign(&assign(3, "s3", None), 101, STALE, &mut out),
            Some(1)
        );
    }

    #[test]
    fn assign_none_without_healthy_slots() {
        let mut f = two_slots();
        let mut out = Vec::new();
        assert_eq!(f.assign(&assign(1, "s1", None), 200, STALE, &mut out), None);
        assert_eq!(
            FleetState::default().assign(&assign(1, "s1", None), 1, STALE, &mut out),
            None
        );
    }

    #[test]
    fn reservation_expires() {
        let mut f = two_slots();
        let mut out = Vec::new();
        f.assign(&assign(1, "s1", None), 101, STALE, &mut out);
        f.reap(101 + RESERVATION_SECS - 1, u64::MAX, &mut out);
        assert!(f.reservations.contains_key(&1));
        f.reap(101 + RESERVATION_SECS, u64::MAX, &mut out);
        assert!(f.reservations.is_empty());
    }

    #[test]
    fn join_refused_while_on_another_healthy_slot() {
        let mut f = two_slots();
        let mut out = Vec::new();
        assert_eq!(
            f.join(&join(1, "b", 9, "s9"), 101, STALE, &mut out),
            JoinOutcome::AlreadyOnOtherServer { slot: 0 }
        );
        assert!(out.is_empty());
    }

    #[test]
    fn join_after_old_slot_went_stale_reports_the_old_slot_first() {
        let mut f = two_slots();
        let mut out = Vec::new();
        f.heartbeat(&hb(1, "b", &[]), 120, STALE, &mut out);
        assert_eq!(
            f.join(&join(1, "b", 9, "s9b"), 140, STALE, &mut out),
            JoinOutcome::Admitted
        );
        assert_eq!(out, vec![p("s9", 0, 9, false), p("s9b", 1, 9, true)]);
        assert!(!f.slots.contains_key(&0));
    }

    #[test]
    fn join_clears_reservation() {
        let mut f = two_slots();
        let mut out = Vec::new();
        f.assign(&assign(1, "s1", None), 101, STALE, &mut out);
        f.join(&join(1, "b", 1, "s1"), 102, STALE, &mut out);
        assert!(f.reservations.is_empty());
    }

    #[test]
    fn new_boot_drops_old_players() {
        let mut f = two_slots();
        let mut out = Vec::new();
        f.heartbeat(&hb(0, "a2", &[]), 101, STALE, &mut out);
        assert_eq!(out, vec![p("s9", 0, 9, false)]);
        assert!(f.slots[&0].players.is_empty());
    }

    #[test]
    fn first_heartbeat_adopts_roster() {
        let mut f = FleetState::default();
        let mut out = Vec::new();
        f.heartbeat(&hb(2, "c", &[(1, "s1"), (2, "s2")]), 10, STALE, &mut out);
        assert_eq!(out, vec![p("s1", 2, 1, true), p("s2", 2, 2, true)]);
        assert!(f.slots[&2].adopted);
    }

    #[test]
    fn adoption_skips_characters_on_other_slots() {
        let mut f = two_slots();
        let mut out = Vec::new();
        f.heartbeat(&hb(3, "d", &[(9, "s9x"), (4, "s4")]), 101, STALE, &mut out);
        assert_eq!(out, vec![p("s4", 3, 4, true)]);
        assert_eq!(f.slot_of(9), Some(0));
    }

    #[test]
    fn slot_first_seen_by_join_is_adopted_by_its_first_heartbeat() {
        let mut f = FleetState::default();
        let mut out = Vec::new();
        f.join(&join(0, "a", 1, "s1"), 10, STALE, &mut out);
        out.clear();
        f.heartbeat(&hb(0, "a", &[(1, "s1"), (2, "s2")]), 11, STALE, &mut out);
        assert_eq!(out, vec![p("s2", 0, 2, true)]);
        // From now on a heartbeat never adds.
        out.clear();
        f.heartbeat(
            &hb(0, "a", &[(1, "s1"), (2, "s2"), (3, "s3")]),
            12,
            STALE,
            &mut out,
        );
        assert!(out.is_empty());
        assert!(!f.slots[&0].players.contains_key(&3));
    }

    #[test]
    fn known_boot_heartbeat_removes_missing_and_never_adds() {
        let mut f = two_slots();
        let mut out = Vec::new();
        f.heartbeat(&hb(0, "a", &[(7, "s7")]), 101, STALE, &mut out);
        assert_eq!(out, vec![p("s9", 0, 9, false)]);
        assert!(f.slots[&0].players.is_empty());
    }

    #[test]
    fn leave_needs_matching_boot() {
        let mut f = two_slots();
        let mut out = Vec::new();
        let leave = |boot: &str| LeaveIn {
            slot: 0,
            boot_id: boot.into(),
            character_id: 9,
        };
        f.leave(&leave("old"), 101, STALE, &mut out);
        assert!(out.is_empty());
        f.leave(&leave("a"), 101, STALE, &mut out);
        assert_eq!(out, vec![p("s9", 0, 9, false)]);
    }

    #[test]
    fn outbox_order_follows_events() {
        let mut f = FleetState::default();
        let mut out = Vec::new();
        f.heartbeat(&hb(0, "a", &[]), 1, STALE, &mut out);
        f.join(&join(0, "a", 1, "s1"), 2, STALE, &mut out);
        f.join(&join(0, "a", 2, "s2"), 3, STALE, &mut out);
        f.leave(
            &LeaveIn {
                slot: 0,
                boot_id: "a".into(),
                character_id: 1,
            },
            4,
            STALE,
            &mut out,
        );
        assert_eq!(
            out,
            vec![
                p("s1", 0, 1, true),
                p("s2", 0, 2, true),
                p("s1", 0, 1, false)
            ]
        );
    }

    #[test]
    fn rejoin_by_another_session_retires_the_old_one() {
        let mut f = two_slots();
        let mut out = Vec::new();
        f.join(&join(0, "a", 9, "s9new"), 101, STALE, &mut out);
        assert_eq!(out, vec![p("s9", 0, 9, false), p("s9new", 0, 9, true)]);
    }
}
