//! One workflow per game-server instance, `rots-gs-<queue>-<slot>-g<generation>`. It
//! owns the instance's lease (renewed by heartbeats), its roster (which characters
//! have units in its world) and its lifecycle: provision, wait for the first
//! heartbeat, serve, drain, delete. It tells the coordinator about lifecycle changes
//! and tells session workflows when their characters join or leave.
//!
//! Managed instances (`GAME_SERVER_PROVISIONER=kubernetes` or `fake`) are started by
//! the coordinator when it spawns one. Static servers (`static`, external processes)
//! start their workflow themselves: the internal API signals-with-start it on every
//! heartbeat, so a static server that went silent and came back gets a new run.
//!
//! The rules are pure [`ServerState`] methods that take `now`; the run loop sleeps
//! until [`ServerState::next_wake`] or until there is something to send.
use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use temporalio_common::RetryPolicy;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    CancellableFuture, ContinueAsNewOptions, LocalActivityOptions, SignalWorkflowOptions,
    SyncWorkflowContext, WorkflowContext, WorkflowContextView, WorkflowResult,
};

use super::activities::{
    CoordinatorReport, GameServerActivities, InstanceHealth, InstanceIn, ProvisionOutcome,
    ProvisionerKind,
};
use super::coordinator::{
    ClaimIn, GoneReason, JoinOutcome, MAX_HISTORY, Phase, PlayerRef, RUN_SECS, ReleaseIn,
    ServerGoneIn, ServerReport, ShutdownIn, ShutdownPlan, ShutdownReason,
};
use super::session::{PlayerSessionWorkflow, PresenceIn};
use super::{now_secs, session_id};

/// Closes the instance workflows of a deleted preview (no worker left to continue them).
pub const RUN_TIMEOUT: Duration = Duration::from_secs(24 * 3600);
/// The workflow re-sends its status this often, so a coordinator that lost a signal heals.
pub const STATUS_EVERY_SECS: u64 = 300;
/// While provisioning, the pod is checked for being unschedulable this often.
pub const INSTANCE_CHECK_SECS: u64 = 10;
/// After a shutdown is granted, the instance is deleted this long later (the process
/// exits on its own first; `to_menu` gives clients up to 10 s to leave).
pub const DESTROY_GRACE_SECS: u64 = 5;
pub const TO_MENU_GRACE_SECS: u64 = 15;

fn one() -> u64 {
    1
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct GameServerArgs {
    pub task_queue: String,
    pub slot: u32,
    pub generation: u64,
    pub provisioner: ProvisionerKind,
    pub join_code: Option<String>,
    pub stale_secs: u64,
    pub ready_timeout_secs: u64,
    pub unschedulable_secs: u64,
    pub carried: Option<ServerState>,
}

/// A heartbeat as the game server sends it to the internal API. Servers that predate
/// generations omit `generation` (1) and the counters.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct HeartbeatIn {
    pub slot: u32,
    #[serde(default = "one")]
    pub generation: u64,
    pub boot_id: String,
    /// Every character with units in the world, pending joins included.
    pub players: Vec<PlayerRef>,
    /// Connected clients; `None` from old servers, which count every listed player.
    #[serde(default)]
    pub connected: Option<u32>,
    #[serde(default)]
    pub paused: bool,
    #[serde(default)]
    pub udp_port: Option<u16>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct JoinIn {
    pub slot: u32,
    #[serde(default = "one")]
    pub generation: u64,
    pub boot_id: String,
    pub character_id: i64,
    pub sid: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct LeaveIn {
    pub slot: u32,
    #[serde(default = "one")]
    pub generation: u64,
    pub boot_id: String,
    pub character_id: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ShutdownRequestIn {
    pub slot: u32,
    #[serde(default = "one")]
    pub generation: u64,
    pub boot_id: String,
    pub reason: ShutdownReason,
    #[serde(default)]
    pub players: Vec<i64>,
}

/// What the instance's workflow knows, as answered by the `server` query.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ServerInfo {
    pub slot: u32,
    pub generation: u64,
    pub join_code: Option<String>,
    pub phase: Phase,
    pub udp_port: Option<u16>,
    pub paused: bool,
    pub connected: u32,
    pub players: Vec<PlayerRef>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum ServerEffect {
    Presence(PresenceTo),
    Report(CoordinatorReport),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct PresenceTo {
    pub sid: String,
    pub character_id: i64,
    pub present: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ServerState {
    pub phase: Phase,
    pub boot_id: Option<String>,
    /// character_id -> sid of the login session that joined it.
    pub roster: BTreeMap<i64, String>,
    /// Whether this boot's heartbeat roster has been taken over. Only the first
    /// heartbeat of a boot adds players (rebuilding after a restart of this workflow);
    /// later ones only remove, because a roster can list a join that was refused.
    pub adopted: bool,
    /// Renewed by every heartbeat to `now + stale_secs`. The instance counts as gone one
    /// more `stale_secs` after it lapses ([`ServerState::gone_after`]), so a restart of
    /// the authserver (which delivers the heartbeats) costs nothing.
    pub lease_until: u64,
    pub ready_deadline: u64,
    pub next_check: u64,
    /// When the provisioning pod was first seen unschedulable (or missing).
    pub unschedulable_since: Option<u64>,
    pub next_status: u64,
    pub destroy_at: u64,
    pub connected: u32,
    pub paused: bool,
    pub udp_port: Option<u16>,
    pub outbox: Vec<ServerEffect>,
}

impl ServerState {
    /// A new instance; the timeouts are counted from `now`.
    pub fn new(args: &GameServerArgs, now: u64) -> Self {
        Self {
            phase: Phase::Provisioning,
            boot_id: None,
            roster: BTreeMap::new(),
            adopted: false,
            lease_until: 0,
            ready_deadline: now + args.ready_timeout_secs,
            next_check: now + INSTANCE_CHECK_SECS,
            unschedulable_since: None,
            next_status: 0,
            destroy_at: 0,
            connected: 0,
            paused: false,
            udp_port: None,
            outbox: Vec::new(),
        }
    }

    pub fn info(&self, args: &GameServerArgs) -> ServerInfo {
        ServerInfo {
            slot: args.slot,
            generation: args.generation,
            join_code: args.join_code.clone(),
            phase: self.phase,
            udp_port: self.udp_port,
            paused: self.paused,
            connected: self.connected,
            players: self.players(),
        }
    }

    fn players(&self) -> Vec<PlayerRef> {
        self.roster
            .iter()
            .map(|(c, sid)| PlayerRef {
                character_id: *c,
                sid: sid.clone(),
            })
            .collect()
    }

    fn report(&self, args: &GameServerArgs) -> ServerReport {
        ServerReport {
            slot: args.slot,
            generation: args.generation,
            udp_port: self.udp_port,
            paused: self.paused,
            connected: self.connected,
            players: self.players(),
        }
    }

    fn presence(&mut self, sid: String, character_id: i64, present: bool) {
        self.outbox.push(ServerEffect::Presence(PresenceTo {
            sid,
            character_id,
            present,
        }));
    }

    /// The character's units left this world.
    fn drop_player(&mut self, args: &GameServerArgs, character_id: i64) {
        if let Some(sid) = self.roster.remove(&character_id) {
            self.presence(sid, character_id, false);
            self.outbox
                .push(ServerEffect::Report(CoordinatorReport::Release(
                    ReleaseIn {
                        slot: args.slot,
                        generation: args.generation,
                        character_id,
                    },
                )));
        }
    }

    /// A new process on this instance: its predecessor's players are gone.
    pub fn observe_boot(&mut self, args: &GameServerArgs, boot_id: &str) {
        if self.boot_id.as_deref() == Some(boot_id) {
            return;
        }
        let old: Vec<i64> = self.roster.keys().copied().collect();
        for c in old {
            self.drop_player(args, c);
        }
        self.boot_id = Some(boot_id.to_owned());
        self.adopted = false;
    }

    pub fn heartbeat(&mut self, args: &GameServerArgs, input: &HeartbeatIn, now: u64) {
        self.lease_until = now + args.stale_secs;
        if self.phase == Phase::Draining {
            return;
        }
        self.observe_boot(args, &input.boot_id);
        let listed: BTreeMap<i64, &str> = input
            .players
            .iter()
            .map(|p| (p.character_id, p.sid.as_str()))
            .collect();
        let gone: Vec<i64> = self
            .roster
            .keys()
            .filter(|c| !listed.contains_key(c))
            .copied()
            .collect();
        for c in gone {
            self.drop_player(args, c);
        }
        if !self.adopted {
            self.adopted = true;
            for (c, sid) in listed {
                if let std::collections::btree_map::Entry::Vacant(e) = self.roster.entry(c) {
                    e.insert(sid.to_owned());
                    self.presence(sid.to_owned(), c, true);
                }
            }
        }
        let connected = input.connected.unwrap_or(input.players.len() as u32);
        let changed = (connected, input.paused, input.udp_port)
            != (self.connected, self.paused, self.udp_port);
        self.connected = connected;
        self.paused = input.paused;
        self.udp_port = input.udp_port;
        if matches!(self.phase, Phase::Provisioning | Phase::Recovering) {
            self.phase = Phase::Ready;
            self.next_status = now + STATUS_EVERY_SECS;
            self.outbox
                .push(ServerEffect::Report(CoordinatorReport::Ready(
                    self.report(args),
                )));
        } else if changed {
            self.outbox
                .push(ServerEffect::Report(CoordinatorReport::Status(
                    self.report(args),
                )));
        }
    }

    /// The coordinator admitted the character here.
    pub fn admitted(&mut self, args: &GameServerArgs, character_id: i64, sid: &str, now: u64) {
        self.lease_until = self.lease_until.max(now + args.stale_secs);
        if let Some(old) = self.roster.insert(character_id, sid.to_owned())
            && old != sid
        {
            self.presence(old, character_id, false);
        }
        self.presence(sid.to_owned(), character_id, true);
    }

    /// Only the boot that admitted a player can report it leaving.
    pub fn leave(&mut self, args: &GameServerArgs, input: &LeaveIn) {
        if self.boot_id.as_deref() == Some(input.boot_id.as_str()) {
            self.drop_player(args, input.character_id);
        }
    }

    /// The coordinator granted a shutdown.
    pub fn draining(&mut self, plan: &ShutdownPlan, now: u64) {
        self.phase = Phase::Draining;
        self.destroy_at = now
            + match plan {
                ShutdownPlan::Destroy => DESTROY_GRACE_SECS,
                ShutdownPlan::ToMenu => TO_MENU_GRACE_SECS,
            };
    }

    /// Records a provisioning check; true once the pod has had no place to run for
    /// `unschedulable_secs`, which means the cluster is full.
    pub fn instance_checked(
        &mut self,
        args: &GameServerArgs,
        health: InstanceHealth,
        now: u64,
    ) -> bool {
        self.next_check = now + INSTANCE_CHECK_SECS;
        match health {
            InstanceHealth::Scheduled => {
                self.unschedulable_since = None;
                false
            }
            InstanceHealth::Unschedulable => {
                let since = *self.unschedulable_since.get_or_insert(now);
                now >= since + args.unschedulable_secs
            }
        }
    }

    /// When the run loop must look at the clock again.
    pub fn next_wake(&self, args: &GameServerArgs) -> u64 {
        match self.phase {
            Phase::Provisioning if args.provisioner == ProvisionerKind::Kubernetes => {
                self.ready_deadline.min(self.next_check)
            }
            Phase::Provisioning => self.ready_deadline,
            Phase::Ready | Phase::Recovering => self.gone_after(args).min(self.next_status),
            Phase::Draining => self.destroy_at,
        }
    }

    /// When an instance without heartbeats counts as gone: two stale windows after the
    /// last one.
    pub fn gone_after(&self, args: &GameServerArgs) -> u64 {
        self.lease_until + args.stale_secs
    }

    /// The instance is gone: its players leave and the coordinator frees the slot.
    pub fn finish(&mut self, args: &GameServerArgs, reason: GoneReason) {
        for (c, sid) in std::mem::take(&mut self.roster) {
            self.presence(sid, c, false);
        }
        self.outbox
            .push(ServerEffect::Report(CoordinatorReport::Gone(
                ServerGoneIn {
                    slot: args.slot,
                    generation: args.generation,
                    reason,
                },
            )));
    }
}

/// What the run loop has to do after waking at `now`.
#[derive(Debug, PartialEq)]
enum Due {
    Nothing,
    CheckInstance,
    SendStatus,
    End { delete: bool, reason: GoneReason },
}

fn due(state: &ServerState, args: &GameServerArgs, now: u64) -> Due {
    let managed = args.provisioner != ProvisionerKind::Static;
    match state.phase {
        Phase::Provisioning if now >= state.ready_deadline => Due::End {
            delete: managed,
            // A static server is external: it did not fail to start, it went away.
            reason: if managed {
                GoneReason::StartFailed
            } else {
                GoneReason::Lost
            },
        },
        Phase::Provisioning
            if args.provisioner == ProvisionerKind::Kubernetes && now >= state.next_check =>
        {
            Due::CheckInstance
        }
        Phase::Ready | Phase::Recovering if now >= state.gone_after(args) => Due::End {
            delete: managed,
            reason: GoneReason::Lost,
        },
        Phase::Ready if now >= state.next_status => Due::SendStatus,
        Phase::Draining if now >= state.destroy_at => Due::End {
            delete: managed,
            reason: GoneReason::Destroyed,
        },
        _ => Due::Nothing,
    }
}

#[workflow]
pub struct GameServerWorkflow {
    args: GameServerArgs,
    state: ServerState,
    /// This run continues an earlier one (its state was carried).
    continued: bool,
}

impl GameServerWorkflow {
    fn instance(&self) -> InstanceIn {
        InstanceIn {
            slot: self.args.slot,
            generation: self.args.generation,
            join_code: self.args.join_code.clone(),
        }
    }
}

fn la(timeout_secs: u64, attempts: u32) -> LocalActivityOptions {
    LocalActivityOptions::builder()
        .schedule_to_close_timeout(Duration::from_secs(timeout_secs))
        .retry_policy(
            RetryPolicy::builder()
                .maximum_attempts(attempts)
                .initial_interval(Duration::from_secs(1))
                .build(),
        )
        .build()
}

/// Sends the outbox in order: presence to sessions, reports to the coordinator.
async fn drain(ctx: &mut WorkflowContext<GameServerWorkflow>) {
    let (queue, slot) = ctx.state(|s| (s.args.task_queue.clone(), s.args.slot));
    while let Some(effect) =
        ctx.state_mut(|s| (!s.state.outbox.is_empty()).then(|| s.state.outbox.remove(0)))
    {
        match effect {
            ServerEffect::Presence(p) => {
                // NotFound = the session ended or never started; nothing to tell.
                let _ = ctx
                    .external_workflow(session_id(&queue, &p.sid), None)
                    .signal(
                        PlayerSessionWorkflow::presence,
                        PresenceIn {
                            slot,
                            character_id: p.character_id,
                            present: p.present,
                        },
                        SignalWorkflowOptions::default(),
                    )
                    .await;
            }
            ServerEffect::Report(report) => {
                // Signal-with-start: a terminated coordinator is recreated, not lost.
                if let Err(e) = ctx
                    .execute_local_activity(
                        GameServerActivities::report_to_coordinator,
                        report.clone(),
                        la(120, 0),
                    )
                    .await
                {
                    tracing::error!(?report, "game-server workflow: report not delivered: {e}");
                }
            }
        }
    }
}

/// Deletes the instance's Kubernetes objects (managed only), then reports it gone.
async fn end(
    ctx: &mut WorkflowContext<GameServerWorkflow>,
    delete: bool,
    reason: GoneReason,
) -> WorkflowResult<()> {
    if delete {
        let instance = ctx.state(|s| s.instance());
        if let Err(e) = ctx
            .execute_local_activity(
                GameServerActivities::delete_instance,
                instance.clone(),
                la(300, 0),
            )
            .await
        {
            tracing::error!(
                ?instance,
                "game-server workflow: delete failed, objects leaked: {e}"
            );
        }
    }
    ctx.state_mut(|s| s.state.finish(&s.args, reason));
    drain(ctx).await;
    Ok(())
}

#[workflow_methods]
impl GameServerWorkflow {
    /// The init has no clock: a new instance's timeouts start when `run` does.
    #[init]
    pub fn new(_ctx: &WorkflowContextView, args: GameServerArgs) -> Self {
        let continued = args.carried.is_some();
        let state = args
            .carried
            .clone()
            .unwrap_or_else(|| ServerState::new(&args, 0));
        Self {
            args: GameServerArgs {
                carried: None,
                ..args
            },
            state,
            continued,
        }
    }

    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let started = now_secs(ctx.workflow_time());
        let deadline = started + RUN_SECS;
        let fresh = !ctx.state(|s| s.continued);
        if fresh {
            ctx.state_mut(|s| {
                s.state.ready_deadline = started + s.args.ready_timeout_secs;
                s.state.next_check = started + INSTANCE_CHECK_SECS;
            });
        }
        let provisioner = ctx.state(|s| s.args.provisioner);
        if fresh && provisioner != ProvisionerKind::Static {
            let instance = ctx.state(|s| s.instance());
            let outcome = ctx
                .execute_local_activity(GameServerActivities::provision_server, instance, la(60, 3))
                .await;
            match outcome {
                Ok(ProvisionOutcome::Provisioned) => {}
                Ok(ProvisionOutcome::ServersFull) => {
                    return end(ctx, false, GoneReason::ServersFull).await;
                }
                Ok(ProvisionOutcome::StartFailed { reason }) => {
                    tracing::warn!("game-server workflow: provisioning failed: {reason}");
                    return end(ctx, true, GoneReason::StartFailed).await;
                }
                Err(e) => {
                    tracing::warn!("game-server workflow: provisioning failed: {e}");
                    return end(ctx, true, GoneReason::StartFailed).await;
                }
            }
        }
        loop {
            let now = now_secs(ctx.workflow_time());
            let wake = ctx.state(|s| s.state.next_wake(&s.args));
            if wake > now && ctx.state(|s| s.state.outbox.is_empty()) {
                let mut timer = std::pin::pin!(ctx.timer(Duration::from_secs(wake - now)));
                let fired = temporalio_sdk::workflows::select! {
                    _ = timer => true,
                    _ = ctx.wait_condition(move |s: &Self| {
                        !s.state.outbox.is_empty() || s.state.next_wake(&s.args) < wake
                    }) => false,
                };
                if !fired {
                    timer.cancel();
                }
            }
            drain(ctx).await;
            let now = now_secs(ctx.workflow_time());
            match ctx.state(|s| due(&s.state, &s.args, now)) {
                Due::Nothing => {}
                Due::SendStatus => ctx.state_mut(|s| {
                    s.state.next_status = now + STATUS_EVERY_SECS;
                    let report = s.state.report(&s.args);
                    s.state
                        .outbox
                        .push(ServerEffect::Report(CoordinatorReport::Status(report)));
                }),
                Due::CheckInstance => {
                    let instance = ctx.state(|s| s.instance());
                    let health = ctx
                        .execute_local_activity(
                            GameServerActivities::instance_status,
                            instance,
                            la(10, 2),
                        )
                        .await;
                    // An unanswered check counts as scheduled: only a seen problem frees the slot.
                    let health = health.unwrap_or(InstanceHealth::Scheduled);
                    let now = now_secs(ctx.workflow_time());
                    if ctx.state_mut(|s| s.state.instance_checked(&s.args, health, now)) {
                        return end(ctx, true, GoneReason::ServersFull).await;
                    }
                }
                Due::End { delete, reason } => return end(ctx, delete, reason).await,
            }
            // Checked on every wake, not only when the timer fires.
            let due_can = ctx.continue_as_new_suggested()
                || ctx.history_length() > MAX_HISTORY
                || now_secs(ctx.workflow_time()) >= deadline;
            if due_can && ctx.all_handlers_finished() && ctx.state(|s| s.state.outbox.is_empty()) {
                let args = ctx.state(|s| GameServerArgs {
                    carried: Some(s.state.clone()),
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
        self.state
            .heartbeat(&self.args, &input, now_secs(ctx.workflow_time()));
    }

    #[signal]
    pub fn leave(&mut self, _ctx: &mut SyncWorkflowContext<Self>, input: LeaveIn) {
        self.state.leave(&self.args, &input);
    }

    /// Claims the character at the coordinator, then records it here.
    #[update]
    pub async fn join(ctx: &mut WorkflowContext<Self>, input: JoinIn) -> JoinOutcome {
        let claim = ctx.state_mut(|s| {
            if s.state.phase == Phase::Draining {
                return Err(JoinOutcome::ServerDraining);
            }
            s.state.observe_boot(&s.args, &input.boot_id);
            Ok(ClaimIn {
                slot: s.args.slot,
                generation: s.args.generation,
                character_id: input.character_id,
                sid: input.sid.clone(),
            })
        });
        let claim = match claim {
            Ok(claim) => claim,
            Err(outcome) => return outcome,
        };
        // The game server gives up on a join after 6 s and the internal API after 4 s.
        let outcome = ctx
            .execute_local_activity(GameServerActivities::claim_character, claim, la(3, 2))
            .await;
        match outcome {
            Ok(JoinOutcome::Admitted) => {
                let now = now_secs(ctx.workflow_time());
                ctx.state_mut(|s| {
                    s.state
                        .admitted(&s.args, input.character_id, &input.sid, now)
                });
                JoinOutcome::Admitted
            }
            Ok(other) => other,
            Err(e) => JoinOutcome::Failed {
                reason: format!("claim_character: {e}"),
            },
        }
    }

    /// The server chose to shut down; the coordinator decides what happens to its players.
    #[update]
    pub async fn shutdown(
        ctx: &mut WorkflowContext<Self>,
        input: ShutdownRequestIn,
    ) -> ShutdownPlan {
        let request = ctx.state(|s| ShutdownIn {
            slot: s.args.slot,
            generation: s.args.generation,
            reason: input.reason,
            players: input.players.clone(),
        });
        let plan = ctx
            .execute_local_activity(GameServerActivities::request_shutdown, request, la(3, 2))
            .await
            .unwrap_or_else(|e| {
                // Destroying is still right: `server_gone` unbinds its players later.
                tracing::warn!(
                    "game-server workflow: shutdown not granted by the coordinator: {e}"
                );
                ShutdownPlan::Destroy
            });
        let now = now_secs(ctx.workflow_time());
        ctx.state_mut(|s| s.state.draining(&plan, now));
        plan
    }

    #[query]
    pub fn server(&self, _ctx: &WorkflowContextView) -> ServerInfo {
        self.state.info(&self.args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(provisioner: ProvisionerKind) -> GameServerArgs {
        GameServerArgs {
            task_queue: "q".into(),
            slot: 2,
            generation: 7,
            provisioner,
            join_code: Some("ABCD".into()),
            stale_secs: 30,
            ready_timeout_secs: 120,
            unschedulable_secs: 30,
            carried: None,
        }
    }

    fn hb(boot: &str, players: &[(i64, &str)], connected: u32) -> HeartbeatIn {
        HeartbeatIn {
            slot: 2,
            generation: 7,
            boot_id: boot.into(),
            players: players
                .iter()
                .map(|(c, s)| PlayerRef {
                    character_id: *c,
                    sid: (*s).into(),
                })
                .collect(),
            connected: Some(connected),
            paused: false,
            udp_port: Some(36002),
        }
    }

    fn reports(s: &ServerState) -> Vec<&CoordinatorReport> {
        s.outbox
            .iter()
            .filter_map(|e| match e {
                ServerEffect::Report(r) => Some(r),
                ServerEffect::Presence(_) => None,
            })
            .collect()
    }

    #[test]
    fn lease_and_ready_timeout_wake_the_loop() {
        let a = args(ProvisionerKind::Kubernetes);
        let mut s = ServerState::new(&a, 1000);
        // Provisioning: the pod check every 10 s, then the ready timeout.
        assert_eq!(s.next_wake(&a), 1010);
        assert_eq!(due(&s, &a, 1010), Due::CheckInstance);
        s.next_check = 1200;
        assert_eq!(s.next_wake(&a), 1120);
        assert_eq!(
            due(&s, &a, 1120),
            Due::End {
                delete: true,
                reason: GoneReason::StartFailed
            }
        );
        // First heartbeat: ready, reported once, then the lease decides.
        s.heartbeat(&a, &hb("b", &[], 0), 1050);
        assert_eq!(s.phase, Phase::Ready);
        assert!(matches!(reports(&s)[..], [CoordinatorReport::Ready(_)]));
        assert_eq!(s.next_wake(&a), 1110);
        assert_eq!(due(&s, &a, 1109), Due::Nothing);
        s.heartbeat(&a, &hb("b", &[], 0), 1060);
        assert_eq!(reports(&s).len(), 1, "no change, no status");
        assert_eq!(s.next_wake(&a), 1120);
        assert_eq!(
            due(&s, &a, 1120),
            Due::End {
                delete: true,
                reason: GoneReason::Lost
            }
        );
        // Static: never deleted, and a silent start is lost rather than failed.
        let st = args(ProvisionerKind::Static);
        let s = ServerState::new(&st, 1000);
        assert_eq!(s.next_wake(&st), 1120);
        assert_eq!(
            due(&s, &st, 1120),
            Due::End {
                delete: false,
                reason: GoneReason::Lost
            }
        );
    }

    #[test]
    fn unschedulable_for_30_seconds_means_servers_full() {
        let a = args(ProvisionerKind::Kubernetes);
        let mut s = ServerState::new(&a, 0);
        assert!(!s.instance_checked(&a, InstanceHealth::Unschedulable, 10));
        assert!(!s.instance_checked(&a, InstanceHealth::Unschedulable, 30));
        assert!(!s.instance_checked(&a, InstanceHealth::Scheduled, 35));
        assert!(!s.instance_checked(&a, InstanceHealth::Unschedulable, 40));
        assert!(s.instance_checked(&a, InstanceHealth::Unschedulable, 70));
        assert_eq!(s.next_check, 70 + INSTANCE_CHECK_SECS);
    }

    #[test]
    fn status_is_sent_on_change_and_every_300_seconds() {
        let a = args(ProvisionerKind::Kubernetes);
        let mut s = ServerState::new(&a, 0);
        s.heartbeat(&a, &hb("b", &[], 0), 10);
        s.outbox.clear();
        s.admitted(&a, 1, "s1", 11);
        s.heartbeat(&a, &hb("b", &[(1, "s1")], 1), 20);
        assert!(matches!(
            reports(&s)[..],
            [CoordinatorReport::Status(ServerReport { connected: 1, .. })]
        ));
        assert_eq!(s.next_status, 10 + STATUS_EVERY_SECS);
        s.lease_until = u64::MAX / 2;
        assert_eq!(due(&s, &a, 10 + STATUS_EVERY_SECS), Due::SendStatus);
    }

    #[test]
    fn roster_is_adopted_once_per_boot_and_only_shrinks_after() {
        let a = args(ProvisionerKind::Static);
        let mut s = ServerState::new(&a, 0);
        s.heartbeat(&a, &hb("b", &[(1, "s1"), (2, "s2")], 2), 1);
        assert_eq!(s.roster.len(), 2);
        s.outbox.clear();
        // A refused join can still be listed: never added after adoption.
        s.heartbeat(&a, &hb("b", &[(1, "s1"), (3, "s3")], 2), 2);
        assert_eq!(s.roster.keys().copied().collect::<Vec<_>>(), vec![1]);
        assert!(matches!(
            reports(&s)[..],
            [CoordinatorReport::Release(ReleaseIn {
                character_id: 2,
                ..
            })]
        ));
        // A new boot drops everyone from the old one.
        s.outbox.clear();
        s.heartbeat(&a, &hb("c", &[], 0), 3);
        assert!(s.roster.is_empty());
        assert!(s.outbox.iter().any(|e| matches!(
            e,
            ServerEffect::Presence(PresenceTo {
                character_id: 1,
                present: false,
                ..
            })
        )));
    }

    #[test]
    fn draining_ignores_rosters_and_destroys_after_the_grace() {
        let a = args(ProvisionerKind::Kubernetes);
        let mut s = ServerState::new(&a, 0);
        s.heartbeat(&a, &hb("b", &[(1, "s1")], 1), 1);
        s.draining(&ShutdownPlan::ToMenu, 10);
        s.outbox.clear();
        s.heartbeat(&a, &hb("b", &[], 0), 12);
        assert!(s.outbox.is_empty());
        assert_eq!(s.next_wake(&a), 10 + TO_MENU_GRACE_SECS);
        assert_eq!(
            due(&s, &a, 10 + TO_MENU_GRACE_SECS),
            Due::End {
                delete: true,
                reason: GoneReason::Destroyed
            }
        );
        s.finish(&a, GoneReason::Destroyed);
        assert!(s.roster.is_empty());
        assert!(matches!(
            reports(&s)[..],
            [CoordinatorReport::Gone(ServerGoneIn {
                reason: GoneReason::Destroyed,
                ..
            })]
        ));
    }

    #[test]
    fn old_servers_omit_generation_and_counters() {
        let hb: HeartbeatIn =
            serde_json::from_str(r#"{"slot":0,"boot_id":"a","players":[]}"#).unwrap();
        assert_eq!((hb.generation, hb.connected, hb.paused), (1, None, false));
    }
}
