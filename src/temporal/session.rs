//! One workflow per login session (`rots-session-<queue>-<sid>`), started by the
//! session's first `/play`. It serializes Play for the session, remembers the slot it
//! was last given, and tracks which (slot, character) pairs the coordinator reports as
//! connected, so `status` answers "in menus / assigned / in game".
//!
//! It stores no character: each `play` carries the session row's current character,
//! so switching characters (card ROTS #31) needs nothing here.
use std::collections::BTreeSet;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use temporalio_common::RetryPolicy;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    LocalActivityOptions, SyncWorkflowContext, WorkflowContext, WorkflowContextView, WorkflowResult,
};

use super::activities::{MintArgs, SessionActivities};
use super::coordinator::AssignIn;
use super::now_secs;

/// A session with nobody in game ends after this long; the next `/play` starts a new run.
pub const IDLE_SECS: u64 = 24 * 3600;
/// Closes sessions of a deleted preview that has no worker left.
pub const EXECUTION_TIMEOUT: Duration = Duration::from_secs(48 * 3600);

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SessionArgs {
    pub sid: String,
}

/// The session row's values at the time of the call.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PlayIn {
    pub character_id: i64,
    pub account_id: Option<i64>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct PresenceIn {
    pub slot: u32,
    pub character_id: i64,
    pub present: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SessionStatus {
    InMenus,
    Assigned { slot: u32, expires_at: u64 },
    InGame { slot: u32 },
    Ended,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum PlayOutcome {
    Assigned {
        ticket: String,
        expires_at: u64,
        slot: u32,
    },
    NoGameServer,
    Busy,
    Ended,
    Failed {
        reason: String,
    },
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SessionState {
    /// (slot, ticket expiry) of the last successful Play.
    pub assigned: Option<(u32, u64)>,
    /// (slot, character_id) pairs the coordinator reports as connected. Keyed by the
    /// pair so one character leaving never reports another one on the same slot as gone.
    pub present: BTreeSet<(u32, i64)>,
    pub playing: bool,
    pub ended: bool,
}

impl SessionState {
    pub fn status(&self) -> SessionStatus {
        if self.ended {
            SessionStatus::Ended
        } else if let Some(&(slot, _)) = self.present.first() {
            SessionStatus::InGame { slot }
        } else if let Some((slot, expires_at)) = self.assigned {
            SessionStatus::Assigned { slot, expires_at }
        } else {
            SessionStatus::InMenus
        }
    }

    pub fn apply_presence(&mut self, input: &PresenceIn) {
        let pair = (input.slot, input.character_id);
        if input.present {
            self.present.insert(pair);
            self.assigned = None;
        } else {
            self.present.remove(&pair);
        }
    }

    /// Unexpired assignment, else the lowest slot this session is playing on.
    pub fn preferred(&self, now: u64) -> Option<u32> {
        self.assigned
            .filter(|&(_, expires_at)| expires_at > now)
            .map(|(slot, _)| slot)
            .or_else(|| self.present.first().map(|&(slot, _)| slot))
    }
}

#[workflow]
pub struct PlayerSessionWorkflow {
    args: SessionArgs,
    state: SessionState,
}

#[workflow_methods]
impl PlayerSessionWorkflow {
    #[init]
    pub fn new(_ctx: &WorkflowContextView, args: SessionArgs) -> Self {
        Self {
            args,
            state: SessionState::default(),
        }
    }

    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        temporalio_sdk::workflows::select! {
            _ = ctx.timer(Duration::from_secs(IDLE_SECS)) => {}
            _ = ctx.wait_condition(|s: &Self| s.state.ended) => {}
        }
        // Never end under a connected player, and let an in-flight Play answer.
        ctx.wait_condition(|s: &Self| s.state.ended || s.state.present.is_empty())
            .await?;
        ctx.wait_condition(|s: &Self| !s.state.playing).await?;
        Ok(())
    }

    #[update]
    pub async fn play(ctx: &mut WorkflowContext<Self>, input: PlayIn) -> PlayOutcome {
        let now = now_secs(ctx.workflow_time());
        let gate = ctx.state_mut(|s| {
            if s.state.ended {
                Err(PlayOutcome::Ended)
            } else if s.state.playing {
                Err(PlayOutcome::Busy)
            } else {
                s.state.playing = true;
                Ok((s.args.sid.clone(), s.state.preferred(now)))
            }
        });
        let (sid, preferred) = match gate {
            Ok(v) => v,
            Err(outcome) => return outcome,
        };
        let outcome = place_and_mint(ctx, sid, preferred, input).await;
        ctx.state_mut(|s| {
            s.state.playing = false;
            if let PlayOutcome::Assigned {
                slot, expires_at, ..
            } = &outcome
            {
                s.state.assigned = Some((*slot, *expires_at));
            }
        });
        outcome
    }

    #[signal]
    pub fn presence(&mut self, _ctx: &mut SyncWorkflowContext<Self>, input: PresenceIn) {
        self.state.apply_presence(&input);
    }

    #[signal]
    pub fn logout(&mut self, _ctx: &mut SyncWorkflowContext<Self>, _input: ()) {
        self.state.ended = true;
    }

    #[query]
    pub fn status(&self, _ctx: &WorkflowContextView) -> SessionStatus {
        self.state.status()
    }
}

/// Assign (≤3 s) then mint (≤4 s): together inside the 10 s `/play` HTTP budget.
async fn place_and_mint(
    ctx: &WorkflowContext<PlayerSessionWorkflow>,
    sid: String,
    preferred: Option<u32>,
    input: PlayIn,
) -> PlayOutcome {
    let two_attempts = || RetryPolicy::builder().maximum_attempts(2).build();
    let assigned = ctx
        .execute_local_activity(
            SessionActivities::assign_server,
            AssignIn {
                character_id: input.character_id,
                sid: sid.clone(),
                preferred,
            },
            LocalActivityOptions::builder()
                .schedule_to_close_timeout(Duration::from_secs(3))
                .retry_policy(two_attempts())
                .build(),
        )
        .await;
    let slot = match assigned {
        Ok(Some(slot)) => slot,
        Ok(None) => return PlayOutcome::NoGameServer,
        Err(e) => {
            return PlayOutcome::Failed {
                reason: format!("assign_server: {e}"),
            };
        }
    };
    let minted = ctx
        .execute_local_activity(
            SessionActivities::mint_ticket,
            MintArgs {
                character_id: input.character_id,
                account_id: input.account_id,
                sid,
                slot,
            },
            LocalActivityOptions::builder()
                .schedule_to_close_timeout(Duration::from_secs(4))
                .start_to_close_timeout(Duration::from_secs(2))
                .retry_policy(two_attempts())
                .build(),
        )
        .await;
    match minted {
        Ok(m) => PlayOutcome::Assigned {
            ticket: m.ticket,
            expires_at: m.expires_at,
            slot,
        },
        Err(e) => PlayOutcome::Failed {
            reason: format!("mint_ticket: {e}"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pres(slot: u32, c: i64, present: bool) -> PresenceIn {
        PresenceIn {
            slot,
            character_id: c,
            present,
        }
    }

    #[test]
    fn status_for_each_state() {
        let mut s = SessionState::default();
        assert_eq!(s.status(), SessionStatus::InMenus);
        s.assigned = Some((2, 500));
        assert_eq!(
            s.status(),
            SessionStatus::Assigned {
                slot: 2,
                expires_at: 500
            }
        );
        s.apply_presence(&pres(2, 7, true));
        assert_eq!(s.status(), SessionStatus::InGame { slot: 2 });
        assert_eq!(s.assigned, None, "joining clears the assignment");
        s.ended = true;
        assert_eq!(s.status(), SessionStatus::Ended);
    }

    #[test]
    fn preferred_ignores_expired_assignment() {
        let mut s = SessionState::default();
        s.assigned = Some((3, 100));
        assert_eq!(s.preferred(99), Some(3));
        assert_eq!(s.preferred(100), None);
        s.apply_presence(&pres(1, 7, true));
        assert_eq!(s.preferred(100), Some(1));
    }

    #[test]
    fn presence_on_two_slots_reports_lowest_until_both_leave() {
        let mut s = SessionState::default();
        s.apply_presence(&pres(3, 7, true));
        s.apply_presence(&pres(1, 8, true));
        assert_eq!(s.status(), SessionStatus::InGame { slot: 1 });
        s.apply_presence(&pres(1, 8, false));
        assert_eq!(s.status(), SessionStatus::InGame { slot: 3 });
        s.apply_presence(&pres(3, 7, false));
        assert_eq!(s.status(), SessionStatus::InMenus);
    }

    #[test]
    fn two_characters_on_one_slot_are_tracked_separately() {
        let mut s = SessionState::default();
        s.apply_presence(&pres(0, 7, true));
        s.apply_presence(&pres(0, 8, true));
        s.apply_presence(&pres(0, 7, false));
        assert_eq!(s.status(), SessionStatus::InGame { slot: 0 });
        s.apply_presence(&pres(0, 8, false));
        assert_eq!(s.status(), SessionStatus::InMenus);
    }

    #[test]
    fn wire_shapes() {
        assert_eq!(
            serde_json::to_value(SessionStatus::InGame { slot: 1 }).unwrap(),
            serde_json::json!({"state":"in_game","slot":1})
        );
        assert_eq!(
            serde_json::to_value(PlayOutcome::NoGameServer).unwrap(),
            serde_json::json!({"result":"no_game_server"})
        );
    }
}
