//! One workflow per login session (`rots-session-<queue>-<sid>`), started by the
//! session's first `/play`. It serializes Play for the session, remembers the slot it
//! was last given, and tracks which (slot, character) pairs the game-server workflows
//! report as joined, so `status` answers "in menus / assigned / in game".
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
use super::coordinator::{CrashedIn, PlaceIn, Placement};

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
    Assigned {
        slot: u32,
        expires_at: u64,
    },
    InGame {
        slot: u32,
    },
    /// A client of this session lost its server and reported it; cleared when a character
    /// joins again.
    Reconnecting,
    Ended,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum PlayOutcome {
    Assigned {
        ticket: String,
        expires_at: u64,
        slot: u32,
        generation: u64,
        udp_port: Option<u16>,
        join_code: Option<String>,
        paused: bool,
    },
    /// The character's instance is being spawned: ask again shortly.
    Starting,
    /// The character's instance is restarting after a crash: ask again shortly.
    Recovering,
    NoGameServer,
    ServersFull,
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
    pub reconnecting: bool,
}

impl SessionState {
    pub fn status(&self) -> SessionStatus {
        if self.ended {
            SessionStatus::Ended
        } else if self.reconnecting {
            SessionStatus::Reconnecting
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
            self.reconnecting = false;
        } else {
            self.present.remove(&pair);
        }
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
        let gate = ctx.state_mut(|s| {
            if s.state.ended {
                Err(PlayOutcome::Ended)
            } else if s.state.playing {
                Err(PlayOutcome::Busy)
            } else {
                s.state.playing = true;
                Ok(s.args.sid.clone())
            }
        });
        let sid = match gate {
            Ok(v) => v,
            Err(outcome) => return outcome,
        };
        let outcome = place_and_mint(ctx, sid, input).await;
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

    /// The client lost its server: have the character's instance probed. True when a
    /// probe was requested.
    #[update]
    pub async fn server_crashed(ctx: &mut WorkflowContext<Self>, input: CrashedIn) -> bool {
        ctx.state_mut(|s| s.state.reconnecting = true);
        ctx.execute_local_activity(
            SessionActivities::report_crash,
            input,
            LocalActivityOptions::builder()
                .schedule_to_close_timeout(Duration::from_secs(3))
                .retry_policy(RetryPolicy::builder().maximum_attempts(2).build())
                .build(),
        )
        .await
        .unwrap_or(false)
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

/// Place (≤3 s) then mint (≤4 s): together inside the 10 s `/play` HTTP budget.
async fn place_and_mint(
    ctx: &WorkflowContext<PlayerSessionWorkflow>,
    sid: String,
    input: PlayIn,
) -> PlayOutcome {
    let two_attempts = || RetryPolicy::builder().maximum_attempts(2).build();
    let placed = ctx
        .execute_local_activity(
            SessionActivities::place_character,
            PlaceIn {
                character_id: input.character_id,
                sid: sid.clone(),
                location: None,
            },
            LocalActivityOptions::builder()
                .schedule_to_close_timeout(Duration::from_secs(3))
                .retry_policy(two_attempts())
                .build(),
        )
        .await;
    let (slot, generation, udp_port, join_code, paused) = match placed {
        Ok(Placement::Ready {
            slot,
            generation,
            udp_port,
            join_code,
            paused,
        }) => (slot, generation, udp_port, join_code, paused),
        Ok(Placement::Starting { .. }) => return PlayOutcome::Starting,
        Ok(Placement::Recovering { .. }) => return PlayOutcome::Recovering,
        Ok(Placement::NoGameServer) => return PlayOutcome::NoGameServer,
        Ok(Placement::ServersFull) => return PlayOutcome::ServersFull,
        Err(e) => {
            return PlayOutcome::Failed {
                reason: format!("place_character: {e}"),
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
            generation,
            udp_port,
            join_code,
            paused,
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
