//! Local activities run by `PlayerSessionWorkflow::play` on the authserver's worker.
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};

use super::coordinator::{AssignIn, CoordinatorWorkflow};
use crate::api::AppState;

pub struct SessionActivities {
    pub state: AppState,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MintArgs {
    pub character_id: i64,
    pub account_id: Option<i64>,
    pub sid: String,
    pub slot: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MintedTicket {
    pub ticket: String,
    pub expires_at: u64,
}

#[activities]
impl SessionActivities {
    /// Asks the coordinator for a slot (`None` = no healthy game server).
    #[activity]
    pub async fn assign_server(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: AssignIn,
    ) -> Result<Option<u32>, ActivityError> {
        Ok(super::coordinator_update(&self.state, CoordinatorWorkflow::assign, input).await?)
    }

    /// Signs a ticket bound to `slot` for the character.
    #[activity]
    pub async fn mint_ticket(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: MintArgs,
    ) -> Result<MintedTicket, ActivityError> {
        let (ticket, expires_at) = crate::tickets::mint(
            &self.state,
            input.character_id,
            input.account_id,
            &input.sid,
            input.slot,
        )
        .await?;
        Ok(MintedTicket { ticket, expires_at })
    }
}
