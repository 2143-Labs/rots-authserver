//! Local activities run on the authserver's worker: the session's placement and ticket
//! minting, and everything the coordinator and the game-server workflows do outside
//! Temporal (start a workflow, talk to Kubernetes, signal the coordinator). They are
//! the only code that calls the Kubernetes API.
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use temporalio_client::{
    WorkflowDescribeOptions, WorkflowExecutionStatus, WorkflowIdConflictPolicy,
    WorkflowIdReusePolicy, WorkflowListOptions, WorkflowQueryOptions, WorkflowStartOptions,
    errors::WorkflowStartError,
};
use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};

use super::coordinator::{
    ClaimIn, CoordinatorWorkflow, CrashedIn, InstanceRef, JoinOutcome, Phase, PlaceIn, Placement,
    ReleaseIn, ServerGoneIn, ServerReport, ShutdownIn, ShutdownPlan,
};
use super::server::{GameServerWorkflow, ServerInfo};
use crate::api::AppState;
use crate::k8s;

/// How game-server instances come to exist (`GAME_SERVER_PROVISIONER`).
#[derive(Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum ProvisionerKind {
    /// External servers (prod today, local dev): never spawned, placed least-loaded.
    #[default]
    Static,
    /// Tests: records each call and pretends it worked.
    Fake,
    /// One StatefulSet + PVC per instance, created through the Kubernetes API.
    Kubernetes,
}

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
    /// Asks the coordinator where the character plays.
    #[activity]
    pub async fn place_character(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: PlaceIn,
    ) -> Result<Placement, ActivityError> {
        Ok(super::coordinator_update(&self.state, CoordinatorWorkflow::place, input).await?)
    }

    /// Tells the coordinator a client lost the character's server.
    #[activity]
    pub async fn report_crash(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: CrashedIn,
    ) -> Result<bool, ActivityError> {
        Ok(
            super::coordinator_update(&self.state, CoordinatorWorkflow::server_crashed, input)
                .await?,
        )
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

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct StartServerIn {
    pub slot: u32,
    pub generation: u64,
    pub join_code: Option<String>,
    /// Start it in pre-game, as a warm pool instance.
    #[serde(default)]
    pub warm: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct InstanceIn {
    pub slot: u32,
    pub generation: u64,
    pub join_code: Option<String>,
    /// Boot the container in pre-game (warm pool instances only).
    #[serde(default)]
    pub pre_game: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum ProvisionOutcome {
    Provisioned,
    /// The game-server quota has no room for another instance.
    ServersFull,
    StartFailed {
        reason: String,
    },
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InstanceHealth {
    Scheduled,
    /// The pod is `Unschedulable`, or does not exist (rejected by the quota).
    Unschedulable,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProbeResult {
    /// `/healthz` answered 200.
    Alive,
    /// No pod, no pod IP, or no 200 within 2 s.
    Dead,
}

/// A game-server workflow's report to the coordinator.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum CoordinatorReport {
    Ready(ServerReport),
    Status(ServerReport),
    Release(ReleaseIn),
    Gone(ServerGoneIn),
    Recovering(InstanceRef),
}

/// What the fake provisioner was asked to do, in order.
#[derive(Clone, Debug, PartialEq)]
pub enum FakeCall {
    Provision(InstanceIn),
    Delete(InstanceIn),
    Probe(InstanceIn),
    Kill(InstanceIn),
    /// `/evict` on the instance at `slot`.
    Evict {
        instance: InstanceIn,
        character_id: i64,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct EvictCharacterIn {
    pub instance: InstanceIn,
    pub character_id: i64,
}

pub struct GameServerActivities {
    pub state: AppState,
    /// Present when `GAME_SERVER_PROVISIONER=kubernetes` and the in-cluster
    /// credentials were readable.
    pub kube: Option<k8s::Kube>,
}

impl GameServerActivities {
    fn kind(&self) -> ProvisionerKind {
        self.state.config.game_server_provisioner
    }

    fn kube(&self) -> anyhow::Result<&k8s::Kube> {
        self.kube
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("no in-cluster Kubernetes credentials"))
    }
}

/// `(slot, generation)` from `rots-gs-<queue>-<slot>-g<generation>`.
/// What a coordinator that starts without state learns from Temporal.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct Rebuilt {
    pub servers: Vec<ServerInfo>,
    /// The highest generation of any instance workflow on the queue, closed or not.
    pub max_generation: u64,
}

fn parse_server_id(queue: &str, id: &str) -> Option<(u32, u64)> {
    let rest = id.strip_prefix(&format!("rots-gs-{queue}-"))?;
    let (slot, generation) = rest.split_once("-g")?;
    Some((slot.parse().ok()?, generation.parse().ok()?))
}

#[activities]
impl GameServerActivities {
    /// Starts the instance's workflow. "Already started" is success only while that
    /// workflow is running (an earlier attempt got through); a closed one means the
    /// generation was used before, and the start fails rather than leaving the slot
    /// waiting for an instance that does not exist.
    #[activity]
    pub async fn start_server_workflow(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: StartServerIn,
    ) -> Result<(), ActivityError> {
        let st = &self.state;
        let client = super::client(st).map_err(|_| anyhow::anyhow!("temporal unavailable"))?;
        let queue = st.config.temporal_task_queue.clone();
        let options = WorkflowStartOptions::new(
            queue.clone(),
            super::server_id(&queue, input.slot, input.generation),
        )
        .id_conflict_policy(WorkflowIdConflictPolicy::Fail)
        .id_reuse_policy(WorkflowIdReusePolicy::RejectDuplicate)
        .run_timeout(super::server::RUN_TIMEOUT)
        .build();
        let args = super::server_args(
            st,
            input.slot,
            input.generation,
            input.join_code,
            input.warm,
        );
        match client
            .start_workflow(GameServerWorkflow::run, args, options)
            .await
        {
            Ok(_) => Ok(()),
            Err(WorkflowStartError::AlreadyStarted { .. }) => {
                let id = super::server_id(&queue, input.slot, input.generation);
                let running = client
                    .get_workflow_handle::<GameServerWorkflow>(id.clone())
                    .describe(WorkflowDescribeOptions::default())
                    .await
                    .is_ok_and(|d| d.status() == WorkflowExecutionStatus::Running);
                if running {
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("{id} was used by an earlier instance").into())
                }
            }
            Err(e) => Err(anyhow::anyhow!("starting the instance workflow: {e}").into()),
        }
    }

    /// The running instances of this queue, for a coordinator that starts without
    /// state, and the highest generation any instance (closed ones too) has used, so
    /// the new coordinator never hands out an instance id that already exists. An
    /// instance whose query does not answer quickly (its workflow may be waiting on
    /// this very coordinator) is listed with an empty roster.
    #[activity]
    pub async fn rebuild_directory(
        self: Arc<Self>,
        _ctx: ActivityContext,
        _input: (),
    ) -> Result<Rebuilt, ActivityError> {
        let st = &self.state;
        let client = super::client(st).map_err(|_| anyhow::anyhow!("temporal unavailable"))?;
        let queue = &st.config.temporal_task_queue;
        let query = format!("WorkflowType='GameServerWorkflow' AND TaskQueue='{queue}'");
        let mut ids = Vec::new();
        let mut max_generation = 0;
        let mut listing = client.list_workflows(query, WorkflowListOptions::default());
        while let Some(found) = listing.next().await {
            let found = found.map_err(anyhow::Error::from)?;
            if let Some((_, generation)) = parse_server_id(queue, found.id()) {
                max_generation = max_generation.max(generation);
            }
            if found.status() == WorkflowExecutionStatus::Running {
                ids.push(found.id().to_owned());
            }
        }
        let queries = ids.into_iter().map(|id| {
            let client = client.clone();
            async move {
                let handle = client.get_workflow_handle::<GameServerWorkflow>(id.clone());
                let answer = tokio::time::timeout(
                    Duration::from_millis(1500),
                    handle.query(
                        GameServerWorkflow::server,
                        (),
                        WorkflowQueryOptions::default(),
                    ),
                )
                .await;
                match answer {
                    Ok(Ok(info)) => Some(info),
                    _ => {
                        let (slot, generation) = parse_server_id(queue, &id)?;
                        tracing::warn!(workflow = %id, "rebuild: no answer to the server query");
                        Some(ServerInfo {
                            slot,
                            generation,
                            join_code: None,
                            phase: Phase::Ready,
                            udp_port: None,
                            paused: false,
                            connected: 0,
                            players: Vec::new(),
                            // Not known: a stranded pool instance is never handed out and
                            // its own workflow retires it.
                            warm: false,
                        })
                    }
                }
            }
        });
        let servers = futures::future::join_all(queries)
            .await
            .into_iter()
            .flatten()
            .collect();
        Ok(Rebuilt {
            servers,
            max_generation,
        })
    }

    #[activity]
    pub async fn provision_server(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: InstanceIn,
    ) -> Result<ProvisionOutcome, ActivityError> {
        match self.kind() {
            ProvisionerKind::Static => Ok(ProvisionOutcome::StartFailed {
                reason: "static provisioner cannot spawn".into(),
            }),
            ProvisionerKind::Fake => {
                self.state
                    .fake_calls
                    .lock()
                    .expect("fake calls lock poisoned")
                    .push(FakeCall::Provision(input));
                Ok(ProvisionOutcome::Provisioned)
            }
            ProvisionerKind::Kubernetes => {
                let config = &self.state.config;
                let port = u32::from(config.game_udp_port_base) + input.slot;
                let port = u16::try_from(port)
                    .map_err(|_| anyhow::anyhow!("udp port {port} out of range"))?;
                Ok(k8s::provision(self.kube()?, &input, port).await?)
            }
        }
    }

    #[activity]
    pub async fn instance_status(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: InstanceIn,
    ) -> Result<InstanceHealth, ActivityError> {
        match self.kind() {
            ProvisionerKind::Kubernetes => Ok(k8s::instance_health(self.kube()?, &input).await?),
            ProvisionerKind::Static | ProvisionerKind::Fake => Ok(InstanceHealth::Scheduled),
        }
    }

    #[activity]
    pub async fn delete_instance(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: InstanceIn,
    ) -> Result<(), ActivityError> {
        match self.kind() {
            ProvisionerKind::Static => Ok(()),
            ProvisionerKind::Fake => {
                self.state
                    .fake_calls
                    .lock()
                    .expect("fake calls lock poisoned")
                    .push(FakeCall::Delete(input));
                Ok(())
            }
            ProvisionerKind::Kubernetes => Ok(k8s::delete_instance(self.kube()?, &input).await?),
        }
    }

    /// GETs the server's `/healthz` (2 s timeout).
    #[activity]
    pub async fn probe_server(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: InstanceIn,
    ) -> Result<ProbeResult, ActivityError> {
        match self.kind() {
            ProvisionerKind::Static => {
                // The chart's per-slot Service exposes the health port.
                let url = format!("http://rots-gs-{}:25566/healthz", input.slot);
                Ok(k8s::healthz(&self.state.http, &url).await)
            }
            ProvisionerKind::Fake => {
                self.state
                    .fake_calls
                    .lock()
                    .expect("fake calls lock poisoned")
                    .push(FakeCall::Probe(input));
                Ok(
                    if self
                        .state
                        .fake_alive
                        .load(std::sync::atomic::Ordering::SeqCst)
                    {
                        ProbeResult::Alive
                    } else {
                        ProbeResult::Dead
                    },
                )
            }
            ProvisionerKind::Kubernetes => {
                Ok(k8s::probe(self.kube()?, &self.state.http, &input).await?)
            }
        }
    }

    /// Force-deletes the instance's pod; its StatefulSet recreates it on the same PVC.
    #[activity]
    pub async fn kill_pod(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: InstanceIn,
    ) -> Result<(), ActivityError> {
        match self.kind() {
            ProvisionerKind::Static => Ok(()),
            ProvisionerKind::Fake => {
                self.state
                    .fake_calls
                    .lock()
                    .expect("fake calls lock poisoned")
                    .push(FakeCall::Kill(input));
                Ok(())
            }
            ProvisionerKind::Kubernetes => Ok(k8s::kill_pod(self.kube()?, &input).await?),
        }
    }

    /// Tells a pool instance the coordinator is handing it to a player.
    #[activity]
    pub async fn activate_instance(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: InstanceIn,
    ) -> Result<(), ActivityError> {
        match self.kind() {
            ProvisionerKind::Kubernetes => {
                k8s::activate(
                    self.kube()?,
                    &self.state.http,
                    &self.state.config.coordinator_token,
                    &input,
                )
                .await?;
                Ok(())
            }
            ProvisionerKind::Static | ProvisionerKind::Fake => Ok(()),
        }
    }

    /// Tells an instance to remove a character that moved to another tutorial.
    #[activity]
    pub async fn evict_character(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: EvictCharacterIn,
    ) -> Result<(), ActivityError> {
        match self.kind() {
            ProvisionerKind::Kubernetes => {
                k8s::evict(
                    self.kube()?,
                    &self.state.http,
                    &self.state.config.coordinator_token,
                    &input.instance,
                    input.character_id,
                )
                .await?;
                Ok(())
            }
            ProvisionerKind::Fake => {
                self.state
                    .fake_calls
                    .lock()
                    .expect("fake calls lock poisoned")
                    .push(FakeCall::Evict {
                        instance: input.instance,
                        character_id: input.character_id,
                    });
                Ok(())
            }
            // Static servers have no join codes, so nothing is ever evicted from one.
            ProvisionerKind::Static => Ok(()),
        }
    }

    /// Whether a warm instance still runs the image a fresh pod would pull.
    #[activity]
    pub async fn warm_is_current(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: InstanceIn,
    ) -> Result<bool, ActivityError> {
        match self.kind() {
            ProvisionerKind::Kubernetes => {
                Ok(k8s::warm_is_current(self.kube()?, &self.state.http, &input)
                    .await
                    .unwrap_or_else(|e| {
                        tracing::warn!(
                            ?input,
                            "warm: image check failed, keeping the instance: {e:#}"
                        );
                        true
                    }))
            }
            ProvisionerKind::Static | ProvisionerKind::Fake => Ok(true),
        }
    }

    #[activity]
    pub async fn claim_character(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: ClaimIn,
    ) -> Result<JoinOutcome, ActivityError> {
        Ok(super::coordinator_update(&self.state, CoordinatorWorkflow::claim, input).await?)
    }

    #[activity]
    pub async fn request_shutdown(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: ShutdownIn,
    ) -> Result<ShutdownPlan, ActivityError> {
        Ok(
            super::coordinator_update(&self.state, CoordinatorWorkflow::request_shutdown, input)
                .await?,
        )
    }

    #[activity]
    pub async fn report_to_coordinator(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: CoordinatorReport,
    ) -> Result<(), ActivityError> {
        let st = &self.state;
        match input {
            CoordinatorReport::Ready(r) => {
                super::coordinator_signal(st, CoordinatorWorkflow::server_ready, r).await?
            }
            CoordinatorReport::Status(r) => {
                super::coordinator_signal(st, CoordinatorWorkflow::server_status, r).await?
            }
            CoordinatorReport::Release(r) => {
                super::coordinator_signal(st, CoordinatorWorkflow::release, r).await?
            }
            CoordinatorReport::Gone(r) => {
                super::coordinator_signal(st, CoordinatorWorkflow::server_gone, r).await?
            }
            CoordinatorReport::Recovering(r) => {
                super::coordinator_signal(st, CoordinatorWorkflow::server_recovering, r).await?
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_ids_parse_back() {
        assert_eq!(
            parse_server_id("rots-pr-7", "rots-gs-rots-pr-7-3-g12"),
            Some((3, 12))
        );
        assert_eq!(parse_server_id("q", "rots-gs-other-3-g12"), None);
        assert_eq!(
            parse_server_id("q", &super::super::server_id("q", 0, 1)),
            Some((0, 1))
        );
    }
}
