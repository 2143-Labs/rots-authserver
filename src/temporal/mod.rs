//! Temporal: the fleet [`coordinator`] and per-login [`session`] workflows, their
//! worker (hosted in this process) and the client calls the HTTP handlers make.
//!
//! Workflow code must be deterministic: time only from `ctx.workflow_time()`;
//! `BTreeMap`/`BTreeSet` only in workflow state (never `HashMap`/`HashSet`); no I/O;
//! no randomness. Any change to the sequence of commands a workflow issues
//! (activities, timers, external signals, continue-as-new) must be wrapped in
//! `ctx.patched("<change-id>")`: during a rolling deploy the old and new authservers
//! poll the same task queue and replay each other's histories.
pub mod activities;
pub mod coordinator;
pub mod session;

use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use anyhow::Context;
use http::StatusCode;
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, Url, WorkflowIdConflictPolicy,
    WorkflowIdReusePolicy, WorkflowSignalOptions, WorkflowStartOptions,
    WorkflowUpdateWithStartOptions,
};
use temporalio_common::{HasWorkflowDefinition, SignalDefinition, UpdateDefinition};
use temporalio_sdk::{Runtime, Worker, WorkerOptions};
use tokio::sync::watch;

use crate::api::{ApiError, AppState};
use crate::sessions::SessionRow;
use activities::SessionActivities;
use coordinator::{CoordinatorArgs, CoordinatorWorkflow};
use session::{PlayIn, PlayOutcome, PlayerSessionWorkflow, SessionArgs};

/// The connected client, `None` while Temporal is unreachable or unconfigured.
pub type TemporalSlot = Arc<RwLock<Option<Client>>>;

/// The type the `#[workflow_methods]` macro ties the coordinator's signals and updates to.
type CoordinatorRun = <CoordinatorWorkflow as HasWorkflowDefinition>::Run;

/// Upper bound for `/play` (the session's assign + mint fit in 7 s).
const PLAY_TIMEOUT: Duration = Duration::from_secs(10);
const RECONNECT_EVERY: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub struct TemporalDown;

pub fn client(st: &AppState) -> Result<Client, TemporalDown> {
    st.temporal
        .read()
        .expect("temporal slot lock poisoned")
        .clone()
        .ok_or(TemporalDown)
}

pub fn is_connected(st: &AppState) -> bool {
    st.temporal
        .read()
        .expect("temporal slot lock poisoned")
        .is_some()
}

/// Unix seconds of a workflow time; 0 if the SDK has none (never during an activation).
pub fn now_secs(t: Option<SystemTime>) -> u64 {
    t.and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
        .map_or(0, |d| d.as_secs())
}

/// The task queue is part of both ids because all previews share one namespace.
pub fn coordinator_id(queue: &str) -> String {
    format!("rots-coordinator-{queue}")
}

pub fn session_id(queue: &str, sid: &str) -> String {
    format!("rots-session-{queue}-{sid}")
}

fn coordinator_args(st: &AppState) -> CoordinatorArgs {
    CoordinatorArgs {
        task_queue: st.config.temporal_task_queue.clone(),
        stale_secs: st.config.coordinator_stale_secs,
        carried: None,
    }
}

/// Update the coordinator, starting it if needed. Both ids outlive their first run,
/// so reuse is allowed explicitly rather than left to the server default.
pub async fn coordinator_update<U>(
    st: &AppState,
    update: U,
    input: U::Input,
) -> anyhow::Result<U::Output>
where
    U: UpdateDefinition<Workflow = CoordinatorRun>,
    U::Input: Send,
{
    let client = client(st).map_err(|_| anyhow::anyhow!("temporal unavailable"))?;
    let queue = &st.config.temporal_task_queue;
    let options = WorkflowUpdateWithStartOptions::new(
        queue.clone(),
        coordinator_id(queue),
        WorkflowIdConflictPolicy::UseExisting,
    )
    .id_reuse_policy(WorkflowIdReusePolicy::AllowDuplicate)
    .run_timeout(coordinator::RUN_TIMEOUT)
    .build();
    client
        .execute_update_with_start_workflow(
            CoordinatorWorkflow::run,
            coordinator_args(st),
            update,
            input,
            options,
        )
        .await
        .context("coordinator update")
}

/// Signal the coordinator, starting it if needed.
pub async fn coordinator_signal<S>(st: &AppState, signal: S, input: S::Input) -> anyhow::Result<()>
where
    S: SignalDefinition<Workflow = CoordinatorRun>,
    S::Input: Send,
{
    let client = client(st).map_err(|_| anyhow::anyhow!("temporal unavailable"))?;
    let queue = &st.config.temporal_task_queue;
    let options = WorkflowStartOptions::new(queue.clone(), coordinator_id(queue))
        .id_conflict_policy(WorkflowIdConflictPolicy::UseExisting)
        .id_reuse_policy(WorkflowIdReusePolicy::AllowDuplicate)
        .run_timeout(coordinator::RUN_TIMEOUT)
        .build();
    client
        .signal_with_start_workflow(
            CoordinatorWorkflow::run,
            coordinator_args(st),
            signal,
            input,
            options,
        )
        .await
        .context("coordinator signal")?;
    Ok(())
}

/// Runs the session's `play` update, starting its workflow on the first call.
pub async fn play(st: &AppState, s: &SessionRow) -> Result<PlayOutcome, ApiError> {
    let unavailable = || ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "temporal_unavailable");
    let client = client(st).map_err(|_| {
        tracing::error!("play: temporal unavailable (no worker connection)");
        unavailable()
    })?;
    let queue = &st.config.temporal_task_queue;
    let sid = s.token_hash[..16].to_owned();
    let options = WorkflowUpdateWithStartOptions::new(
        queue.clone(),
        session_id(queue, &sid),
        WorkflowIdConflictPolicy::UseExisting,
    )
    .id_reuse_policy(WorkflowIdReusePolicy::AllowDuplicate)
    .execution_timeout(session::EXECUTION_TIMEOUT)
    .build();
    let call = client.execute_update_with_start_workflow(
        PlayerSessionWorkflow::run,
        SessionArgs { sid },
        PlayerSessionWorkflow::play,
        PlayIn {
            character_id: s.character_id,
            account_id: s.account_id,
        },
        options,
    );
    match tokio::time::timeout(PLAY_TIMEOUT, call).await {
        Ok(Ok(outcome)) => Ok(outcome),
        Ok(Err(e)) => {
            tracing::error!("play: session update failed: {e:#}");
            Err(unavailable())
        }
        Err(_) => {
            tracing::error!("play: session update timed out after {PLAY_TIMEOUT:?}");
            Err(unavailable())
        }
    }
}

/// Tells a session's workflow it is over (logout or replaced by a new login). Fire and
/// forget: the workflow may never have started, and the caller's response must not wait.
pub fn signal_logout(st: &AppState, sid: &str) {
    let Ok(client) = client(st) else {
        return;
    };
    let id = session_id(&st.config.temporal_task_queue, sid);
    tokio::spawn(async move {
        let handle = client.get_workflow_handle::<PlayerSessionWorkflow>(id.clone());
        let sent = tokio::time::timeout(
            Duration::from_secs(5),
            handle.signal(
                PlayerSessionWorkflow::logout,
                (),
                WorkflowSignalOptions::default(),
            ),
        )
        .await;
        match sent {
            Ok(Ok(())) => tracing::debug!(workflow = %id, "logout signalled"),
            Ok(Err(e)) => tracing::debug!(workflow = %id, "logout not signalled: {e}"),
            Err(_) => tracing::debug!(workflow = %id, "logout signal timed out"),
        }
    });
}

/// Stops the worker when dropped: in-flight tasks finish, then its thread exits and
/// releases its `AppState`. `listen` keeps it for the life of the process.
#[must_use = "dropping the handle stops the worker"]
pub struct WorkerHandle {
    _stop: watch::Sender<()>,
}

/// Runs the worker on its own thread and runtime, reconnecting every 10 s while
/// Temporal is unreachable. The connected client is published in `state.temporal`.
pub fn start_worker(state: AppState) -> WorkerHandle {
    let (stop_tx, stop) = watch::channel(());
    std::thread::Builder::new()
        .name("temporal-worker".into())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_name("temporal-rt")
                .enable_all()
                .build()
                .expect("building the temporal worker runtime");
            rt.block_on(worker_loop(state, stop));
        })
        .expect("spawning the temporal-worker thread");
    WorkerHandle { _stop: stop_tx }
}

/// `stop.changed()` resolves (with an error) once the `WorkerHandle` is dropped.
async fn worker_loop(state: AppState, mut stop: watch::Receiver<()>) {
    let runtime = match Runtime::from_current_tokio(Default::default()) {
        Ok(runtime) => runtime,
        Err(e) => {
            tracing::error!("temporal runtime failed to start: {e:#}");
            return;
        }
    };
    loop {
        if let Err(e) = run_worker(&runtime, &state, &mut stop).await {
            tracing::warn!("temporal unavailable: {e:#}");
        }
        *state.temporal.write().expect("temporal slot lock poisoned") = None;
        tokio::select! {
            _ = tokio::time::sleep(RECONNECT_EVERY) => {}
            _ = stop.changed() => return,
        }
    }
}

async fn run_worker(
    runtime: &Runtime,
    state: &AppState,
    stop: &mut watch::Receiver<()>,
) -> anyhow::Result<()> {
    let config = &state.config;
    let address = config
        .temporal_address
        .as_deref()
        .context("TEMPORAL_ADDRESS is unset")?;
    let url = Url::parse(address).with_context(|| format!("TEMPORAL_ADDRESS {address:?}"))?;
    let connection = Connection::connect(ConnectionOptions::new(url).build())
        .await
        .context("connecting")?;
    let client = Client::new(
        connection,
        ClientOptions::new(config.temporal_namespace.clone()).build(),
    )
    .context("client")?;
    let options = WorkerOptions::new(config.temporal_task_queue.clone())
        .register_workflow::<CoordinatorWorkflow>()?
        .register_workflow::<PlayerSessionWorkflow>()?
        .register_activities(SessionActivities {
            state: state.clone(),
        })
        .build();
    let mut worker = Worker::new(runtime, client.clone(), options).context("worker")?;
    *state.temporal.write().expect("temporal slot lock poisoned") = Some(client);
    tracing::info!(
        "temporal worker polling {}/{}",
        config.temporal_namespace,
        config.temporal_task_queue
    );
    let shutdown = worker.shutdown_handle();
    let mut run = std::pin::pin!(worker.run());
    tokio::select! {
        done = &mut run => done.context("worker stopped")?,
        _ = stop.changed() => {
            shutdown();
            run.await.context("worker shutdown")?;
        }
    }
    Ok(())
}
