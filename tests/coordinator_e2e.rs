//! End-to-end: the public and internal routers against a real Postgres (a fresh database
//! per test, via `#[sqlx::test]`) and a real Temporal dev server, with fake game servers
//! that report like bevy2025's (one ordered sender, heartbeat every 500 ms). Static-mode
//! tests stand in for today's prod; `GAME_SERVER_PROVISIONER=fake` tests stand in for the
//! Kubernetes provisioner: they start the fake server the coordinator asked for.
//!
//! Run with `nix develop -c ./scripts/ci-test.sh`, which sets `DATABASE_URL` and
//! `TEMPORAL_TEST_ADDRESS`. Each test uses its own task queue, so they run in parallel.
use std::{
    collections::BTreeMap,
    future::Future,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{Router, body::Body};
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use http::{Method, Request, StatusCode, header};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sqlx::PgPool;
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, Url, WorkflowDescribeOptions,
    WorkflowExecutionStatus, WorkflowFetchHistoryOptions, WorkflowHistory, WorkflowQueryOptions,
    WorkflowTerminateOptions,
};
use temporalio_sdk::workflow_replayer::{WorkflowReplayer, WorkflowReplayerOptions};
use tokio::sync::{mpsc, oneshot};
use tower::ServiceExt;

use rots_authserver::{
    Config,
    api::{AppState, create_api},
    build_state, internal, sessions,
    temporal::{
        self,
        activities::{FakeCall, InstanceIn, ProvisionerKind},
        coordinator::{CoordinatorWorkflow, FleetState, Phase},
        server::GameServerWorkflow,
        session::PlayerSessionWorkflow,
    },
};

const TOKEN: &str = "devtoken";

// ---------------------------------------------------------------- setup

/// Server-side logs go to the test writer, so a failing test prints its own worker's
/// warnings (`play failed`, `coordinator ... failed`). `RUST_LOG` overrides the filter.
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_test_writer()
        .try_init();
}

fn config(temporal_address: Option<String>) -> Config {
    init_tracing();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    Config {
        public_url: "http://localhost:8000".into(),
        ticket_signing_key: STANDARD.encode([7u8; 32]),
        disabled_providers: "steam,discord,au2143".into(),
        temporal_address,
        temporal_namespace: "rots".into(),
        temporal_task_queue: format!("t-{nanos}"),
        coordinator_token: TOKEN.into(),
        game_server_slots: 4,
        game_server_capacity_per_server: 32,
        game_server_ready_timeout_secs: 120,
        game_server_unschedulable_secs: 30,
        // `Config::default()` bypasses the clap defaults: a pool instance must not
        // expire the instant it boots.
        game_server_warm_max_age_secs: 21600,
        game_udp_port_base: 25555,
        coordinator_stale_secs: 2,
        session_execution_timeout_secs: 48 * 3600,
        ..Default::default()
    }
}

fn temporal_address() -> String {
    std::env::var("TEMPORAL_TEST_ADDRESS").expect("TEMPORAL_TEST_ADDRESS (run scripts/ci-test.sh)")
}

/// A state whose worker is polling. Hold the handle for the whole test: dropping it stops
/// the worker, whose `AppState` would otherwise keep the test's pool (and the shared
/// sqlx master-pool permits it borrowed) alive for the rest of the run.
async fn connected(db: PgPool) -> (AppState, temporal::WorkerHandle) {
    connected_with(db, ProvisionerKind::Static).await
}

async fn connected_with(
    db: PgPool,
    provisioner: ProvisionerKind,
) -> (AppState, temporal::WorkerHandle) {
    connected_config(db, provisioner, 0).await
}

/// `warm` instances are kept booted in pre-game and handed out on placement.
async fn connected_config(
    db: PgPool,
    provisioner: ProvisionerKind,
    warm: u32,
) -> (AppState, temporal::WorkerHandle) {
    let config = Config {
        game_server_provisioner: provisioner,
        game_server_warm_instances: warm,
        ..config(Some(temporal_address()))
    };
    let st = build_state(config, db).await.unwrap();
    let worker = temporal::start_worker(st.clone());
    eventually("worker connected", Duration::from_secs(15), || {
        let st = st.clone();
        async move { temporal::is_connected(&st) }
    })
    .await;
    (st, worker)
}

async fn eventually<F, Fut>(what: &str, within: Duration, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let start = Instant::now();
    while start.elapsed() < within {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("timed out after {within:?} waiting for {what}");
}

async fn call(
    router: Router,
    method: Method,
    path: &str,
    bearer: Option<&str>,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut req = Request::builder().method(method).uri(path);
    if let Some(b) = bearer {
        req = req.header(header::AUTHORIZATION, format!("Bearer {b}"));
    }
    let req = match body {
        Some(v) => req
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(v.to_string())),
        None => req.body(Body::empty()),
    }
    .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn guest(st: &AppState) -> String {
    let (status, body) = call(
        create_api(st.clone()),
        Method::POST,
        "/api/v1/guest",
        None,
        Some(json!({"client":"native"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body["session_token"].as_str().unwrap().to_owned()
}

async fn play(st: &AppState, token: &str) -> (StatusCode, Value) {
    call(
        create_api(st.clone()),
        Method::POST,
        "/api/v1/play",
        Some(token),
        None,
    )
    .await
}

async fn logout(st: &AppState, token: &str) {
    let (status, _) = call(
        create_api(st.clone()),
        Method::POST,
        "/api/v1/logout",
        Some(token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

/// The JSON payload of a ticket (no signature check; tickets.rs tests that).
fn claims(ticket: &str) -> Value {
    let payload = ticket.split('.').nth(1).unwrap();
    serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).unwrap()).unwrap()
}

/// `(character_id, sid)` of a successful `/play` response.
fn who(body: &Value) -> (i64, String) {
    let c = claims(body["ticket"].as_str().unwrap());
    (
        c["sub"].as_str().unwrap().parse().unwrap(),
        c["sid"].as_str().unwrap().to_owned(),
    )
}

async fn status(st: &AppState, sid: &str) -> Value {
    let client = temporal::client(st).unwrap();
    let handle = client.get_workflow_handle::<PlayerSessionWorkflow>(temporal::session_id(
        &st.config.temporal_task_queue,
        sid,
    ));
    let status = handle
        .query(
            PlayerSessionWorkflow::status,
            (),
            WorkflowQueryOptions::default(),
        )
        .await
        .unwrap();
    serde_json::to_value(status).unwrap()
}

async fn status_becomes(st: &AppState, sid: &str, want: Value) {
    eventually(&format!("status {want}"), Duration::from_secs(8), || {
        let (st, want) = (st.clone(), want.clone());
        let sid = sid.to_owned();
        async move { status(&st, &sid).await == want }
    })
    .await;
}

async fn fleet(st: &AppState) -> Option<FleetState> {
    let client = temporal::client(st).unwrap();
    client
        .get_workflow_handle::<CoordinatorWorkflow>(temporal::coordinator_id(
            &st.config.temporal_task_queue,
        ))
        .query(
            CoordinatorWorkflow::fleet,
            (),
            WorkflowQueryOptions::default(),
        )
        .await
        .ok()
}

async fn slots_up(st: &AppState, slots: &[u32]) {
    eventually(
        &format!("slots {slots:?} registered"),
        Duration::from_secs(10),
        || {
            let st = st.clone();
            let slots = slots.to_vec();
            async move {
                fleet(&st).await.is_some_and(|f| {
                    slots
                        .iter()
                        .all(|s| f.servers.get(s).is_some_and(|e| e.phase == Phase::Ready))
                })
            }
        },
    )
    .await;
}

/// A workflow's history once no workflow task is open, so the replay never ends inside
/// one (the SDK's eviction after such a replay trips a debug assertion).
async fn settled_history(client: &Client, id: String) -> WorkflowHistory {
    let start = Instant::now();
    loop {
        let json = client
            .get_workflow_handle::<CoordinatorWorkflow>(id.clone())
            .fetch_history(WorkflowFetchHistoryOptions::default())
            .to_json()
            .await
            .unwrap();
        let history: Value = serde_json::from_slice(&json).unwrap();
        // pbjson writes `EVENT_TYPE_WORKFLOW_TASK_STARTED`; a numeric encoding is 5 / 6.
        let last = history["events"]
            .as_array()
            .and_then(|e| e.last())
            .map(|e| e["eventType"].clone())
            .unwrap_or_default();
        let name = last.to_string().to_lowercase().replace('_', "");
        let open = name.contains("workflowtaskscheduled")
            || name.contains("workflowtaskstarted")
            || last == json!(5)
            || last == json!(6);
        if !open {
            return WorkflowHistory::from_json(&json).unwrap();
        }
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "{id}: a workflow task stayed open"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Replays the coordinator's, the instances' and the sessions' histories against the
/// current code.
async fn replays_cleanly(st: &AppState, instances: &[(u32, u64)], sids: &[&str]) {
    let client = temporal::client(st).unwrap();
    let queue = &st.config.temporal_task_queue;
    let mut ids = vec![temporal::coordinator_id(queue)];
    ids.extend(
        instances
            .iter()
            .map(|(slot, generation)| temporal::server_id(queue, *slot, *generation)),
    );
    ids.extend(sids.iter().map(|sid| temporal::session_id(queue, sid)));
    let mut histories = Vec::new();
    for id in ids {
        histories.push(settled_history(&client, id).await);
    }
    let replayer = WorkflowReplayer::new(
        WorkflowReplayerOptions::new()
            .register_workflow::<CoordinatorWorkflow>()
            .unwrap()
            .register_workflow::<GameServerWorkflow>()
            .unwrap()
            .register_workflow::<PlayerSessionWorkflow>()
            .unwrap()
            .build(),
    )
    .unwrap();
    for result in replayer.replay_workflows(histories).await.unwrap() {
        assert!(
            result.replay_failure.is_none(),
            "{:?}",
            result.replay_failure
        );
    }
}

// ---------------------------------------------------------------- fake game server

enum Op {
    Shutdown {
        reason: &'static str,
        reply: oneshot::Sender<(StatusCode, Value)>,
    },
    Join {
        character_id: i64,
        sid: String,
        reply: oneshot::Sender<(StatusCode, Value)>,
    },
    Leave {
        character_id: i64,
    },
    Stop,
}

/// One ordered sender per server, like bevy2025's coordinator link: heartbeats (with
/// the roster, pending joins included) and joins/leaves never overtake each other.
struct FakeServer {
    tx: mpsc::UnboundedSender<Op>,
}

impl FakeServer {
    /// A static server as today's prod runs them: its reports carry no generation.
    fn start(st: &AppState, slot: u32, boot_id: &str, roster: &[(i64, &str)]) -> Self {
        Self::spawn(st, slot, None, boot_id, roster)
    }

    /// A server of `(slot, generation)`; its heartbeats also carry `connected` and a UDP
    /// port, as a managed instance's do.
    fn start_instance(
        st: &AppState,
        slot: u32,
        generation: u64,
        boot_id: &str,
        roster: &[(i64, &str)],
    ) -> Self {
        Self::spawn(st, slot, Some(generation), boot_id, roster)
    }

    fn spawn(
        st: &AppState,
        slot: u32,
        managed: Option<u64>,
        boot_id: &str,
        roster: &[(i64, &str)],
    ) -> Self {
        let generation = managed.unwrap_or(1);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let router = internal::router(st.clone());
        let boot_id = boot_id.to_owned();
        let mut players: BTreeMap<i64, String> =
            roster.iter().map(|(c, s)| (*c, (*s).to_owned())).collect();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(500));
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        let players: Vec<Value> = players.iter().map(|(c, s)| json!({"character_id": c, "sid": s})).collect();
                        let body = match managed {
                            None => json!({"slot": slot, "boot_id": boot_id, "players": players}),
                            Some(_) => json!({"slot": slot, "generation": generation, "boot_id": boot_id, "players": players, "connected": players.len(), "udp_port": 36000 + slot}),
                        };
                        let sent = Instant::now();
                        let (status, _) = call(router.clone(), Method::POST, "/internal/v1/servers/heartbeat", Some(TOKEN), Some(body)).await;
                        if status != StatusCode::NO_CONTENT || sent.elapsed() > Duration::from_millis(500) {
                            eprintln!("fake server {slot}: heartbeat {status} after {:?}", sent.elapsed());
                        }
                    }
                    op = rx.recv() => match op {
                        Some(Op::Join { character_id, sid, reply }) => {
                            players.insert(character_id, sid.clone());
                            let body = json!({"slot": slot, "generation": generation, "boot_id": boot_id, "character_id": character_id, "sid": sid});
                            let answer = call(router.clone(), Method::POST, "/internal/v1/servers/join", Some(TOKEN), Some(body)).await;
                            if answer.0 != StatusCode::OK {
                                players.remove(&character_id);
                            }
                            let _ = reply.send(answer);
                        }
                        Some(Op::Leave { character_id }) => {
                            players.remove(&character_id);
                            let body = json!({"slot": slot, "generation": generation, "boot_id": boot_id, "character_id": character_id});
                            call(router.clone(), Method::POST, "/internal/v1/servers/leave", Some(TOKEN), Some(body)).await;
                        }
                        Some(Op::Shutdown { reason, reply }) => {
                            let ids: Vec<i64> = players.keys().copied().collect();
                            let body = json!({"slot": slot, "generation": generation, "boot_id": boot_id, "reason": reason, "players": ids});
                            let _ = reply.send(call(router.clone(), Method::POST, "/internal/v1/servers/shutdown-request", Some(TOKEN), Some(body)).await);
                            // A managed server exits once its shutdown is granted.
                            break;
                        }
                        Some(Op::Stop) | None => break,
                    }
                }
            }
        });
        Self { tx }
    }

    async fn join(&self, character_id: i64, sid: &str) -> (StatusCode, Value) {
        let (reply, answer) = oneshot::channel();
        self.tx
            .send(Op::Join {
                character_id,
                sid: sid.to_owned(),
                reply,
            })
            .unwrap();
        answer.await.unwrap()
    }

    fn leave(&self, character_id: i64) {
        self.tx.send(Op::Leave { character_id }).unwrap();
    }

    /// Asks the coordinator to shut down, then stops reporting.
    async fn shutdown(&self, reason: &'static str) -> (StatusCode, Value) {
        let (reply, answer) = oneshot::channel();
        self.tx.send(Op::Shutdown { reason, reply }).unwrap();
        answer.await.unwrap()
    }
}

impl Drop for FakeServer {
    fn drop(&mut self) {
        let _ = self.tx.send(Op::Stop);
    }
}

fn in_game(slot: u32) -> Value {
    json!({"state": "in_game", "slot": slot})
}

fn in_menus() -> Value {
    json!({"state": "in_menus"})
}

// ---------------------------------------------------------------- tests

#[sqlx::test]
#[ignore = "needs Postgres + Temporal"]
async fn play_without_servers_is_503(db: PgPool) {
    let (st, _worker) = connected(db).await;
    let token = guest(&st).await;
    assert_eq!(
        play(&st, &token).await,
        (
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error":"no_game_server"})
        )
    );
}

#[sqlx::test]
#[ignore = "needs Postgres + Temporal"]
async fn play_picks_least_loaded(db: PgPool) {
    let (st, _worker) = connected(db).await;
    let _s0 = FakeServer::start(&st, 0, "a", &[(99, "x")]);
    let _s1 = FakeServer::start(&st, 1, "b", &[]);
    slots_up(&st, &[0, 1]).await;
    let token = guest(&st).await;
    let (status, body) = play(&st, &token).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["server"]["slot"], 1);
    assert_eq!(body["server"]["udp"], "localhost:25556");
    assert_eq!(body["server"]["ws_path"], "/ws/1");
    assert_eq!(body["kind"], "guest");
    let c = claims(body["ticket"].as_str().unwrap());
    assert_eq!(c["srv"], 1);
    let sid = c["sid"].as_str().unwrap();
    assert!(
        sid.len() == 16 && sid.bytes().all(|b| b.is_ascii_hexdigit()),
        "{sid}"
    );
}

#[sqlx::test]
#[ignore = "needs Postgres + Temporal"]
async fn join_leave_drive_status(db: PgPool) {
    let (st, _worker) = connected(db).await;
    let server = FakeServer::start(&st, 1, "b", &[]);
    slots_up(&st, &[1]).await;
    let token = guest(&st).await;
    let (status, body) = play(&st, &token).await;
    assert_eq!(
        (status, body["server"]["slot"].clone()),
        (StatusCode::OK, json!(1)),
        "{body}"
    );
    let (character, sid) = who(&body);
    assert_eq!(
        server.join(character, &sid).await,
        (StatusCode::OK, json!({"result":"admitted"}))
    );
    status_becomes(&st, &sid, in_game(1)).await;
    server.leave(character);
    status_becomes(&st, &sid, in_menus()).await;
    replays_cleanly(&st, &[(1, 1)], &[&sid]).await;
}

#[sqlx::test]
#[ignore = "needs Postgres + Temporal"]
async fn second_server_refused(db: PgPool) {
    let (st, _worker) = connected(db).await;
    let s0 = FakeServer::start(&st, 0, "a", &[]);
    let s1 = FakeServer::start(&st, 1, "b", &[]);
    slots_up(&st, &[0, 1]).await;
    let token = guest(&st).await;
    let (_, body) = play(&st, &token).await;
    let (character, sid) = who(&body);
    assert_eq!(s1.join(character, &sid).await.0, StatusCode::OK);
    assert_eq!(
        s0.join(character, &sid).await,
        (
            StatusCode::CONFLICT,
            json!({"error":"already_on_other_server"})
        )
    );
    status_becomes(&st, &sid, in_game(1)).await;
    replays_cleanly(&st, &[(0, 1), (1, 1)], &[&sid]).await;
}

#[sqlx::test]
#[ignore = "needs Postgres + Temporal"]
async fn same_character_two_sessions_same_slot(db: PgPool) {
    let (st, _worker) = connected(db.clone()).await;
    let s0 = FakeServer::start(&st, 0, "a", &[]);
    let _s1 = FakeServer::start(&st, 1, "b", &[]);
    slots_up(&st, &[0, 1]).await;
    // Two logins of one account: two sessions on the same (newest) character.
    let (token_a, character) = sessions::login_account(&st.db, "steam", "765", "Steam 765", None)
        .await
        .unwrap();
    let (token_b, character_b) = sessions::login_account(&st.db, "steam", "765", "Steam 765", None)
        .await
        .unwrap();
    assert_eq!(character, character_b);
    let (_, a) = play(&st, &token_a).await;
    assert_eq!(a["server"]["slot"], 0, "{a}");
    assert_eq!(s0.join(character, &who(&a).1).await.0, StatusCode::OK);
    // Least-loaded would now pick slot 1; presence-first keeps the character on 0.
    let (status, b) = play(&st, &token_b).await;
    assert_eq!(
        (status, b["server"]["slot"].clone()),
        (StatusCode::OK, json!(0)),
        "{b}"
    );
}

#[sqlx::test]
#[ignore = "needs Postgres + Temporal"]
async fn dead_server_reaped(db: PgPool) {
    let (st, _worker) = connected(db).await;
    let s1 = FakeServer::start(&st, 1, "b", &[]);
    slots_up(&st, &[1]).await;
    let token = guest(&st).await;
    let (_, body) = play(&st, &token).await;
    let (character, sid) = who(&body);
    assert_eq!(s1.join(character, &sid).await.0, StatusCode::OK);
    status_becomes(&st, &sid, in_game(1)).await;
    let _s0 = FakeServer::start(&st, 0, "a", &[]);
    drop(s1);
    // Slot 0's heartbeats drive the reap once slot 1 is 2 s stale.
    status_becomes(&st, &sid, in_menus()).await;
    for _ in 0..3 {
        let (status, body) = play(&st, &token).await;
        assert_eq!(
            (status, body["server"]["slot"].clone()),
            (StatusCode::OK, json!(0)),
            "{body}"
        );
    }
}

#[sqlx::test]
#[ignore = "needs Postgres + Temporal"]
async fn new_boot_clears_players(db: PgPool) {
    let (st, _worker) = connected(db).await;
    let old = FakeServer::start(&st, 1, "boot-a", &[]);
    slots_up(&st, &[1]).await;
    let token = guest(&st).await;
    let (_, body) = play(&st, &token).await;
    let (character, sid) = who(&body);
    assert_eq!(old.join(character, &sid).await.0, StatusCode::OK);
    status_becomes(&st, &sid, in_game(1)).await;
    drop(old);
    let _restarted = FakeServer::start(&st, 1, "boot-b", &[]);
    status_becomes(&st, &sid, in_menus()).await;
}

/// Holds `social.characters` locked for `hold`, so ticket minting blocks.
async fn lock_characters(db: &PgPool, hold: Duration) -> tokio::task::JoinHandle<()> {
    let mut tx = db.begin().await.unwrap();
    sqlx::query("LOCK TABLE social.characters IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *tx)
        .await
        .unwrap();
    tokio::spawn(async move {
        tokio::time::sleep(hold).await;
        tx.commit().await.unwrap();
    })
}

#[sqlx::test]
#[ignore = "needs Postgres + Temporal"]
async fn play_is_bounded(db: PgPool) {
    let (st, _worker) = connected(db.clone()).await;
    let _s0 = FakeServer::start(&st, 0, "a", &[]);
    slots_up(&st, &[0]).await;
    let token = guest(&st).await;
    let release = lock_characters(&db, Duration::from_secs(12)).await;
    let start = Instant::now();
    let answer = play(&st, &token).await;
    assert!(
        start.elapsed() < Duration::from_secs(10),
        "took {:?}",
        start.elapsed()
    );
    assert_eq!(
        answer,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"internal"})
        )
    );
    release.await.unwrap();
    let sid = sessions::hash_token(&token)[..16].to_owned();
    assert_eq!(status(&st, &sid).await, in_menus());
}

/// Temporal holds a workflow's new updates while one of its tasks is open, and `play` runs
/// inside one task (local activities), so a play that arrives while another is running
/// waits for it and is placed on its slot. Two healthy slots: interleaved plays would be
/// spread over both.
#[sqlx::test]
#[ignore = "needs Postgres + Temporal"]
async fn play_during_play_waits(db: PgPool) {
    let (st, _worker) = connected(db.clone()).await;
    let _s0 = FakeServer::start(&st, 0, "a", &[]);
    let _s1 = FakeServer::start(&st, 1, "b", &[]);
    slots_up(&st, &[0, 1]).await;
    let token = guest(&st).await;
    let release = lock_characters(&db, Duration::from_millis(1500)).await;
    let first = tokio::spawn({
        let (st, token) = (st.clone(), token.clone());
        async move { play(&st, &token).await }
    });
    // The first play's task is open (minting waits on the lock) when the second arrives.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let second = play(&st, &token).await;
    let first = first.await.unwrap();
    release.await.unwrap();
    assert_eq!(
        (first.0, second.0),
        (StatusCode::OK, StatusCode::OK),
        "{first:?} {second:?}"
    );
    assert_eq!(first.1["server"]["slot"], second.1["server"]["slot"]);
}

/// Plays that reach the workflow in the same task (two plays racing the session's start,
/// or a task retried after a worker crash; here, no worker polls until both are admitted)
/// would interleave without the `playing` guard, which answers the second with 409.
#[sqlx::test]
#[ignore = "needs Postgres + Temporal"]
async fn plays_in_one_task_one_is_busy(db: PgPool) {
    let st = build_state(config(Some(temporal_address())), db)
        .await
        .unwrap();
    let url = Url::parse(&temporal_address()).unwrap();
    let connection = Connection::connect(ConnectionOptions::new(url).build())
        .await
        .unwrap();
    let namespace = st.config.temporal_namespace.clone();
    *st.temporal.write().unwrap() =
        Some(Client::new(connection, ClientOptions::new(namespace).build()).unwrap());
    let _s0 = FakeServer::start(&st, 0, "a", &[]);
    let token = guest(&st).await;
    let plays = tokio::spawn({
        let st = st.clone();
        async move { tokio::join!(play(&st, &token), play(&st, &token)) }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    let _worker = temporal::start_worker(st.clone());
    let (a, b) = plays.await.unwrap();
    let mut statuses = [a.0, b.0];
    statuses.sort();
    assert_eq!(
        statuses,
        [StatusCode::OK, StatusCode::CONFLICT],
        "{a:?} {b:?}"
    );
    let busy = if a.0 == StatusCode::CONFLICT {
        a.1
    } else {
        b.1
    };
    assert_eq!(busy, json!({"error":"play_in_progress"}));
}

#[sqlx::test]
#[ignore = "needs Postgres + Temporal"]
async fn logout_ends_session(db: PgPool) {
    let (st, _worker) = connected(db).await;
    let _s0 = FakeServer::start(&st, 0, "a", &[]);
    slots_up(&st, &[0]).await;
    let token = guest(&st).await;
    let (_, body) = play(&st, &token).await;
    let (_, sid) = who(&body);
    logout(&st, &token).await;
    let client = temporal::client(&st).unwrap();
    let handle = client.get_workflow_handle::<PlayerSessionWorkflow>(temporal::session_id(
        &st.config.temporal_task_queue,
        &sid,
    ));
    eventually("session workflow completed", Duration::from_secs(5), || {
        let handle = &handle;
        async move {
            handle
                .describe(WorkflowDescribeOptions::default())
                .await
                .is_ok_and(|d| d.status() == WorkflowExecutionStatus::Completed)
        }
    })
    .await;
}

#[sqlx::test]
#[ignore = "needs Postgres + Temporal"]
async fn session_execution_timeout_comes_from_config(db: PgPool) {
    let config = Config {
        session_execution_timeout_secs: 7200,
        ..config(Some(temporal_address()))
    };
    let st = build_state(config, db).await.unwrap();
    let _worker = temporal::start_worker(st.clone());
    eventually("worker connected", Duration::from_secs(15), || {
        let st = st.clone();
        async move { temporal::is_connected(&st) }
    })
    .await;
    let _s0 = FakeServer::start(&st, 0, "a", &[]);
    slots_up(&st, &[0]).await;
    let token = guest(&st).await;
    let (_, body) = play(&st, &token).await;
    let (_, sid) = who(&body);
    let client = temporal::client(&st).unwrap();
    let d = client
        .get_workflow_handle::<PlayerSessionWorkflow>(temporal::session_id(
            &st.config.temporal_task_queue,
            &sid,
        ))
        .describe(WorkflowDescribeOptions::default())
        .await
        .unwrap();
    let timeout = d
        .raw_description
        .execution_config
        .as_ref()
        .and_then(|c| c.workflow_execution_timeout.as_ref())
        .map(|t| t.seconds);
    assert_eq!(timeout, Some(7200));
}

#[sqlx::test]
#[ignore = "needs Postgres + Temporal"]
async fn temporal_down(db: PgPool) {
    let st = build_state(config(Some("http://127.0.0.1:1".into())), db)
        .await
        .unwrap();
    let _worker = temporal::start_worker(st.clone());
    let token = guest(&st).await;
    let start = Instant::now();
    assert_eq!(
        play(&st, &token).await,
        (
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error":"temporal_unavailable"})
        )
    );
    assert!(start.elapsed() < Duration::from_secs(11));
    let heartbeat = json!({"slot": 0, "boot_id": "a", "players": []});
    assert_eq!(
        call(
            internal::router(st.clone()),
            Method::POST,
            "/internal/v1/servers/heartbeat",
            Some(TOKEN),
            Some(heartbeat)
        )
        .await,
        (
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error":"temporal_unavailable"})
        )
    );
    assert_eq!(
        call(create_api(st.clone()), Method::GET, "/ready", None, None).await,
        (
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"status":"starting"})
        )
    );
}

#[sqlx::test]
#[ignore = "needs Postgres + Temporal"]
async fn logged_out_ticket_refused(db: PgPool) {
    let (st, _worker) = connected(db).await;
    let s0 = FakeServer::start(&st, 0, "a", &[]);
    slots_up(&st, &[0]).await;
    let (token_a, token_b) = (guest(&st).await, guest(&st).await);
    let (_, a) = play(&st, &token_a).await;
    let (_, b) = play(&st, &token_b).await;
    logout(&st, &token_a).await;
    let (character_a, sid_a) = who(&a);
    let (character_b, sid_b) = who(&b);
    assert_eq!(
        s0.join(character_a, &sid_a).await,
        (StatusCode::CONFLICT, json!({"error":"session_ended"}))
    );
    assert_eq!(
        s0.join(character_b, &sid_b).await,
        (StatusCode::OK, json!({"result":"admitted"}))
    );
}

#[sqlx::test]
#[ignore = "needs Postgres + Temporal"]
async fn ready_needs_worker(db: PgPool) {
    let ready =
        |st: AppState| async move { call(create_api(st), Method::GET, "/ready", None, None).await };
    let st = build_state(config(Some(temporal_address())), db.clone())
        .await
        .unwrap();
    assert_eq!(
        ready(st.clone()).await,
        (
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"status":"starting"})
        )
    );
    let _worker = temporal::start_worker(st.clone());
    eventually("ready", Duration::from_secs(15), || {
        let st = st.clone();
        async move { ready(st).await.0 == StatusCode::OK }
    })
    .await;
    assert_eq!(ready(st).await, (StatusCode::OK, json!({"status":"ready"})));
    let without = build_state(config(None), db).await.unwrap();
    assert_eq!(
        ready(without).await,
        (StatusCode::OK, json!({"status":"ready"}))
    );
}

// ---------------------------------------------------------------- managed instances

fn fake_calls(st: &AppState) -> Vec<FakeCall> {
    st.fake_calls.lock().unwrap().clone()
}

/// Waits until the fake provisioner has been asked for `n` instances and returns them.
async fn provisioned(st: &AppState, n: usize) -> Vec<InstanceIn> {
    let list = || {
        fake_calls(st)
            .into_iter()
            .filter_map(|c| match c {
                FakeCall::Provision(i) => Some(i),
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    eventually(
        &format!("{n} instances provisioned"),
        Duration::from_secs(10),
        || {
            let found = list().len();
            async move { found >= n }
        },
    )
    .await;
    list()
}

/// Plays until the answer is no longer `server_starting`.
async fn play_until_ready(st: &AppState, token: &str) -> Value {
    let start = Instant::now();
    loop {
        let (status, body) = play(st, token).await;
        if status == StatusCode::OK {
            return body;
        }
        assert_eq!(body, json!({"error":"server_starting"}), "{status}");
        assert!(start.elapsed() < Duration::from_secs(10), "never ready");
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

async fn instance_status(st: &AppState, slot: u32, generation: u64) -> WorkflowExecutionStatus {
    temporal::client(st)
        .unwrap()
        .get_workflow_handle::<GameServerWorkflow>(temporal::server_id(
            &st.config.temporal_task_queue,
            slot,
            generation,
        ))
        .describe(WorkflowDescribeOptions::default())
        .await
        .unwrap()
        .status()
}

fn starting() -> (StatusCode, Value) {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        json!({"error":"server_starting"}),
    )
}

/// One spawned instance (slot 0, generation 1) with one joined player.
async fn one_instance_one_player(st: &AppState) -> (FakeServer, String, i64, String) {
    let token = guest(st).await;
    assert_eq!(play(st, &token).await, starting());
    let calls = provisioned(st, 1).await;
    assert_eq!((calls[0].slot, calls[0].generation), (0, 1));
    let server = FakeServer::start_instance(st, 0, 1, "a", &[]);
    let body = play_until_ready(st, &token).await;
    let (character, sid) = who(&body);
    assert_eq!(
        server.join(character, &sid).await,
        (StatusCode::OK, json!({"result":"admitted"}))
    );
    status_becomes(st, &sid, in_game(0)).await;
    (server, token, character, sid)
}

#[sqlx::test]
#[ignore = "needs Postgres + Temporal"]
async fn new_player_gets_a_new_tutorial_instance(db: PgPool) {
    let (st, _worker) = connected_with(db, ProvisionerKind::Fake).await;
    let (a, b) = (guest(&st).await, guest(&st).await);
    assert_eq!(play(&st, &a).await, starting());
    assert_eq!(play(&st, &b).await, starting());
    let calls = provisioned(&st, 2).await;
    assert_eq!(
        calls
            .iter()
            .map(|c| (c.slot, c.generation))
            .collect::<Vec<_>>(),
        vec![(0, 1), (1, 2)]
    );
    let codes: Vec<&str> = calls
        .iter()
        .map(|c| c.join_code.as_deref().unwrap())
        .collect();
    assert!(
        codes.iter().all(|c| c.len() == 4) && codes[0] != codes[1],
        "{codes:?}"
    );
    // Asking again while it starts spawns nothing new.
    assert_eq!(play(&st, &a).await, starting());
    assert_eq!(provisioned(&st, 2).await.len(), 2);

    let s0 = FakeServer::start_instance(&st, 0, 1, "a", &[]);
    let _s1 = FakeServer::start_instance(&st, 1, 2, "b", &[]);
    let body = play_until_ready(&st, &a).await;
    assert_eq!(body["server"]["slot"], 0, "{body}");
    assert_eq!(body["server"]["udp"], "localhost:36000");
    assert_eq!(body["server"]["ws_path"], "/ws/0");
    assert_eq!(body["server"]["join_code"], codes[0]);
    assert_eq!(body["server"]["paused"], false);
    assert_eq!(claims(body["ticket"].as_str().unwrap())["srv"], 0);
    let (character, sid) = who(&body);
    assert_eq!(s0.join(character, &sid).await.0, StatusCode::OK);
    status_becomes(&st, &sid, in_game(0)).await;
    assert_eq!(play_until_ready(&st, &b).await["server"]["slot"], 1);
}

/// The pool is filled by the coordinator itself; the next player is placed on a warm
/// instance on the first `/play` (no `server_starting`), and the hand-out refills the pool.
#[sqlx::test]
#[ignore = "needs Postgres + Temporal"]
async fn the_second_player_is_placed_on_the_warm_instance(db: PgPool) {
    let (st, _worker) = connected_config(db, ProvisionerKind::Fake, 1).await;
    let first = guest(&st).await;
    assert_eq!(play(&st, &first).await, starting());
    let calls = provisioned(&st, 2).await;
    // One instance for the player and one for the pool. Which slot the pool takes depends
    // on whether the coordinator's own refill (it runs as soon as the directory is
    // rebuilt, before the first placement) or the placement's refill spawns first.
    let player = calls
        .iter()
        .find(|c| !c.pre_game)
        .expect("one instance for the player");
    let pooled = calls
        .iter()
        .find(|c| c.pre_game)
        .expect("one instance for the pool");
    assert_ne!(
        (player.slot, player.generation),
        (pooled.slot, pooled.generation)
    );

    let _player_server = FakeServer::start_instance(&st, player.slot, player.generation, "a", &[]);
    let body = play_until_ready(&st, &first).await;
    assert_eq!(body["server"]["slot"], player.slot, "{body}");
    assert_eq!(body["server"]["paused"], false);
    let _pool_server = FakeServer::start_instance(&st, pooled.slot, pooled.generation, "b", &[]);
    slots_up(&st, &[player.slot, pooled.slot]).await;

    // A second guest is on the warm instance on the first call.
    let second = guest(&st).await;
    let started = Instant::now();
    let (status, body) = play(&st, &second).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "took {:?}",
        started.elapsed()
    );
    assert_eq!(body["server"]["slot"], pooled.slot, "{body}");
    assert_eq!(body["server"]["paused"], false);
    // The hand-out reached the instance's workflow: the pool instance is activated.
    eventually("pool instance activated", Duration::from_secs(10), || {
        let st = st.clone();
        async move {
            !temporal::client(&st)
                .unwrap()
                .get_workflow_handle::<GameServerWorkflow>(temporal::server_id(
                    &st.config.temporal_task_queue,
                    pooled.slot,
                    pooled.generation,
                ))
                .query(
                    GameServerWorkflow::server,
                    (),
                    WorkflowQueryOptions::default(),
                )
                .await
                .unwrap()
                .warm
        }
    })
    .await;

    // Handing it out refilled the pool: one new instance, in pre-game, on the next slot.
    let calls = provisioned(&st, 3).await;
    assert_eq!(calls.len(), 3, "{calls:?}");
    assert!(calls[2].pre_game, "{calls:?}");
    assert_eq!((calls[2].slot, calls[2].generation), (2, 3));
    // The handed-out instance is out of the pool.
    let f = fleet(&st).await.unwrap();
    assert!(!f.servers[&pooled.slot].warm);
    assert_eq!(f.servers.values().filter(|e| e.warm).count(), 1);
}

#[sqlx::test]
#[ignore = "needs Postgres + Temporal"]
async fn stale_generation_gets_unknown_instance(db: PgPool) {
    let (st, _worker) = connected_with(db, ProvisionerKind::Fake).await;
    let token = guest(&st).await;
    let sid = sessions::hash_token(&token)[..16].to_owned();
    let unknown = (StatusCode::CONFLICT, json!({"error":"unknown_instance"}));
    let report = |path: &'static str, body: Value| {
        let st = st.clone();
        async move {
            call(
                internal::router(st),
                Method::POST,
                path,
                Some(TOKEN),
                Some(body),
            )
            .await
        }
    };
    let id = json!({"slot": 0, "generation": 9, "boot_id": "z"});
    let with = |extra: Value| {
        let mut body = id.clone();
        body.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        body
    };
    assert_eq!(
        report(
            "/internal/v1/servers/heartbeat",
            with(json!({"players": []}))
        )
        .await,
        unknown
    );
    assert_eq!(
        report(
            "/internal/v1/servers/join",
            with(json!({"character_id": 1, "sid": sid}))
        )
        .await,
        unknown
    );
    assert_eq!(
        report(
            "/internal/v1/servers/leave",
            with(json!({"character_id": 1}))
        )
        .await,
        unknown
    );
    assert_eq!(
        report(
            "/internal/v1/servers/shutdown-request",
            with(json!({"reason": "idle", "players": []}))
        )
        .await,
        unknown
    );
}

#[sqlx::test]
#[ignore = "needs Postgres + Temporal"]
async fn static_mode_places_like_today(db: PgPool) {
    let (st, _worker) = connected(db).await;
    let _s0 = FakeServer::start(&st, 0, "a", &[]);
    let _s1 = FakeServer::start(&st, 1, "b", &[]);
    slots_up(&st, &[0, 1]).await;
    let mut slots = Vec::new();
    for _ in 0..2 {
        let token = guest(&st).await;
        let (status, body) = play(&st, &token).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let slot = body["server"]["slot"].as_u64().unwrap();
        assert_eq!(body["server"]["udp"], format!("localhost:{}", 25555 + slot));
        assert_eq!(body["server"]["join_code"], Value::Null);
        slots.push(slot);
    }
    slots.sort();
    assert_eq!(slots, vec![0, 1], "least-loaded spreads players");
    assert!(fake_calls(&st).is_empty(), "static never spawns");
    for slot in [0, 1] {
        assert_eq!(
            instance_status(&st, slot, 1).await,
            WorkflowExecutionStatus::Running
        );
    }
}

#[sqlx::test]
#[ignore = "needs Postgres + Temporal"]
async fn idle_shutdown_destroys_the_instance(db: PgPool) {
    let (st, _worker) = connected_with(db, ProvisionerKind::Fake).await;
    let (server, token, _, sid) = one_instance_one_player(&st).await;
    assert_eq!(
        server.shutdown("idle").await,
        (StatusCode::OK, json!({"plan":"destroy"}))
    );
    let deleted = InstanceIn {
        slot: 0,
        generation: 1,
        join_code: provisioned(&st, 1).await[0].join_code.clone(),
        pre_game: false,
    };
    eventually("instance deleted", Duration::from_secs(15), || {
        let found = fake_calls(&st).contains(&FakeCall::Delete(deleted.clone()));
        async move { found }
    })
    .await;
    eventually(
        "instance workflow completed",
        Duration::from_secs(5),
        || {
            let st = st.clone();
            async move { instance_status(&st, 0, 1).await == WorkflowExecutionStatus::Completed }
        },
    )
    .await;
    status_becomes(&st, &sid, in_menus()).await;
    eventually("slot freed", Duration::from_secs(5), || {
        let st = st.clone();
        async move {
            fleet(&st)
                .await
                .is_some_and(|f| f.servers.is_empty() && f.index.is_empty())
        }
    })
    .await;
    // A coordinator that restarts now (a rollout) finds no running instance, but must
    // still not reuse generation 1: that workflow id is taken by the closed instance.
    temporal::client(&st)
        .unwrap()
        .get_workflow_handle::<CoordinatorWorkflow>(temporal::coordinator_id(
            &st.config.temporal_task_queue,
        ))
        .terminate(WorkflowTerminateOptions::builder().reason("test").build())
        .await
        .unwrap();
    // The world is gone: the next play spawns a new instance on the freed slot.
    assert_eq!(play(&st, &token).await, starting());
    let calls = provisioned(&st, 2).await;
    assert_eq!((calls[1].slot, calls[1].generation), (0, 2));
    assert_eq!(
        instance_status(&st, 0, 2).await,
        WorkflowExecutionStatus::Running
    );
    let heartbeat = json!({"slot": 0, "generation": 1, "boot_id": "a", "players": []});
    assert_eq!(
        call(
            internal::router(st.clone()),
            Method::POST,
            "/internal/v1/servers/heartbeat",
            Some(TOKEN),
            Some(heartbeat)
        )
        .await,
        (StatusCode::CONFLICT, json!({"error":"unknown_instance"}))
    );
}

#[sqlx::test]
#[ignore = "needs Postgres + Temporal"]
async fn fresh_coordinator_rebuilds_directory_from_running_servers(db: PgPool) {
    let (st, _worker) = connected_with(db, ProvisionerKind::Fake).await;
    let (a, b) = (guest(&st).await, guest(&st).await);
    assert_eq!(play(&st, &a).await, starting());
    assert_eq!(play(&st, &b).await, starting());
    provisioned(&st, 2).await;
    let s0 = FakeServer::start_instance(&st, 0, 1, "a", &[]);
    let s1 = FakeServer::start_instance(&st, 1, 2, "b", &[]);
    for (token, server, slot) in [(&a, &s0, 0), (&b, &s1, 1)] {
        let body = play_until_ready(&st, token).await;
        let (character, sid) = who(&body);
        assert_eq!(server.join(character, &sid).await.0, StatusCode::OK);
        status_becomes(&st, &sid, in_game(slot)).await;
    }
    temporal::client(&st)
        .unwrap()
        .get_workflow_handle::<CoordinatorWorkflow>(temporal::coordinator_id(
            &st.config.temporal_task_queue,
        ))
        .terminate(WorkflowTerminateOptions::builder().reason("test").build())
        .await
        .unwrap();
    // The returning character is placed on its own instance, not a new one.
    let (status, body) = play(&st, &a).await;
    assert_eq!(
        (status, body["server"]["slot"].clone()),
        (StatusCode::OK, json!(0)),
        "{body}"
    );
    // A new player gets the next free slot with a generation above both.
    let c = guest(&st).await;
    assert_eq!(play(&st, &c).await, starting());
    let calls = provisioned(&st, 3).await;
    assert_eq!((calls[2].slot, calls[2].generation), (2, 3));
}

/// A full managed lifecycle (spawn, ready, join, leave, SIGTERM to the menu, destroy),
/// then every workflow's history is replayed against this code.
#[sqlx::test]
#[ignore = "needs Postgres + Temporal"]
async fn live_coordinator_and_server_replay(db: PgPool) {
    let (st, _worker) = connected_with(db, ProvisionerKind::Fake).await;
    let (server, _, character, sid) = one_instance_one_player(&st).await;
    server.leave(character);
    status_becomes(&st, &sid, in_menus()).await;
    // Rejoin on the still-valid ticket, then SIGTERM with the player on board.
    assert_eq!(server.join(character, &sid).await.0, StatusCode::OK);
    status_becomes(&st, &sid, in_game(0)).await;
    assert_eq!(
        server.shutdown("sigterm").await,
        (StatusCode::OK, json!({"plan":"to_menu"}))
    );
    eventually(
        "instance workflow completed",
        Duration::from_secs(25),
        || {
            let st = st.clone();
            async move { instance_status(&st, 0, 1).await == WorkflowExecutionStatus::Completed }
        },
    )
    .await;
    status_becomes(&st, &sid, in_menus()).await;
    replays_cleanly(&st, &[(0, 1)], &[&sid]).await;
}

// ---------------------------------------------------------------- forced failure

fn calls_of(st: &AppState, pick: fn(&FakeCall) -> bool) -> usize {
    fake_calls(st).iter().filter(|c| pick(c)).count()
}

#[sqlx::test]
#[ignore = "needs Postgres + Temporal"]
async fn lease_expiry_without_a_probe_failure_kills_nothing(db: PgPool) {
    let (st, _worker) = connected_with(db, ProvisionerKind::Fake).await;
    let (server, _, _, sid) = one_instance_one_player(&st).await;
    // Heartbeats stop (the authserver cannot hear the server), but the server answers.
    drop(server);
    eventually("two probes", Duration::from_secs(15), || {
        let probes = calls_of(&st, |c| matches!(c, FakeCall::Probe(_)));
        async move { probes >= 2 }
    })
    .await;
    assert_eq!(
        calls_of(&st, |c| matches!(
            c,
            FakeCall::Kill(_) | FakeCall::Delete(_)
        )),
        0
    );
    assert_eq!(
        instance_status(&st, 0, 1).await,
        WorkflowExecutionStatus::Running
    );
    assert_eq!(status(&st, &sid).await, in_game(0));
}

#[sqlx::test]
#[ignore = "needs Postgres + Temporal"]
async fn crash_report_triggers_a_probe(db: PgPool) {
    let (st, _worker) = connected_with(db, ProvisionerKind::Fake).await;
    let (server, token, character, sid) = one_instance_one_player(&st).await;
    // The server hangs: no heartbeats, no answer to the probe.
    drop(server);
    st.fake_alive
        .store(false, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        call(
            create_api(st.clone()),
            Method::POST,
            "/api/v1/my-server-crashed",
            Some(&token),
            Some(json!({}))
        )
        .await,
        (StatusCode::ACCEPTED, json!({"status":"accepted"}))
    );
    eventually("pod killed", Duration::from_secs(10), || {
        let kills = calls_of(&st, |c| matches!(c, FakeCall::Kill(_)));
        async move { kills == 1 }
    })
    .await;
    assert!(calls_of(&st, |c| matches!(c, FakeCall::Probe(_))) >= 1);
    eventually("play says recovering", Duration::from_secs(5), || {
        let st = st.clone();
        let token = token.clone();
        async move {
            play(&st, &token).await
                == (
                    StatusCode::SERVICE_UNAVAILABLE,
                    json!({"error":"server_recovering"}),
                )
        }
    })
    .await;
    // The pod restarts on the same volume and restores the character as a ghost.
    let restarted = FakeServer::start_instance(&st, 0, 1, "a2", &[(character, &sid)]);
    eventually("same instance ready again", Duration::from_secs(10), || {
        let st = st.clone();
        let token = token.clone();
        async move {
            let (status, body) = play(&st, &token).await;
            status == StatusCode::OK && body["server"]["slot"] == 0
        }
    })
    .await;
    assert_eq!(
        restarted.join(character, &sid).await,
        (StatusCode::OK, json!({"result":"admitted"}))
    );
    status_becomes(&st, &sid, in_game(0)).await;
    assert_eq!(calls_of(&st, |c| matches!(c, FakeCall::Delete(_))), 0);
    assert_eq!(provisioned(&st, 1).await.len(), 1, "no new instance");
    replays_cleanly(&st, &[(0, 1)], &[&sid]).await;
}
