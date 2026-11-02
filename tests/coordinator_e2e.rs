//! End-to-end: the public and internal routers against a real Postgres (a fresh database
//! per test, via `#[sqlx::test]`) and a real Temporal dev server, with fake game servers
//! that report like bevy2025's (one ordered sender, heartbeat every 500 ms).
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
    WorkflowExecutionStatus, WorkflowFetchHistoryOptions, WorkflowQueryOptions,
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
        coordinator::{CoordinatorWorkflow, FleetState},
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
        game_udp_port_base: 25555,
        coordinator_stale_secs: 2,
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
    let st = build_state(config(Some(temporal_address())), db)
        .await
        .unwrap();
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
                fleet(&st)
                    .await
                    .is_some_and(|f| slots.iter().all(|s| f.slots.contains_key(s)))
            }
        },
    )
    .await;
}

/// Replays the coordinator's and the sessions' histories against the current code.
async fn replays_cleanly(st: &AppState, sids: &[&str]) {
    let client = temporal::client(st).unwrap();
    let queue = &st.config.temporal_task_queue;
    let mut histories = vec![
        client
            .get_workflow_handle::<CoordinatorWorkflow>(temporal::coordinator_id(queue))
            .fetch_history(WorkflowFetchHistoryOptions::default()),
    ];
    for sid in sids {
        histories.push(
            client
                .get_workflow_handle::<PlayerSessionWorkflow>(temporal::session_id(queue, sid))
                .fetch_history(WorkflowFetchHistoryOptions::default()),
        );
    }
    let replayer = WorkflowReplayer::new(
        WorkflowReplayerOptions::new()
            .register_workflow::<CoordinatorWorkflow>()
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
    fn start(st: &AppState, slot: u32, boot_id: &str, roster: &[(i64, &str)]) -> Self {
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
                        let body = json!({"slot": slot, "boot_id": boot_id, "players": players});
                        let sent = Instant::now();
                        let (status, _) = call(router.clone(), Method::POST, "/internal/v1/servers/heartbeat", Some(TOKEN), Some(body)).await;
                        if status != StatusCode::NO_CONTENT || sent.elapsed() > Duration::from_millis(500) {
                            eprintln!("fake server {slot}: heartbeat {status} after {:?}", sent.elapsed());
                        }
                    }
                    op = rx.recv() => match op {
                        Some(Op::Join { character_id, sid, reply }) => {
                            players.insert(character_id, sid.clone());
                            let body = json!({"slot": slot, "boot_id": boot_id, "character_id": character_id, "sid": sid});
                            let answer = call(router.clone(), Method::POST, "/internal/v1/servers/join", Some(TOKEN), Some(body)).await;
                            if answer.0 != StatusCode::OK {
                                players.remove(&character_id);
                            }
                            let _ = reply.send(answer);
                        }
                        Some(Op::Leave { character_id }) => {
                            players.remove(&character_id);
                            let body = json!({"slot": slot, "boot_id": boot_id, "character_id": character_id});
                            call(router.clone(), Method::POST, "/internal/v1/servers/leave", Some(TOKEN), Some(body)).await;
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
    replays_cleanly(&st, &[&sid]).await;
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
    replays_cleanly(&st, &[&sid]).await;
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
