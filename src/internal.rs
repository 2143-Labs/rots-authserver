//! Internal API for game servers, served on `INTERNAL_PORT` only (never through the
//! gateway). Every route needs `Authorization: Bearer $COORDINATOR_TOKEN` and turns
//! the report into a signal (heartbeat, leave) or update (join, shutdown-request) on
//! the instance's workflow, `rots-gs-<queue>-<slot>-g<generation>`. Static servers
//! (`GAME_SERVER_PROVISIONER=static`) start that workflow with their reports; a report
//! for a managed instance whose workflow is not running gets 409 `unknown_instance`,
//! which tells the server to exit. Game servers refuse a join they cannot get an answer
//! for, so every 503 here is a critical game-server event and logs `error!`.
use std::{future::Future, time::Duration};

use axum::{
    Json, Router,
    body::Bytes,
    extract::{Request, State},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::post,
};
use http::{StatusCode, header};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};
use subtle::ConstantTimeEq;

use crate::{
    api::{ApiError, AppState},
    sessions,
    temporal::{
        self, ServerCallError,
        activities::ProvisionerKind,
        coordinator::{JoinOutcome, ShutdownPlan},
        server::{GameServerWorkflow, HeartbeatIn, JoinIn, LeaveIn, ShutdownRequestIn},
    },
};

/// Per call; the game server gives up on a join after 6 s.
const REPORT_TIMEOUT: Duration = Duration::from_secs(4);

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/internal/v1/servers/heartbeat", post(heartbeat))
        .route("/internal/v1/servers/join", post(join))
        .route("/internal/v1/servers/leave", post(leave))
        .route(
            "/internal/v1/servers/shutdown-request",
            post(shutdown_request),
        )
        .route("/internal/v1/servers/tutorial-done", post(tutorial_done))
        .layer(middleware::from_fn_with_state(state.clone(), require_token))
        .with_state(state)
}

async fn require_token(State(st): State<AppState>, req: Request, next: Next) -> Response {
    let presented = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .unwrap_or("");
    let expected = st.config.coordinator_token.as_bytes();
    if expected.is_empty() || !bool::from(presented.as_bytes().ct_eq(expected)) {
        return ApiError::new(StatusCode::UNAUTHORIZED, "bad_coordinator_token").into_response();
    }
    next.run(req).await
}

fn parse<T: DeserializeOwned>(body: &Bytes) -> Result<T, ApiError> {
    serde_json::from_slice(body).map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "bad_request"))
}

fn check_slot(st: &AppState, slot: u32) -> Result<(), ApiError> {
    if slot >= st.config.game_server_slots {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "bad_slot"));
    }
    Ok(())
}

fn unknown_instance() -> ApiError {
    ApiError::new(StatusCode::CONFLICT, "unknown_instance")
}

async fn report<T>(
    path: &'static str,
    call: impl Future<Output = Result<T, ServerCallError>>,
) -> Result<T, ApiError> {
    let unavailable = ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "temporal_unavailable");
    match tokio::time::timeout(REPORT_TIMEOUT, call).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(ServerCallError::NotFound)) => Err(unknown_instance()),
        Ok(Err(ServerCallError::Other(e))) => {
            tracing::error!(path, "game-server report failed: {e:#}");
            Err(unavailable)
        }
        Err(_) => {
            tracing::error!(
                path,
                "game-server report timed out after {REPORT_TIMEOUT:?}"
            );
            Err(unavailable)
        }
    }
}

async fn heartbeat(State(st): State<AppState>, body: Bytes) -> Result<StatusCode, ApiError> {
    let input: HeartbeatIn = parse(&body)?;
    check_slot(&st, input.slot)?;
    let (slot, generation) = (input.slot, input.generation);
    report(
        "heartbeat",
        temporal::server_signal(&st, slot, generation, GameServerWorkflow::heartbeat, input),
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn join(State(st): State<AppState>, body: Bytes) -> Result<Json<Value>, ApiError> {
    let input: JoinIn = parse(&body)?;
    check_slot(&st, input.slot)?;
    match sessions::session_live(&st.db, &input.sid).await {
        Ok(true) => {}
        Ok(false) => return Err(ApiError::new(StatusCode::CONFLICT, "session_ended")),
        Err(e) => {
            tracing::error!(path = "join", "game-server report failed: {e:#}");
            return Err(ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "db_unavailable",
            ));
        }
    }
    let (slot, generation) = (input.slot, input.generation);
    match report(
        "join",
        temporal::server_update(&st, slot, generation, GameServerWorkflow::join, input),
    )
    .await?
    {
        JoinOutcome::Admitted => Ok(Json(json!({ "result": "admitted" }))),
        JoinOutcome::AlreadyOnOtherServer { .. } => Err(ApiError::new(
            StatusCode::CONFLICT,
            "already_on_other_server",
        )),
        JoinOutcome::ServerDraining => Err(ApiError::new(StatusCode::CONFLICT, "server_draining")),
        JoinOutcome::UnknownInstance => Err(unknown_instance()),
        JoinOutcome::Failed { reason } => {
            tracing::error!(path = "join", "game-server report failed: {reason}");
            Err(ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "temporal_unavailable",
            ))
        }
    }
}

async fn leave(State(st): State<AppState>, body: Bytes) -> Result<StatusCode, ApiError> {
    let input: LeaveIn = parse(&body)?;
    check_slot(&st, input.slot)?;
    let (slot, generation) = (input.slot, input.generation);
    let sent = report(
        "leave",
        temporal::server_signal_existing(&st, slot, generation, GameServerWorkflow::leave, input),
    )
    .await;
    match sent {
        // A static server's workflow ends when it goes silent, taking its players with it.
        Err(e)
            if e.code() == "unknown_instance"
                && st.config.game_server_provisioner == ProvisionerKind::Static =>
        {
            Ok(StatusCode::NO_CONTENT)
        }
        Err(e) => Err(e),
        Ok(()) => Ok(StatusCode::NO_CONTENT),
    }
}

/// The server decided to shut down (idle, or SIGTERM); the answer says what to do with
/// its players.
async fn shutdown_request(
    State(st): State<AppState>,
    body: Bytes,
) -> Result<Json<ShutdownPlan>, ApiError> {
    let input: ShutdownRequestIn = parse(&body)?;
    check_slot(&st, input.slot)?;
    let (slot, generation) = (input.slot, input.generation);
    let plan = report(
        "shutdown-request",
        temporal::server_update(&st, slot, generation, GameServerWorkflow::shutdown, input),
    )
    .await?;
    Ok(Json(plan))
}

#[derive(Deserialize)]
struct DumpIn {
    character_id: i64,
    units: serde_json::Value,
}

#[derive(Deserialize)]
struct TutorialDoneIn {
    slot: u32,
    generation: u64,
    characters: Vec<DumpIn>,
}

/// A tutorial's flag was reached: save every character in it. Characters with no row
/// (unverified connections) are skipped.
async fn tutorial_done(State(st): State<AppState>, body: Bytes) -> Result<StatusCode, ApiError> {
    let input: TutorialDoneIn = parse(&body)?;
    check_slot(&st, input.slot)?;
    for c in &input.characters {
        sqlx::query!(
            r#"INSERT INTO social.character_states (character_id, units, saved_from, slot, generation)
               SELECT $1, $2, 'tutorial_complete', $3, $4
               WHERE EXISTS (SELECT 1 FROM social.characters WHERE character_id = $1)
               ON CONFLICT (character_id) DO UPDATE SET units = EXCLUDED.units,
                 saved_from = EXCLUDED.saved_from, slot = EXCLUDED.slot,
                 generation = EXCLUDED.generation, saved_at = now()"#,
            c.character_id,
            sqlx::types::Json(&c.units) as _,
            input.slot as i32,
            input.generation as i64,
        )
        .execute(&st.db)
        .await
        .map_err(|e| {
            tracing::error!(path = "tutorial-done", "game-server report failed: {e:#}");
            ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "db_unavailable")
        })?;
    }
    tracing::info!(
        slot = input.slot,
        generation = input.generation,
        characters = input.characters.len(),
        "tutorial complete: characters saved"
    );
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::Body;
    use http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;
    use crate::{Config, providers::Providers, tickets::TicketKeys};

    /// No database or Temporal: these requests must be answered before either is used.
    async fn state() -> AppState {
        let config = Config {
            public_url: "http://localhost:8000".into(),
            disabled_providers: "steam,discord,au2143".into(),
            coordinator_token: "devtoken".into(),
            game_server_slots: 2,
            ..Default::default()
        };
        let http = reqwest::Client::new();
        let providers = Providers::from_config(&config, &http).await;
        AppState {
            config: Arc::new(config),
            db: sqlx::PgPool::connect_lazy("postgres://localhost/unused").unwrap(),
            tickets: Arc::new(TicketKeys::from_seed(&[1; 32]).unwrap()),
            providers: Arc::new(providers),
            http,
            temporal: Default::default(),
            fake_calls: Default::default(),
            fake_alive: Arc::new(true.into()),
        }
    }

    async fn post(bearer: Option<&str>, body: &str) -> (StatusCode, Value) {
        let mut req = Request::post("/internal/v1/servers/heartbeat");
        if let Some(b) = bearer {
            req = req.header(header::AUTHORIZATION, format!("Bearer {b}"));
        }
        let resp = router(state().await)
            .oneshot(req.body(Body::from(body.to_owned())).unwrap())
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    const HEARTBEAT: &str = r#"{"slot":0,"boot_id":"a","players":[]}"#;

    #[tokio::test]
    async fn wrong_or_missing_bearer_is_401() {
        for bearer in [Some("devtokem"), Some("devtoken2"), Some(""), None] {
            assert_eq!(
                post(bearer, HEARTBEAT).await,
                (
                    StatusCode::UNAUTHORIZED,
                    json!({"error":"bad_coordinator_token"})
                ),
                "{bearer:?}"
            );
        }
    }

    #[tokio::test]
    async fn slot_outside_the_fleet_is_400() {
        assert_eq!(
            post(Some("devtoken"), r#"{"slot":2,"boot_id":"a","players":[]}"#).await,
            (StatusCode::BAD_REQUEST, json!({"error":"bad_slot"}))
        );
    }

    #[tokio::test]
    async fn malformed_body_is_400() {
        assert_eq!(
            post(Some("devtoken"), r#"{"slot":"zero"}"#).await,
            (StatusCode::BAD_REQUEST, json!({"error":"bad_request"}))
        );
    }

    #[tokio::test]
    async fn temporal_down_is_503() {
        assert_eq!(
            post(Some("devtoken"), HEARTBEAT).await,
            (
                StatusCode::SERVICE_UNAVAILABLE,
                json!({"error":"temporal_unavailable"})
            )
        );
    }
}
