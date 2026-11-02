//! Internal API for game servers, served on `INTERNAL_PORT` only (never through the
//! gateway). Every route needs `Authorization: Bearer $COORDINATOR_TOKEN` and turns
//! the report into an update (join) or signal (heartbeat, leave) on the coordinator
//! workflow. Game servers refuse a join they cannot get an answer for, so every 503
//! here is a critical game-server event and logs `error!`.
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
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use subtle::ConstantTimeEq;

use crate::{
    api::{ApiError, AppState},
    sessions,
    temporal::{
        self,
        coordinator::{CoordinatorWorkflow, HeartbeatIn, JoinIn, JoinOutcome, LeaveIn},
    },
};

/// Per call; the game server gives up on a join after 6 s.
const REPORT_TIMEOUT: Duration = Duration::from_secs(4);

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/internal/v1/servers/heartbeat", post(heartbeat))
        .route("/internal/v1/servers/join", post(join))
        .route("/internal/v1/servers/leave", post(leave))
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

async fn report<T>(
    path: &'static str,
    call: impl Future<Output = anyhow::Result<T>>,
) -> Result<T, ApiError> {
    let unavailable = ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "temporal_unavailable");
    match tokio::time::timeout(REPORT_TIMEOUT, call).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(e)) => {
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
    report(
        "heartbeat",
        temporal::coordinator_signal(&st, CoordinatorWorkflow::heartbeat, input),
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
    match report(
        "join",
        temporal::coordinator_update(&st, CoordinatorWorkflow::join, input),
    )
    .await?
    {
        JoinOutcome::Admitted => Ok(Json(json!({ "result": "admitted" }))),
        JoinOutcome::AlreadyOnOtherServer { .. } => Err(ApiError::new(
            StatusCode::CONFLICT,
            "already_on_other_server",
        )),
    }
}

async fn leave(State(st): State<AppState>, body: Bytes) -> Result<StatusCode, ApiError> {
    let input: LeaveIn = parse(&body)?;
    check_slot(&st, input.slot)?;
    report(
        "leave",
        temporal::coordinator_signal(&st, CoordinatorWorkflow::leave, input),
    )
    .await?;
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
