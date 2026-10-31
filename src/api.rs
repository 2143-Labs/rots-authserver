use std::{collections::HashMap, sync::Arc};

use anyhow::{Context, bail};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, Query, State},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use http::{StatusCode, header};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::PgPool;
use tower_http::normalize_path::NormalizePathLayer;

use crate::{
    Config,
    providers::{self, ProviderConfig, Providers},
    sessions::{self, SESSION_COOKIE, SESSION_TTL_DAYS, Session},
    tickets::{GameTicketClaims, TICKET_ISSUER, TICKET_TTL_SECS, TicketKeys},
};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub db: PgPool,
    pub tickets: Arc<TicketKeys>,
    pub providers: Arc<Providers>,
    pub http: reqwest::Client,
}

/// The gateway strips `/auth`, so these are served at the root.
pub fn create_api(state: AppState) -> Router {
    let api_routes = Router::new()
        .route("/providers", get(list_providers))
        .route("/ticket/public-key", get(ticket_public_key))
        .route("/guest", post(create_guest))
        .route("/me", get(me))
        .route("/ticket", post(issue_ticket))
        .route("/steam/ticket", post(steam_ticket_login))
        .route("/steam/callback", get(steam_callback))
        .route("/{provider}/login", get(provider_login))
        .route("/{provider}/callback", get(oauth_callback))
        .route("/claim", post(claim))
        .route("/logout", post(logout));

    Router::new()
        .route("/health", get(health))
        .nest("/api/v1", api_routes)
        .with_state(state)
        .layer(NormalizePathLayer::trim_trailing_slash())
}

/// Returns 200, always
async fn health() -> (StatusCode, &'static str) {
    (StatusCode::OK, "Ok")
}

/// JSON `{"error":"<code>"}` with a status; logged when turned into a response.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
}

impl ApiError {
    pub fn new(status: StatusCode, code: &'static str) -> Self {
        Self { status, code }
    }

    pub fn internal(err: anyhow::Error) -> Self {
        tracing::warn!("internal error: {err:#}");
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal")
    }

    fn no_session() -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "no_session")
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(err: anyhow::Error) -> Self {
        Self::internal(err)
    }
}

impl From<sqlx::Error> for ApiError {
    fn from(err: sqlx::Error) -> Self {
        Self::internal(err.into())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        tracing::warn!(status = %self.status, error = self.code, "request failed");
        (self.status, Json(json!({ "error": self.code }))).into_response()
    }
}

fn session_cookie(token: &str) -> String {
    format!(
        "{SESSION_COOKIE}={token}; Path=/auth; HttpOnly; Secure; SameSite=Lax; Max-Age={}",
        i64::from(SESSION_TTL_DAYS) * 86_400
    )
}

fn found(location: String) -> Response {
    (StatusCode::FOUND, [(header::LOCATION, location)]).into_response()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ClientKind {
    Web,
    Native,
}

/// Native clients get the session token in the body; web clients get the
/// `rots_session` cookie instead.
fn session_response<T: Serialize>(
    client: ClientKind,
    token: String,
    body: impl FnOnce(Option<String>) -> T,
) -> Response {
    match client {
        ClientKind::Native => Json(body(Some(token))).into_response(),
        ClientKind::Web => (
            [(header::SET_COOKIE, session_cookie(&token))],
            Json(body(None)),
        )
            .into_response(),
    }
}

fn callback_url(config: &Config, provider: &str) -> String {
    format!("{}/auth/api/v1/{provider}/callback", config.public_url)
}

async fn list_providers(State(st): State<AppState>) -> Json<Value> {
    Json(json!({ "providers": st.providers.enabled() }))
}

async fn ticket_public_key(State(st): State<AppState>) -> Json<Value> {
    Json(json!({ "alg": "EdDSA", "x": st.tickets.public_key_b64url }))
}

#[derive(Deserialize)]
struct ClientBody {
    client: ClientKind,
}

#[derive(Serialize)]
struct GuestResponse {
    character_id: i64,
    claim_code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_token: Option<String>,
}

async fn create_guest(State(st): State<AppState>, body: Bytes) -> Result<Response, ApiError> {
    let ClientBody { client } = serde_json::from_slice(&body)
        .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "bad_client"))?;
    let (character_id, claim_code, token) = sessions::create_guest(&st.db).await?;
    Ok(session_response(client, token, |session_token| {
        GuestResponse {
            character_id,
            claim_code,
            session_token,
        }
    }))
}

#[derive(Serialize)]
struct MeResponse {
    kind: &'static str,
    character_id: i64,
    character_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    account_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    claim_code: Option<String>,
}

async fn me(
    State(st): State<AppState>,
    Session(session): Session,
) -> Result<Json<MeResponse>, ApiError> {
    let s = session.ok_or_else(ApiError::no_session)?;
    let row = sqlx::query!(
        r#"SELECT c.name, c.claim_code, a.display_name AS "display_name?"
           FROM social.characters c
           LEFT JOIN social.accounts a ON a.account_id = $2
           WHERE c.character_id = $1"#,
        s.character_id,
        s.account_id
    )
    .fetch_optional(&st.db)
    .await?
    .ok_or_else(ApiError::no_session)?;
    Ok(Json(MeResponse {
        kind: if s.account_id.is_some() {
            "account"
        } else {
            "guest"
        },
        character_id: s.character_id,
        character_name: row.name,
        account_id: s.account_id,
        display_name: row.display_name,
        claim_code: row.claim_code.filter(|_| s.account_id.is_none()),
    }))
}

#[derive(Serialize)]
struct TicketResponse {
    ticket: String,
    expires_at: u64,
    character_id: i64,
    kind: &'static str,
}

async fn issue_ticket(
    State(st): State<AppState>,
    Session(session): Session,
) -> Result<Json<TicketResponse>, ApiError> {
    let s = session.ok_or_else(ApiError::no_session)?;
    let row = sqlx::query!(
        r#"SELECT c.name,
             (SELECT i.provider FROM social.account_identities i
              WHERE i.account_id = $2 ORDER BY i.created_at, i.provider LIMIT 1) AS "prov?"
           FROM social.characters c WHERE c.character_id = $1"#,
        s.character_id,
        s.account_id
    )
    .fetch_optional(&st.db)
    .await?
    .ok_or_else(ApiError::no_session)?;

    let kind = if s.account_id.is_some() {
        "account"
    } else {
        "guest"
    };
    let iat = jsonwebtoken::get_current_timestamp();
    let claims = GameTicketClaims {
        iss: TICKET_ISSUER.into(),
        aud: st.config.public_url.clone(),
        sub: s.character_id.to_string(),
        sid: s.token_hash[..16].to_owned(),
        kind: kind.into(),
        prov: row.prov.filter(|_| s.account_id.is_some()),
        acc: s.account_id,
        name: row.name,
        iat,
        exp: iat + TICKET_TTL_SECS,
    };
    Ok(Json(TicketResponse {
        ticket: st.tickets.sign(&claims)?,
        expires_at: claims.exp,
        character_id: s.character_id,
        kind,
    }))
}

#[derive(Deserialize)]
struct SteamTicketBody {
    steam_ticket_hex: String,
    client: ClientKind,
}

#[derive(Serialize)]
struct SteamTicketResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    session_token: Option<String>,
    character_id: i64,
}

async fn steam_ticket_login(
    State(st): State<AppState>,
    Session(session): Session,
    body: Bytes,
) -> Result<Response, ApiError> {
    let Some(api_key) = st.providers.steam_api_key.as_deref() else {
        return Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "provider_unavailable",
        ));
    };
    let body: SteamTicketBody = serde_json::from_slice(&body)
        .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "bad_request"))?;
    let steam_id = providers::verify_ticket(
        &st.http,
        api_key,
        st.providers.steam_app_id,
        &body.steam_ticket_hex,
        "rots-auth",
    )
    .await
    .map_err(|e| {
        tracing::warn!("steam ticket rejected: {e:#}");
        ApiError::new(StatusCode::UNAUTHORIZED, "steam_ticket_rejected")
    })?;
    let subject = steam_id.to_string();
    let (token, character_id) = sessions::login_account(
        &st.db,
        "steam",
        &subject,
        &format!("Steam {subject}"),
        session,
    )
    .await?;
    Ok(session_response(body.client, token, |session_token| {
        SteamTicketResponse {
            session_token,
            character_id,
        }
    }))
}

#[derive(Deserialize)]
struct ClaimBody {
    claim_code: String,
}

async fn claim(
    State(st): State<AppState>,
    Session(session): Session,
    body: Bytes,
) -> Result<Json<Value>, ApiError> {
    let s = session.ok_or_else(ApiError::no_session)?;
    let Some(account_id) = s.account_id else {
        return Err(ApiError::new(StatusCode::FORBIDDEN, "guest_cannot_claim"));
    };
    let ClaimBody { claim_code } = serde_json::from_slice(&body)
        .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "bad_request"))?;

    let mut tx = st.db.begin().await?;
    let character_id = sqlx::query_scalar!(
        "UPDATE social.characters SET account_id = $1, claim_code = NULL
         WHERE claim_code = upper($2) AND account_id IS NULL RETURNING character_id",
        account_id,
        claim_code.trim()
    )
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "invalid_claim_code"))?;
    // The character now belongs to the account; its guest sessions must not
    // keep minting guest tickets for it.
    sqlx::query!(
        "DELETE FROM social.sessions WHERE character_id = $1 AND account_id IS NULL",
        character_id
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Json(json!({ "character_id": character_id })))
}

async fn logout(
    State(st): State<AppState>,
    Session(session): Session,
) -> Result<Response, ApiError> {
    if let Some(s) = session {
        sqlx::query!(
            "DELETE FROM social.sessions WHERE token_hash = $1",
            s.token_hash
        )
        .execute(&st.db)
        .await?;
    }
    let clear = format!("{SESSION_COOKIE}=; Path=/auth; HttpOnly; Secure; SameSite=Lax; Max-Age=0");
    Ok((StatusCode::NO_CONTENT, [(header::SET_COOKIE, clear)]).into_response())
}

async fn provider_login(
    State(st): State<AppState>,
    Path(provider): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    Session(session): Session,
) -> Result<Response, ApiError> {
    let oauth = st.providers.oauth(&provider);
    let enabled = if provider == "steam" {
        st.providers.steam_openid
    } else {
        oauth.is_some()
    };
    if !enabled {
        return Err(ApiError::new(StatusCode::NOT_FOUND, "provider_disabled"));
    }
    let return_to = query.get("return_to").map_or("/", String::as_str);
    if !return_to.starts_with('/')
        || return_to.starts_with("//")
        || return_to.contains(char::is_control)
    {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "bad_return_to"));
    }

    let state = sessions::random_token();
    let (verifier, challenge) = match oauth {
        Some(cfg) if cfg.use_pkce => {
            let (v, c) = providers::pkce_pair();
            (Some(v), c)
        }
        _ => (None, String::new()),
    };
    let guest_token_hash = session
        .filter(|s| s.account_id.is_none())
        .map(|s| s.token_hash);

    // Abandoned logins never reach a callback; drop them here.
    sqlx::query!(
        "DELETE FROM social.oauth_states WHERE created_at < now() - interval '10 minutes'"
    )
    .execute(&st.db)
    .await?;
    sqlx::query!(
        "INSERT INTO social.oauth_states (state, provider, pkce_verifier, return_to, guest_token_hash)
         VALUES ($1, $2, $3, $4, $5)",
        state,
        provider,
        verifier,
        return_to,
        guest_token_hash
    )
    .execute(&st.db)
    .await?;

    let url = match oauth {
        None => providers::openid_redirect_url(&st.config.public_url, &state),
        Some(cfg) => providers::authorization_url(
            cfg,
            &callback_url(&st.config, &provider),
            &state,
            verifier.as_deref(),
            &challenge,
        ),
    };
    Ok(found(url))
}

struct OAuthState {
    pkce_verifier: Option<String>,
    return_to: String,
    guest_token_hash: Option<String>,
}

/// Single-use: the row is deleted whether or not it is still fresh.
async fn consume_state(
    db: &PgPool,
    state: &str,
    provider: &str,
) -> anyhow::Result<Option<OAuthState>> {
    let row = sqlx::query!(
        r#"DELETE FROM social.oauth_states WHERE state = $1 AND provider = $2
           RETURNING pkce_verifier, return_to, guest_token_hash,
                     created_at > now() - interval '10 minutes' AS "fresh!""#,
        state,
        provider
    )
    .fetch_optional(db)
    .await
    .context("consuming oauth state")?;
    Ok(row.filter(|r| r.fresh).map(|r| OAuthState {
        pkce_verifier: r.pkce_verifier,
        return_to: r.return_to,
        guest_token_hash: r.guest_token_hash,
    }))
}

/// Callback failures send the browser home with `?login_error=<code>`; the
/// cause is only logged.
fn login_failed(
    st: &AppState,
    provider: &str,
    code: &'static str,
    cause: impl std::fmt::Display,
) -> Response {
    tracing::warn!(provider, error = code, "login callback failed: {cause:#}");
    found(format!("{}/?login_error={code}", st.config.public_url))
}

/// Attaches the guest that started the login (if still valid), sets the
/// session cookie and returns to `return_to`.
async fn finish_login(
    st: &AppState,
    provider: &str,
    subject: &str,
    display_name: &str,
    stored: OAuthState,
) -> Response {
    let prior = match &stored.guest_token_hash {
        None => None,
        Some(hash) => match sessions::session_by_hash(&st.db, hash).await {
            Ok(prior) => prior.filter(|p| p.account_id.is_none()),
            Err(e) => return login_failed(st, provider, "internal", e),
        },
    };
    match sessions::login_account(&st.db, provider, subject, display_name, prior).await {
        Ok((token, _)) => (
            StatusCode::FOUND,
            [
                (
                    header::LOCATION,
                    format!("{}{}", st.config.public_url, stored.return_to),
                ),
                (header::SET_COOKIE, session_cookie(&token)),
            ],
        )
            .into_response(),
        Err(e) => login_failed(st, provider, "internal", e),
    }
}

async fn steam_callback(
    State(st): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let Some(state) = params.get("state") else {
        return login_failed(&st, "steam", "invalid_state", "missing state");
    };
    let stored = match consume_state(&st.db, state, "steam").await {
        Ok(Some(stored)) => stored,
        Ok(None) => return login_failed(&st, "steam", "invalid_state", "unknown or expired state"),
        Err(e) => return login_failed(&st, "steam", "internal", e),
    };
    let expected_return_to = providers::steam_return_to(&st.config.public_url, state);
    let steam_id = match providers::verify_openid(&st.http, &params, &expected_return_to).await {
        Ok(id) => id.to_string(),
        Err(e) => return login_failed(&st, "steam", "provider_error", e),
    };
    finish_login(
        &st,
        "steam",
        &steam_id,
        &format!("Steam {steam_id}"),
        stored,
    )
    .await
}

async fn oauth_callback(
    State(st): State<AppState>,
    Path(provider): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let Some(cfg) = st.providers.oauth(&provider) else {
        return ApiError::new(StatusCode::NOT_FOUND, "provider_disabled").into_response();
    };
    let Some(state) = params.get("state") else {
        return login_failed(&st, &provider, "invalid_state", "missing state");
    };
    let stored = match consume_state(&st.db, state, &provider).await {
        Ok(Some(stored)) => stored,
        Ok(None) => {
            return login_failed(&st, &provider, "invalid_state", "unknown or expired state");
        }
        Err(e) => return login_failed(&st, &provider, "internal", e),
    };
    let Some(code) = params.get("code") else {
        let reason = params.get("error").map_or("missing code", String::as_str);
        return login_failed(&st, &provider, "provider_error", reason);
    };
    let (subject, name) =
        match oauth_identity(&st, cfg, code, stored.pkce_verifier.as_deref()).await {
            Ok(identity) => identity,
            Err(e) => return login_failed(&st, &provider, "provider_error", e),
        };
    finish_login(&st, &provider, &subject, &name, stored).await
}

/// Code exchange + userinfo; returns `(subject, display_name)`.
async fn oauth_identity(
    st: &AppState,
    cfg: &ProviderConfig,
    code: &str,
    verifier: Option<&str>,
) -> anyhow::Result<(String, String)> {
    // au.2143.me's bot protection rejects non-browser user agents.
    let ua = if cfg.id == "au2143" {
        providers::BROWSER_UA
    } else {
        "rots-authserver"
    };
    let redirect_uri = callback_url(&st.config, &cfg.id);
    let mut form = vec![
        ("client_id", cfg.client_id.as_str()),
        ("client_secret", cfg.client_secret.as_str()),
        ("code", code),
        ("redirect_uri", redirect_uri.as_str()),
        ("grant_type", "authorization_code"),
    ];
    if let Some(v) = verifier {
        form.push(("code_verifier", v));
    }
    let token: Value = st
        .http
        .post(&cfg.token_endpoint)
        .header(header::USER_AGENT, ua)
        .form(&form)
        .send()
        .await
        .context("token request")?
        .error_for_status()
        .context("token exchange")?
        .json()
        .await
        .context("token response")?;
    let access = token["access_token"].as_str().context("no access_token")?;
    let info: Value = st
        .http
        .get(&cfg.userinfo_endpoint)
        .header(header::USER_AGENT, ua)
        .bearer_auth(access)
        .send()
        .await
        .context("userinfo request")?
        .error_for_status()
        .context("userinfo")?
        .json()
        .await
        .context("userinfo response")?;
    let subject = match &info[&cfg.id_field] {
        Value::String(s) if !s.is_empty() => s.clone(),
        Value::Number(n) => n.to_string(),
        _ => bail!("userinfo has no {}", cfg.id_field),
    };
    Ok((subject, providers::userinfo_name(&info, cfg)))
}
