#![allow(unused)]
use crate::Config;
use axum::{extract::Extension, response::IntoResponse};
use std::sync::Arc;
use anyhow::Result;
use axum::{
    RequestPartsExt,
    extract::{FromRequestParts, OptionalFromRequestParts, Query},
    response::Response,
};
use axum_extra::extract::CookieJar;
use reqwest::Client;
use serde::Deserialize;
use sqlx::PgPool;

pub async fn get_login_url(
    Extension(database): Extension<PgPool>,
    Extension(config): Extension<Arc<Config>>
) -> impl IntoResponse {
    let state = rand::random_iter().take(16)
        //map to hex:
        .map(|b: u8| format!("{:02x}", b))
        .collect::<String>();

    let cfg = config
        .get_discord_oauth()
        .expect("Discord OAuth configuration not found on prod?");


    sqlx::query!(
        r#"
        INSERT INTO social.discord_login_attempts (state, attempted_at)
        VALUES ($1, now())
        "#,
        &state,
    ).execute(&database)
        .await
        .expect("Failed to record login attempt");

    let url = authorization_url(&state, &cfg);
    let url_as_body = format!("{}", url);
    Response::builder()
        .status(axum::http::StatusCode::FOUND)
        .header(axum::http::header::LOCATION, url)
        .body(axum::body::Body::from(url_as_body))
        .unwrap()
}

pub async fn callback(
    Extension(database): Extension<PgPool>,
    Extension(config): Extension<Arc<Config>>,
    Query(params): Query<CallbackAPIParams>,
) -> impl IntoResponse {
    // First, mark the login attempt as completed, and ensure we havent been in yet.
    // TODO race condition check here
    let num_changed = sqlx::query!(
        r#"
        UPDATE social.discord_login_attempts
        SET completed_at = now()
        WHERE state = $1 AND completed_at IS NULL
        "#,
        &params.state,
    ).execute(&database)
        .await
        .expect("Failed to record login attempt");

    if num_changed.rows_affected() == 0 {
        return Err(basic_error_response("Invalid or expired 'state' parameter in callback"));
    }

    if let Some(error) = params.error {
        return Err(basic_error_response(&error));
    }

    let Some(code) = params.code else {
        return Err(basic_error_response("Missing 'code' parameter in callback"));
    };

    let cfg = config
        .get_discord_oauth()
        .expect("Discord OAuth configuration not found");

    let token_response = exchange_code(&code, &cfg)
        .await
        .map_err(|e| basic_error_response(&format!("Failed to exchange code: {}", e)))?;
    let user = fetch_user(&token_response.access_token)
        .await
        .map_err(|e| basic_error_response(&format!("Failed to fetch user: {}", e)))?;

    println!("{:?}", token_response);

    let global_name = user
        .global_name
        .as_deref()
        .unwrap_or(user.username.as_str());

    // Check if we have this user,
    let res = sqlx::query!(
        r#"
        INSERT INTO social.discord_accounts (discord_user_id, discord_username, discord_avatar, discord_global_name)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (discord_user_id)
        DO UPDATE SET
            discord_avatar = EXCLUDED.discord_avatar,
            discord_global_name = EXCLUDED.discord_global_name
        "#,
        user.id,
        user.username,
        user.avatar,
        global_name,
    ).execute(&database)
        .await
        .map_err(|e| {
            basic_error_response(&format!("Failed to insert or fetch user: {}", e))
        })?;

    // one last time: update table to set discord_login_attempt.linked_discord_user_id
    let res = sqlx::query!(
        r#"
        UPDATE social.discord_login_attempts
        SET
            linked_discord_user_id = $1,
            linked_token = $3,
            refresh_token = $4
        WHERE state = $2
        "#,
        user.id,
        &params.state,
        &token_response.access_token,
        &token_response.refresh_token,
    ).execute(&database)
        .await
        .map_err(|e| {
            basic_error_response(&format!("Failed to update login attempt with linked user id: {}", e))
        }
    );

        //INSERT INTO social.playerid_by_discord (discord_user_id, player_id)
        //VALUES ($1, nextval('social.playerid_seq'))
        //ON CONFLICT (discord_user_id)
        //DO NOTHING
        //RETURNING player_id
    let player_id_to_discord_id = sqlx::query!(
        r#"
        SELECT player_id
        FROM social.playerid_by_discord
        WHERE discord_user_id = $1
        "#,
        user.id,
    ).fetch_optional(&database)
        .await
        .map_err(|e| {
            basic_error_response(&format!("Failed to fetch registered user: {}", e))
        })?;

    let player_id = match player_id_to_discord_id {
        Some(pid) => pid.player_id,
        None => {
            tracing::info!("Creating new player id for discord user id {}", user.id);

            let res = sqlx::query!(
                r#"
                INSERT INTO social.playerid_by_discord (discord_user_id, player_id)
                VALUES ($1, nextval('social.playerid_seq'))
                RETURNING player_id
                "#,
                user.id,
            ).fetch_one(&database)
                .await
                .map_err(|e| {
                    basic_error_response(&format!("Failed to create registered user: {}", e))
                })?;

            res.player_id
        }
    };

    let login_token = crate::steam::create_token();

    sqlx::query!(
        r#"
        INSERT INTO social.player_valid_logins (login_token, player_id, created_at)
        VALUES ($1, $2, now())
        "#,
        &login_token,
        player_id,
    ).execute(&database)
        .await
        .expect("Failed to record valid login");

    let is_production = cfg.redirect_uri.starts_with("https://");
    let cookie_attributes = if is_production {
        "; Secure; SameSite=Lax"
    } else {
        "; SameSite=Lax"
    };

    let location = format!("http://192.168.1.32:8080/#player_id={}&login_token={}", player_id, login_token);

    let res = Response::builder()
        .status(axum::http::StatusCode::FOUND)
        .header(axum::http::header::CONTENT_TYPE, "text/plain")
        .header(axum::http::header::CACHE_CONTROL, "no-cache")
        // Send them back to /
        .header(axum::http::header::LOCATION, location)
        // Add logged in cookies
        //   1. discord user id
        //   2. api token
        .header(
            axum::http::header::SET_COOKIE,
            format!(
                "player_id={}; Path=/; HttpOnly{}",
                player_id,
                cookie_attributes
            ),
        )
        .header(
            axum::http::header::SET_COOKIE,
            format!(
                "login_token={}; Path=/; HttpOnly{}",
                login_token,
                cookie_attributes
            ),
        )
        .body(axum::body::Body::from(format!(
            "Logged in as {} ({}). You can close this tab.", player_id, login_token,
        )))
        .map_err(|e| basic_error_response(&format!("Failed to build response: {}", e)))?;

    Ok(res)
}

impl Config {
    pub fn get_discord_oauth(&self) -> Option<OAuthBundle> {
        if self.discord_client_id.is_empty()
            || self.discord_client_secret.is_empty()
            || self.discord_redirect_uri.is_empty()
        {
            None
        } else {
            Some(OAuthBundle {
                client_id: self.discord_client_id.clone(),
                client_secret: self.discord_client_secret.clone(),
                redirect_uri: self.discord_redirect_uri.clone(),
            })
        }
    }
}

/// Discord OAuth configuration extracted from the main Config
#[derive(Debug, Clone)]
pub struct OAuthBundle {
    pub client_id: String,
    pub client_secret: String,
    pub redirect_uri: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,
    pub token_type: String,
    pub expires_in: u64,
    pub refresh_token: String,
    pub scope: String,
}

//{
//"id": "262351348534738945",
//"username": "john2143",
//"avatar": "f3e0bba6767b9bbb3e724bf75e4b34a2",
//"discriminator": "0",
//"public_flags": 0,
//"flags": 0,
//"banner": null,
//"accent_color": 11753991,
//"global_name": "John2143",
//"avatar_decoration_data": null,
//"collectibles": null,
//"display_name_styles": null,
//"banner_color": "#b35a07",
//"clan": null,
//"primary_guild": null,
//"mfa_enabled": false,
//"locale": "en-US",
//"premium_type": 3
//}
#[derive(Debug, Clone, Deserialize)]
pub struct DiscordUser {
    pub id: String,
    pub username: String,
    pub avatar: Option<String>,
    pub banner: Option<String>,
    pub accent_color: Option<u32>,
    pub global_name: Option<String>,
}

pub fn authorization_url(state: &str, cfg: &OAuthBundle) -> String {
    format!(
        "https://discord.com/api/oauth2/authorize?client_id={}&redirect_uri={}&response_type=code&scope=identify+openid&state={}",
        cfg.client_id,
        urlencoding::encode(&cfg.redirect_uri),
        urlencoding::encode(state)
    )
}

pub async fn exchange_code(code: &str, cfg: &OAuthBundle) -> Result<TokenResponse> {
    let params = [
        ("client_id", cfg.client_id.as_str()),
        ("client_secret", cfg.client_secret.as_str()),
        ("grant_type", "authorization_code"),
        ("code", code),
        ("redirect_uri", cfg.redirect_uri.as_str()),
    ];

    let client = Client::new();
    let res = client
        .post("https://discord.com/api/oauth2/token")
        .form(&params)
        .send()
        .await?;

    let token_response: TokenResponse = res.json().await?;
    Ok(token_response)
}

pub async fn fetch_user(token: &str) -> Result<DiscordUser> {
    let client = Client::new();
    let res = client
        .get("https://discord.com/api/users/@me")
        .bearer_auth(token)
        .send()
        .await?;

    let user: DiscordUser = res.json().await?;
    Ok(user)
}

#[derive(Debug, Deserialize)]
pub struct CallbackAPIParams {
    code: Option<String>,
    error: Option<String>,
    state: String,
}

pub fn basic_error_response(error: &str) -> Response {
    Response::builder()
        .status(axum::http::StatusCode::BAD_REQUEST)
        .body(axum::body::Body::from(format!("Error: {}", error)))
        .unwrap()
}

/*
CREATE TABLE social.user_sessions (
    session_id SERIAL PRIMARY KEY,
    created_at_unix_sec BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW()),
    expires_at_unix_sec BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM NOW()) + 3600 * 24), -- 1 day
    user_id CHAR(10) NOT NULL REFERENCES social.registered_users(id) ON DELETE CASCADE,
    session_token_hash TEXT NOT NULL UNIQUE,
);
*/

//pub struct APIUser {
    //pub user_id: String,
    //pub username: String,
//}

//impl<S: Sync> FromRequestParts<S> for APIUser {
    //type Rejection = axum::response::Response;

    //async fn from_request_parts(
        //parts: &mut axum::http::request::Parts,
        //state: &S,
    //) -> std::result::Result<Self, Self::Rejection> {
        //if let Some(Extension(config)) = parts.extract::<Extension<Arc<crate::Config>>>().await.ok() {
            //if config.get_discord_oauth().is_none() {
                //return Ok(APIUser {
                    //user_id: "testuser".to_string(),
                    //username: "Test User".to_string(),
                //});
            //}
        //}

        //let cookies = CookieJar::from_headers(&parts.headers);
        //let session_token = cookies
            //.get("session_token")
            //.map(|c| c.value().to_string())
            //.or_else(|| {
                //parts
                    //.headers
                    //.get("Authorization")
                    //.and_then(|h| h.to_str().ok())
                    //.and_then(|s| s.strip_prefix("Bearer "))
                    //.map(|s| s.to_string())
            //});

        //let rejection = |msg: &str| {
            //Response::builder()
                //.status(axum::http::StatusCode::UNAUTHORIZED)
                ////as text/plain
                //.header(axum::http::header::CONTENT_TYPE, "text/plain")
                //.header(axum::http::header::CACHE_CONTROL, "no-cache")
                //.body(axum::body::Body::from(msg.to_string()))
                //.unwrap()
        //};

        //let Some(their_token) = session_token else {
            //return Err(rejection(
                //"Sorry, to call this API you need either an discord login or session token.",
            //));
        //};

        //let Extension(db_extension) = parts.extract::<Extension<PgPool>>().await.map_err(|_| {
            //rejection("Sorry, authorization is currently unavailable. Try again later.")
        //})?;

        //let user = sqlx::query_as!(
            //APIUser,
            //r#"
            //SELECT u.id AS user_id, u.username
            //FROM social.registered_users u
            //JOIN social.user_sessions s ON s.user_id = u.id
            //WHERE
                //s.session_token_hash = encode(digest($1, 'sha256'), 'hex')
                //AND s.expires_at_unix_sec > EXTRACT(EPOCH FROM NOW())
            //"#,
            //their_token
        //)
        //.fetch_optional(&db_extension)
        //.await
        //.map_err(|_| rejection("Sorry, authorization is currently unavailable. Try again later."))?
        //.ok_or_else(|| {
            //rejection("Invalid session token or session expired. Please log in with discord again.")
        //})?;

        //Ok(user)
    //}
//}
