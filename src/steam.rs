use crate::Config;
use crate::discord::basic_error_response;
use axum::Json;
use axum::response::Response;
use axum::{extract::Extension, response::IntoResponse};
use sqlx::PgPool;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone)]
pub struct WeakLoginRequest {
    pub steam_player_id: String,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct WeakLoginResponse {
    pub player_id: u64,
    pub login_token: String,
    pub success: bool,
}

pub fn create_token() -> String {
    let login_token = rand::random_iter().take(32)
        //map to hex:
        .map(|b: u8| format!("{:02x}", b))
        .collect::<String>();

    login_token
}

pub async fn weak_login(
    Extension(config): Extension<Arc<Config>>,
    Extension(database): Extension<PgPool>,
    Json(payload): Json<WeakLoginRequest>,
) -> Result<impl IntoResponse, Response> {
    println!(
        "Weak login request for steam player id: {}",
        &payload.steam_player_id
    );

    let login_token = create_token();

    sqlx::query!(
        r#"
        INSERT INTO social.steam_login_attempts (attempted_at, steam_user_id, used_weak_login, login_token)
        VALUES (now(), $1, true, $2)
        "#,
        &payload.steam_player_id,
        &login_token,
    ).execute(&database)
        .await
        .map_err(|e| {
            basic_error_response(&format!("Failed to record login attempt: {}", e))
        })?;

    // TODO get steam user info from this user id (avatar etc)

    let player_id_to_steam_id = sqlx::query!(
        r#"
        INSERT INTO social.playerid_by_steam (steam_user_id, player_id)
        VALUES ($1, nextval('social.playerid_seq'))
        ON CONFLICT (steam_user_id)
        DO NOTHING
        RETURNING player_id
        "#,
        &payload.steam_player_id,
    ).fetch_one(&database)
        .await
        .map_err(|e| {
            basic_error_response(&format!("Failed to fetch registered user: {}", e))
        })?;

    sqlx::query!(
        r#"
        INSERT INTO social.player_valid_logins (login_token, player_id, created_at)
        VALUES ($1, $2, now())
        "#,
        &login_token,
        player_id_to_steam_id.player_id,
    ).execute(&database)
        .await
        .map_err(|e| {
            basic_error_response(&format!("Failed to record valid login: {}", e))
        })?;

    Ok(Json(WeakLoginResponse {
        player_id: player_id_to_steam_id.player_id as u64,
        login_token,
        success: true,
    }))
}
