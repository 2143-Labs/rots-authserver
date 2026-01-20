use crate::Config;
use axum::Json;
use axum::{extract::Extension, response::IntoResponse};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone)]
pub struct WeakLoginRequest {
    pub steam_player_id: u64,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct WeakLoginResponse {
    pub player_id: u128,
}

pub async fn weak_login(
    Extension(config): Extension<Arc<Config>>,
    Json(payload): Json<WeakLoginRequest>,
) -> impl IntoResponse {
    Json(WeakLoginResponse {
        player_id: payload.steam_player_id as u128,
    })
}
