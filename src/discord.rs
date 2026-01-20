use crate::Config;
use axum::{extract::Extension, response::IntoResponse};
use std::sync::Arc;

pub async fn get_login_url(Extension(config): Extension<Arc<Config>>) -> impl IntoResponse {
    "Hi!"
}

pub async fn callback(Extension(config): Extension<Arc<Config>>) -> impl IntoResponse {
    "Hi!"
}
