use axum::Router;
use http::{Method, StatusCode};
use sqlx::PgPool;
use tower_http::normalize_path::NormalizePathLayer;

#[derive(Clone)]
pub struct AppState {
    pub database: PgPool,
}

pub fn create_api(database: PgPool) -> axum::Router {
    let state = AppState {
        database: database.clone(),
    };

    let cors = tower_http::cors::CorsLayer::new()
        // allow `GET` and `POST` when accessing the resource
        .allow_methods([Method::GET, Method::POST, Method::PATCH, Method::DELETE])
        .allow_headers([http::header::CONTENT_TYPE, http::header::AUTHORIZATION])
        // allow requests from any origin
        .allow_origin(tower_http::cors::Any)
        //.allow_origin(origins)
        .allow_credentials(true);

    let api_routes = axum::Router::new()
        .route(
            "/discord/get_login_url",
            axum::routing::get(crate::discord::get_login_url),
        )
        .route(
            "/discord/callback",
            axum::routing::get(crate::discord::callback),
        );

    let api_routes = api_routes
        // TODO: via steamworks funcs in client to get a cert to proove from server
        //.route("/steam/get_login_url", axum::routing::get(steam::get_login_url))
        //.route("/steam/callback", axum::routing::get(steam::callback))
        // This is a way to declare a steamid and login as that player. We can't guarentee players
        // wont cheat here but its a quick way to login now.
        .route(
            "/steam/weak_login",
            axum::routing::post(crate::steam::weak_login),
        );

    let app = Router::new()
        .route("/health", axum::routing::get(health))
        .nest("/api/v1/", api_routes)
        .with_state(state)
        .layer(NormalizePathLayer::trim_trailing_slash());

    app
}

/// Returns 200, always
async fn health() -> (StatusCode, &'static str) {
    (StatusCode::OK, "Ok")
}
