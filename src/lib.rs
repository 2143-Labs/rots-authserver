use std::{convert::Infallible, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::Context;
use sqlx::PgPool;

pub mod api;
pub mod providers;
pub mod sessions;
pub mod tickets;

/// Every field is also read from its SCREAMING_SNAKE_CASE env var. Empty
/// strings count as unset for `Option` fields (see [`Config::normalized`]).
#[derive(clap::Parser, Clone, Default)]
pub struct Config {
    #[clap(long, default_value = "8000", env)]
    pub port: u16,

    #[clap(long, env)]
    pub database_url: String,

    /// Public origin the gateway serves `/auth` under, e.g. `https://rots.john2143.com`.
    /// Also the game ticket audience.
    #[clap(long, env)]
    pub public_url: String,

    /// Standard base64 of a 32-byte ed25519 seed.
    #[clap(long, env, hide_env_values = true)]
    pub ticket_signing_key: String,

    #[clap(long, default_value = "4346770", env)]
    pub steam_app_id: u32,
    #[clap(long, env, hide_env_values = true)]
    pub steam_api_key: Option<String>,

    #[clap(long, env)]
    pub discord_client_id: Option<String>,
    #[clap(long, env, hide_env_values = true)]
    pub discord_client_secret: Option<String>,

    #[clap(long, env)]
    pub au2143_client_id: Option<String>,
    #[clap(long, env, hide_env_values = true)]
    pub au2143_client_secret: Option<String>,
    #[clap(long, default_value = "https://au.2143.me", env)]
    pub au2143_issuer: String,

    /// Comma list of providers to turn off: `steam`, `discord`, `au2143`.
    #[clap(long, default_value = "", env)]
    pub disabled_providers: String,

    /// Do not actually run api requests
    #[clap(long, env)]
    pub dry_run: bool,

    #[clap(subcommand)]
    pub action: Action,
}

#[derive(Clone, Debug, clap::Subcommand, Default)]
pub enum Action {
    /// Run the axum webserver handling auth
    #[default]
    Server,
}

impl Config {
    /// Empty `Option` values become `None`; `public_url` loses any trailing slash.
    pub fn normalized(mut self) -> Self {
        for field in [
            &mut self.steam_api_key,
            &mut self.discord_client_id,
            &mut self.discord_client_secret,
            &mut self.au2143_client_id,
            &mut self.au2143_client_secret,
        ] {
            if field.as_deref().is_some_and(|v| v.trim().is_empty()) {
                *field = None;
            }
        }
        let trimmed = self.public_url.trim_end_matches('/').len();
        self.public_url.truncate(trimmed);
        self
    }

    pub async fn listen(self, database: PgPool) -> anyhow::Result<Infallible> {
        let config = self.normalized();
        let tickets = tickets::TicketKeys::from_seed_b64(&config.ticket_signing_key)?;
        sqlx::migrate!("./migrations")
            .run(&database)
            .await
            .context("Unable to run migrations")?;

        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .context("building http client")?;
        let providers = providers::Providers::from_config(&config, &http).await;
        tracing::info!("enabled providers: {:?}", providers.enabled());

        let port = config.port;
        let app = api::create_api(api::AppState {
            config: Arc::new(config),
            db: database,
            tickets: Arc::new(tickets),
            providers: Arc::new(providers),
            http,
        });

        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
            .await
            .with_context(|| format!("binding port {port}"))?;
        tracing::info!("Server listening on {}", listener.local_addr()?);
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await?;
        unreachable!()
    }
}
