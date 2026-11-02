use std::{convert::Infallible, future::IntoFuture, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::Context;
use sqlx::PgPool;

pub mod api;
pub mod internal;
pub mod providers;
pub mod sessions;
pub mod temporal;
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

    /// Temporal frontend, e.g. `http://temporal-frontend.default.svc.cluster.local:7233`.
    /// Unset: no worker, and `/play` and game-server joins answer 503.
    #[clap(long, env)]
    pub temporal_address: Option<String>,
    #[clap(long, default_value = "rots", env)]
    pub temporal_namespace: String,
    /// Also names this environment's workflows (`rots-coordinator-<queue>`).
    #[clap(long, default_value = "rots-dev", env)]
    pub temporal_task_queue: String,

    /// Port of the internal API game servers report to (never routed publicly).
    #[clap(long, default_value = "8001", env)]
    pub internal_port: u16,
    /// Bearer token game servers present on the internal API. Required.
    #[clap(long, default_value = "", env, hide_env_values = true)]
    pub coordinator_token: String,
    /// Slots are 0..GAME_SERVER_SLOTS; reports for other slots are rejected.
    #[clap(long, default_value = "1", env)]
    pub game_server_slots: u32,
    /// Host in `/play`'s `server.udp`; unset = the host of `PUBLIC_URL`.
    #[clap(long, env)]
    pub game_udp_host: Option<String>,
    /// Slot N's UDP port is this + N (bevy2025's config.yaml uses 25555).
    #[clap(long, default_value = "25555", env)]
    pub game_udp_port_base: u16,
    /// A game server with no report for this long is treated as dead.
    #[clap(long, default_value = "30", env)]
    pub coordinator_stale_secs: u64,

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
            &mut self.temporal_address,
            &mut self.game_udp_host,
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
        if self.coordinator_token.trim().is_empty() {
            anyhow::bail!("COORDINATOR_TOKEN is required");
        }
        let state = build_state(self, database).await?;
        let _worker = if state.config.temporal_address.is_some() {
            Some(temporal::start_worker(state.clone()))
        } else {
            tracing::warn!("TEMPORAL_ADDRESS is unset: /play and game-server joins answer 503");
            None
        };

        let (port, internal_port) = (state.config.port, state.config.internal_port);
        let public = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
            .await
            .with_context(|| format!("binding port {port}"))?;
        let internal = tokio::net::TcpListener::bind(format!("0.0.0.0:{internal_port}"))
            .await
            .with_context(|| format!("binding internal port {internal_port}"))?;
        tracing::info!("Server listening on {}", public.local_addr()?);
        tracing::info!("Internal API listening on {}", internal.local_addr()?);
        tokio::try_join!(
            axum::serve(
                public,
                api::create_api(state.clone()).into_make_service_with_connect_info::<SocketAddr>(),
            )
            .into_future(),
            axum::serve(internal, internal::router(state)).into_future(),
        )?;
        unreachable!()
    }
}

/// Everything `listen` serves, minus the worker and the sockets: normalizes the
/// config, runs migrations and discovers providers. Tests build their state here.
pub async fn build_state(config: Config, db: PgPool) -> anyhow::Result<api::AppState> {
    let mut config = config.normalized();
    if config.game_udp_host.is_none() {
        let url = url::Url::parse(&config.public_url).context("PUBLIC_URL is not a URL")?;
        config.game_udp_host = Some(url.host_str().context("PUBLIC_URL has no host")?.to_owned());
    }
    let tickets = tickets::TicketKeys::from_seed_b64(&config.ticket_signing_key)?;
    sqlx::migrate!("./migrations")
        .run(&db)
        .await
        .context("Unable to run migrations")?;

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("building http client")?;
    let providers = providers::Providers::from_config(&config, &http).await;
    tracing::info!("enabled providers: {:?}", providers.enabled());

    Ok(api::AppState {
        config: Arc::new(config),
        db,
        tickets: Arc::new(tickets),
        providers: Arc::new(providers),
        http,
        temporal: Default::default(),
    })
}
