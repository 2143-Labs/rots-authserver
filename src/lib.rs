use std::{convert::Infallible, net::SocketAddr, sync::Arc};

use sqlx::PgPool;

pub mod api;
pub mod discord;
pub mod steam;

#[derive(clap::Parser, Clone, Default, Debug)]
pub struct Config {
    #[clap(long, default_value = "8000", env)]
    pub port: u16,

    #[clap(long, env)]
    pub database_url: String,

    #[clap(long, env)]
    pub discord_client_id: String,
    #[clap(long, env)]
    pub discord_client_secret: String,
    #[clap(long, env)]
    pub discord_redirect_uri: String,

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
    async fn start_server(&self, database: PgPool) -> anyhow::Result<axum::Router> {
        let app = api::create_api(database);

        // Add config so it can be used in web handlers
        let app = app.layer(axum::Extension(Arc::new(self.clone())));

        Ok(app)
    }

    pub async fn listen(&self, database: PgPool) -> anyhow::Result<Infallible> {
        //sqlx::migrate::run("./migrations")
        //.run(&database)
        //.await
        //.context("Unable to run migrations")?;
        let app = self.start_server(database).await?;
        println!("Starting server on port {}", self.port);

        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", self.port))
            .await
            .unwrap();

        tracing::info!("Server listening on {}", listener.local_addr().unwrap());
        axum::serve(
            listener,
            app.layer(axum::Extension(self.clone()))
                .into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await?;
        unreachable!()
    }
}
