use clap::Parser;

use rots_authserver::{Action, Config};

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = Config::parse();
    let db = sqlx::PgPool::connect(&config.database_url).await?;

    match config.action {
        Action::Server => {
            config.listen(db).await?;
        }
    }

    Ok(())
}
