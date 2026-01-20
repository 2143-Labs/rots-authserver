use clap::Parser;

use rots_authserver::{Action, Config};

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let config = Config::parse();
    let db = sqlx::PgPool::connect(&config.database_url).await?;

    match &config.action {
        Action::Server => {
            config.listen(db).await?;
        }
    }

    Ok(())
}
