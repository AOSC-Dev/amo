use std::future::pending;

use tracing::{Level, info, level_filters::LevelFilter};
use tracing_subscriber::{EnvFilter, Layer, fmt, layer::SubscriberExt, util::SubscriberInitExt};

use crate::server::Amo;

mod oma;
mod server;

const USER_AGENT: &str = concat!("amo/", env!("CARGO_PKG_VERSION"));

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let env_log = EnvFilter::try_from_default_env();

    if let Ok(filter) = env_log {
        tracing_subscriber::registry()
            .with(
                fmt::layer()
                    .with_line_number(true)
                    .with_file(true)
                    .with_filter(filter),
            )
            .init();
    } else {
        tracing_subscriber::registry()
            .with(
                fmt::layer()
                    .with_file(true)
                    .with_line_number(true)
                    .with_filter(LevelFilter::from_level(Level::INFO)),
            )
            .init();
    }

    info!("amo is running");

    let amo = Amo::new()?;
    let _conn = zbus::connection::Builder::system()?
        .name("io.aosc.Amo")?
        .allow_name_replacements(false)
        .serve_at("/io/aosc/Amo", amo)?
        .build()
        .await?;

    pending::<()>().await;

    Ok(())
}
