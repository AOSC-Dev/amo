use std::future::pending;

use tracing::info;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};
use tracing_tree::{HierarchicalLayer, time::LocalDateTime};

use crate::server::Amo;

mod auth;
mod oma;
mod refresh;
mod server;
mod transaction;
mod transaction_object;
mod tum;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"))
        .add_directive("zbus=error".parse()?)
        .add_directive("zbus_fdo=error".parse()?)
        .add_directive("tokio=warn".parse()?);

    tracing_subscriber::registry()
        .with(filter)
        .with(
            HierarchicalLayer::new(2)
                .with_targets(true)
                .with_bracketed_fields(true)
                .with_span_modes(true)
                .with_timer(LocalDateTime {
                    higher_precision: true,
                }),
        )
        .init();

    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider!");

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
