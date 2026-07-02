use std::future::pending;

use tracing::info;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};
use tracing_tree::{HierarchicalLayer, time::LocalDateTime};

use crate::server::Amo;

mod oma;
mod server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"))
        .add_directive("zbus=error".parse()?)
        .add_directive("zbus_fdo=error".parse()?)
        .add_directive("tokio=warn".parse()?);

    let mut dt = LocalDateTime::default();
    dt.higher_precision = true;

    let journald_layer = tracing_journald::Layer::new()
        .inspect_err(|e| eprintln!("Failed to initialize journald layer: {}", e));

    if let Ok(journald) = journald_layer {
        tracing_subscriber::registry()
            .with(filter)
            .with(
                HierarchicalLayer::new(2)
                    .with_targets(true)
                    .with_bracketed_fields(true)
                    .with_span_modes(true)
                    .with_timer(dt),
            )
            .with(journald)
            .init();
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(
                HierarchicalLayer::new(2)
                    .with_targets(true)
                    .with_bracketed_fields(true)
                    .with_span_modes(true)
                    .with_timer(dt),
            )
            .init();
    }

    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

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
