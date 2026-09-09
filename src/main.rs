use tokio::signal::unix::{SignalKind, signal};
use tracing::info;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};
use tracing_tree::{HierarchicalLayer, time::LocalDateTime};

use crate::server::{Amo, announce_restart};

mod oma;
mod self_update;
mod server;
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

    let (amo, mut restart) = Amo::new()?;
    let conn = zbus::connection::Builder::system()?
        .name("io.aosc.Amo")?
        .allow_name_replacements(false)
        .serve_at("/io/aosc/Amo", amo)?
        .build()
        .await?;

    let mut sigterm = signal(SignalKind::terminate())?;

    tokio::select! {
        _ = tokio::signal::ctrl_c() => info!("Received SIGINT, shutting down"),
        _ = sigterm.recv() => info!("Received SIGTERM, shutting down"),
        // 二进制被替换且服务已空闲：通知客户端重连后退出，让 systemd 在
        // 下次 D-Bus 调用时拉起新版本。
        _ = restart.wait() => announce_restart(&conn).await,
    }

    conn.graceful_shutdown().await;
    info!("amo stopped");

    Ok(())
}
