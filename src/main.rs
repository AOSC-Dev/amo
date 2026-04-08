use std::future::pending;

mod server;
mod refresh;

const USER_AGENT: &str = concat!("amo/", env!("CARGO_PKG_VERSION"));

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let amo = server::Amo::new();

    let _conn = zbus::connection::Builder::system()?
        .name("io.aosc.Amo")?
        .serve_at("/io/aosc/Amo", amo)?
        .build()
        .await?;

    pending::<()>().await;

    Ok(())
}
