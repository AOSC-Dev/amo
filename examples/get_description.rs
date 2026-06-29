use zbus::{Connection, proxy};

#[proxy(
    interface = "io.aosc.Amo1",
    default_service = "io.aosc.Amo",
    default_path = "/io/aosc/Amo"
)]
trait AmoContract {
    async fn get_description(&self, pkg_name: &str) -> zbus::Result<String>;
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let connection = Connection::system().await?;
    let proxy = AmoContractProxy::new(&connection).await?;
    let result = proxy.get_description("fish").await?;
    println!("{}", result);

    Ok(())
}
