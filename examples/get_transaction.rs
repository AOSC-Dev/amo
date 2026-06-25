use oma_pm::apt::OmaOperation;
use zbus::{Connection, proxy};

#[proxy(
    interface = "io.aosc.Amo1",
    default_service = "io.aosc.Amo",
    default_path = "/io/aosc/Amo"
)]
trait AmoContract {
    async fn get_transaction(
        &self,
        install: Vec<String>,
        remove: Vec<String>,
        upgrade: bool,
    ) -> zbus::Result<String>;
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let connection = Connection::system().await?;
    let proxy = AmoContractProxy::new(&connection).await?;
    let op = proxy
        .get_transaction(vec!["gnome-base".to_string()], vec![], false)
        .await?;

    let op: OmaOperation = serde_json::from_str(&op)?;

    println!("{}", op);

    Ok(())
}
