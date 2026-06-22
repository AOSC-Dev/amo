use anyhow::bail;
use futures::StreamExt;
use oma_fetch::Event;
use zbus::{Connection, proxy};

#[proxy(
    interface = "io.aosc.Amo1",
    default_service = "io.aosc.Amo",
    default_path = "/io/aosc/Amo"
)]
trait AmoContract {
    async fn install(&self, items: Vec<String>) -> zbus::Result<()>;
    async fn commit(&self) -> zbus::Result<()>;

    #[zbus(signal)]
    async fn refresh_status(&self, status: String) -> zbus::Result<()>;
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Connecting to System D-Bus...");
    let connection = Connection::system().await?;
    let proxy = AmoContractProxy::new(&connection).await?;

    let packages_to_install = vec!["fish".to_string()];
    println!(
        "[Step 1] Requesting install marking for: {:?}",
        packages_to_install
    );

    match proxy.install(packages_to_install).await {
        Ok(_) => println!("[Step 1 Success] Packages marked for installation successfully."),
        Err(e) => {
            eprintln!("[Step 1 Failed] Failed to mark packages: {}", e);
            return Ok(());
        }
    }

    println!("2[Step 2] Triggering transaction commit...");
    match proxy.commit().await {
        Ok(_) => {
            println!("[Step 2 Dispatched] Commit request accepted by server.");
            println!(
                "The server is processing the download/installation asynchronously in the background."
            );
        }
        Err(e) => {
            bail!("[Step 2 Failed] Failed to trigger commit: {}", e);
        }
    }

    let mut status_stream = proxy.receive_refresh_status().await?;
    println!("[Signal Listener] Thread started, waiting for progress events...");
    while let Some(signal) = status_stream.next().await {
        let status = signal.args()?.status;
        let status: Event = serde_json::from_str(&status)?;
        println!("Status: {:?}", status);
        if let Event::AllDone = status {
            break;
        }
    }

    println!("Client finished.");

    Ok(())
}
