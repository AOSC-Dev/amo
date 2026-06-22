use anyhow::bail;
use futures::StreamExt;
use oma_pm::apt::OmaOperation;
use serde::{Deserialize, Serialize};
use zbus::{Connection, proxy};

#[proxy(
    interface = "io.aosc.Amo1",
    default_service = "io.aosc.Amo",
    default_path = "/io/aosc/Amo"
)]
trait AmoContract {
    async fn upgrade_all(&self) -> zbus::Result<String>;
    async fn commit(&self) -> zbus::Result<u64>;
    async fn get_last_result(&self, version: u64) -> zbus::Result<String>;

    #[zbus(signal)]
    async fn refresh_status(&self, status: String) -> zbus::Result<()>;
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct ResultReport {
    version: u64,
    status: TaskStatus,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
enum TaskStatus {
    Success,
    Failed(String),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct DpkgProgress {
    stage: String,
    package: String,
    percent: f32,
    description: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum Progress {
    Dpkg(DpkgProgress),
    Oma(oma_fetch::Event),
    Done { status: String, version: u64 },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Connecting to System D-Bus...");
    let connection = Connection::system().await?;
    let proxy = AmoContractProxy::new(&connection).await?;
    let mut status_stream = proxy.receive_refresh_status().await?;

    let op = match proxy.upgrade_all().await {
        Ok(op) => {
            println!("[Step 1 Success] Packages marked for installation successfully.");
            op
        }
        Err(e) => {
            eprintln!("[Step 1 Failed] Failed to mark packages: {}", e);
            return Ok(());
        }
    };

    let op: OmaOperation = serde_json::from_str(&op)?;

    if op.install.is_empty() && op.remove.is_empty() {
        println!("System is up to date");
        return Ok(());
    }

    println!("2[Step 2] Triggering transaction commit...");
    let id = match proxy.commit().await {
        Ok(id) => {
            println!("[Step 2 Dispatched] Commit request accepted by server.");
            println!(
                "The server is processing the download/installation asynchronously in the background."
            );
            id
        }
        Err(e) => {
            bail!("[Step 2 Failed] Failed to trigger commit: {}", e);
        }
    };

    println!("[Signal Listener] Thread started, waiting for progress events...");
    while let Some(signal) = status_stream.next().await {
        let status = signal.args()?.status;
        let status: Progress = serde_json::from_str(&status)?;
        println!("Status: {:?}", status);
        if let Progress::Done { status, version } = status
            && version == id
        {
            println!("Status: {}({})", status, version);
            break;
        }
    }

    let result = proxy.get_last_result(id).await?;
    let result: Option<ResultReport> = serde_json::from_str(&result)?;

    println!("Client finished.");
    println!("{:?}", result);

    Ok(())
}
