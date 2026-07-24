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
    async fn apply_changes(
        &self,
        install: Vec<&str>,
        remove: Vec<&str>,
        upgrade: bool,
    ) -> zbus::Result<u64>;
    fn updates_list(&self) -> zbus::Result<String>;

    #[zbus(signal)]
    async fn status(&self, status: String) -> zbus::Result<()>;

    #[zbus(signal)]
    async fn result_report(&self, report: String) -> zbus::Result<()>;
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
enum TaskStatus {
    Success,
    Failed(String),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct DpkgProgress {
    status: String,
    stage: String,
    package_or_dpkg_exec: String,
    percent: f32,
    description: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum Progress {
    Dpkg(DpkgProgress),
    Oma(oma_fetch::Event),
    Done { status: String, request_id: u64 },
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct ApplyResult {
    request_id: u64,
    status: TaskStatus,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Connecting to System D-Bus...");
    let connection = Connection::system().await?;
    let proxy = AmoContractProxy::new(&connection).await?;
    let mut status_stream = proxy.receive_status().await?;

    let op = proxy.updates_list().await?;
    let op: OmaOperation = serde_json::from_str(&op)?;

    if op.install.is_empty() && op.remove.is_empty() {
        println!("System is up to date");
        return Ok(());
    }

    println!("[Step 2] Triggering transaction commit...");
    let id = match proxy.apply_changes(vec![], vec![], true).await {
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
    let mut result_stream = proxy.receive_result_report().await?;

    loop {
        tokio::select! {
            Some(signal) = status_stream.next() => {
                let status = signal.args()?.status;
                let status: Progress = serde_json::from_str(&status)?;
                println!("Status: {:?}", status);
                if let Progress::Done { status, request_id } = status
                    && request_id == id
                {
                    let date = request_id >> 32;
                    let seq = request_id & 0xFFFFFFFF; // 提取低 32 位序列号
                    println!(
                        "Status: {}({}) date: {}, seq: {}",
                        status, request_id, date, seq
                    );
                    break;
                }
            }
            Some(signal) = result_stream.next() => {
                let report_str = signal.args()?.report;
                let result: ApplyResult = serde_json::from_str(&report_str)?;
                println!("Client finished.");
                println!("{:?}", result);
                return Ok(());
            }
        }
    }

    // Wait for result_report if not already received
    if let Some(signal) = result_stream.next().await {
        let report_str = signal.args()?.report;
        let result: ApplyResult = serde_json::from_str(&report_str)?;
        println!("Client finished.");
        println!("{:?}", result);
    }

    Ok(())
}
