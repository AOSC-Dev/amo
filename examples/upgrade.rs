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
    fn updates_list(&self) -> zbus::Result<u64>;

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
struct TransactionResult {
    transaction_id: u64,
    status: TaskStatus,
    result: Option<serde_json::Value>,
}

/// 等到指定事务的 ResultReport 信号。
async fn wait_for_report(
    stream: &mut ResultReportStream,
    id: u64,
) -> anyhow::Result<TransactionResult> {
    while let Some(signal) = stream.next().await {
        let report_str = signal.args()?.report;
        let report: TransactionResult = serde_json::from_str(&report_str)?;
        if report.transaction_id == id {
            return Ok(report);
        }
    }
    bail!("result stream closed before receiving transaction {id}");
}

/// Status 信号信封：广播流里混着队列中其他事务的进度，先按事务 id 过滤。
#[derive(Deserialize)]
struct StatusEnvelope {
    transaction_id: u64,
    event: serde_json::Value,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Connecting to System D-Bus...");
    let connection = Connection::system().await?;
    let proxy = AmoContractProxy::new(&connection).await?;
    let mut status_stream = proxy.receive_status().await?;
    let mut result_stream = proxy.receive_result_report().await?;

    // 更新列表（事务）：从它的 ResultReport.result 里取 OmaOperation。
    let id = proxy.updates_list().await?;
    let report = wait_for_report(&mut result_stream, id).await?;
    let op: OmaOperation = serde_json::from_value(report.result.expect("updates list missing"))?;

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

    loop {
        tokio::select! {
            Some(signal) = status_stream.next() => {
                let env: StatusEnvelope = serde_json::from_str(&signal.args()?.status)?;
                // 广播流里混着队列中其他事务的进度，先按事务 id 过滤。
                if env.transaction_id != id {
                    continue;
                }
                let status: Progress = serde_json::from_value(env.event)?;
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
                let result: TransactionResult = serde_json::from_str(&report_str)?;
                // 队列里其他客户端的事务也会广播 ResultReport，必须按 id 过滤，
                // 否则别的事务的报告会让我们提前返回。
                if result.transaction_id != id {
                    continue;
                }
                println!("Client finished.");
                println!("{:?}", result);
                return Ok(());
            }
        }
    }

    // Wait for result_report if not already received (filtered by id).
    let report = wait_for_report(&mut result_stream, id).await?;
    println!("Client finished.");
    println!("{:?}", report);

    Ok(())
}
