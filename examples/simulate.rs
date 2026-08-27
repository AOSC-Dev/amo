use futures_util::StreamExt;
use oma_pm::apt::OmaOperation;
use serde::Deserialize;
use zbus::{Connection, proxy};

#[proxy(
    interface = "io.aosc.Amo1",
    default_service = "io.aosc.Amo",
    default_path = "/io/aosc/Amo"
)]
trait AmoContract {
    async fn simulate(
        &self,
        install: Vec<&str>,
        remove: Vec<&str>,
        upgrade: bool,
    ) -> zbus::Result<u64>;

    #[zbus(signal)]
    async fn result_report(&self, report: String) -> zbus::Result<()>;
}

#[derive(Deserialize, Debug)]
struct TransactionResult {
    transaction_id: u64,
    status: TaskStatus,
    result: Option<serde_json::Value>,
}

#[derive(Deserialize, Debug)]
enum TaskStatus {
    Success,
    Failed(String),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let connection = Connection::system().await?;
    let proxy = AmoContractProxy::new(&connection).await?;

    // 模拟也是一个排队执行的事务：拿到 id 后等它的 ResultReport。
    let id = proxy.simulate(vec!["gnome-base"], vec![], false).await?;
    println!("simulate: transaction {id}");

    let mut result_stream = proxy.receive_result_report().await?;
    while let Some(signal) = result_stream.next().await {
        let report_str = signal.args()?.report;
        let report: TransactionResult = serde_json::from_str(&report_str)?;
        if report.transaction_id != id {
            continue;
        }
        match report.status {
            TaskStatus::Success => {
                let op: OmaOperation =
                    serde_json::from_value(report.result.expect("simulate result missing"))?;
                println!("{op}");
            }
            TaskStatus::Failed(e) => anyhow::bail!("simulate failed: {e}"),
        }
        break;
    }

    Ok(())
}
