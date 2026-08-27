use anyhow::bail;
use futures_util::StreamExt;
use oma_fetch::Event as DownloadEvent;
use oma_pm::apt::OmaOperation;
use oma_refresh::db::Event;
use serde::{Deserialize, Serialize};
use zbus::proxy;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
enum TaskStatus {
    Success,
    Failed(String),
}

#[proxy(
    interface = "io.aosc.Amo1",
    default_service = "io.aosc.Amo",
    default_path = "/io/aosc/Amo"
)]
trait Amo {
    fn refresh(&self) -> zbus::Result<u64>;
    fn updates_list(&self) -> zbus::Result<u64>;
    #[zbus(signal)]
    fn status(&self, status: String) -> zbus::Result<()>;
    #[zbus(signal)]
    fn result_report(&self, report: String) -> zbus::Result<()>;
}

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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let connection = zbus::Connection::system().await?;
    let proxy = AmoProxy::new(&connection).await?;

    let mut status_stream = proxy.receive_status().await?;
    let mut result_stream = proxy.receive_result_report().await?;

    println!("[INFO] Triggering refresh...");
    let id = proxy.refresh().await?;
    println!("Task id: {id}");

    println!("[INFO] Waiting for status updates via D-Bus Signals...\n");
    while let Some(signal) = status_stream.next().await {
        let msg = signal.args()?.status;
        let msg: Event = serde_json::from_str(&msg)?;

        match msg {
            Event::DownloadEvent(event) => match event {
                DownloadEvent::NewProgressBar {
                    index,
                    total,
                    msg,
                    size,
                } => {
                    let size_mb = size as f64 / 1024.0 / 1024.0;
                    println!(
                        "  [DOWNLOAD] [{}/{}] [Task #{}] Started: {} ({:.2} MB)",
                        index + 1,
                        total,
                        index,
                        msg,
                        size_mb
                    );
                }

                DownloadEvent::NewProgressSpinner { index, total, msg } => {
                    println!(
                        "  [SCAN] [{}/{}] [Task #{}] Initializing/Scanning: {}",
                        index + 1,
                        total,
                        index,
                        msg
                    );
                }

                DownloadEvent::DownloadDone { index, msg } => {
                    println!("    -> [SUCCESS] [Task #{}] Completed: {}", index, msg);
                }

                DownloadEvent::NextUrl { file_name, err, .. } => {
                    eprintln!("  [ERROR] Failed to download {file_name}: {err}")
                }

                DownloadEvent::ChecksumMismatch {
                    filename, times, ..
                } => {
                    eprintln!(
                        "    -> [WARN] File '{}' checksum mismatch (Retry #{})...",
                        filename, times
                    );
                }

                DownloadEvent::Timeout { filename, times } => {
                    eprintln!(
                        "    -> [WARN] File '{}' download timeout (Retry #{})...",
                        filename, times
                    );
                }

                DownloadEvent::Failed { file_name, error } => {
                    eprintln!(
                        "  [FATAL] File '{}' completely failed! Reason: {}",
                        file_name, error
                    );
                }

                DownloadEvent::AllDone => {
                    println!("  [INFO] All download tasks are completed.");
                }

                DownloadEvent::GlobalProgressAdd(_)
                | DownloadEvent::GlobalProgressSub(_)
                | DownloadEvent::NewGlobalProgressBar(_)
                | DownloadEvent::ProgressInc { .. }
                | DownloadEvent::ProgressDone(_) => {}
            },
            Event::ScanningTopic => {
                println!("[INFO] Scanning repository topic tree...");
            }
            Event::ClosingTopic(topic) => {
                println!("[INFO] Closing topic branch: {}", topic);
            }
            Event::TopicNotInMirror { topic, mirror } => {
                println!(
                    "[WARN] Topic '{}' is missing in mirror '{}'.",
                    topic, mirror
                );
            }
            Event::RunInvokeScript => {
                println!("[INFO] Running repository invoke scripts...");
            }
            Event::SourceListFileNotSupport { path } => {
                eprintln!("[ERROR] Unsupported source list file: {}", path.display());
            }
            Event::Done => {
                println!("\n[SUCCESS] All repository refresh operations are fully done!");
                break;
            }
        }
    }

    // 等 refresh 的结果。
    let report = wait_for_report(&mut result_stream, id).await?;
    match report.status {
        TaskStatus::Success => {}
        TaskStatus::Failed(e) => bail!("Failed to refresh packages metadata: {e}"),
    }

    // 更新列表也是事务：从它的 ResultReport.result 里取 OmaOperation。
    let id = proxy.updates_list().await?;
    let report = wait_for_report(&mut result_stream, id).await?;
    let op: OmaOperation = serde_json::from_value(report.result.expect("updates list missing"))?;
    println!("{}", op);

    Ok(())
}
