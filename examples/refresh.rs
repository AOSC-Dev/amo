use anyhow::bail;
use futures_util::StreamExt;
use oma_fetch::Event as DownloadEvent;
use oma_pm::apt::OmaOperation;
use oma_refresh::db::Event;
use serde::{Deserialize, Serialize};
use zbus::proxy;

#[derive(Clone, Serialize, Deserialize, Debug)]
struct ResultReport {
    version: String,
    status: TaskStatus,
}

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
    fn get_last_result(&self, version: u64) -> zbus::Result<String>;
    fn updates_list(&self) -> zbus::Result<String>;
    #[zbus(signal)]
    fn status(&self, status: String) -> zbus::Result<()>;
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let connection = zbus::Connection::system().await?;
    let proxy = AmoProxy::new(&connection).await?;

    let mut status_stream = proxy.receive_status().await?;

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

    let result = proxy.get_last_result(id).await?;
    let result: ResultReport = serde_json::from_str(&result)?;

    if result.status == TaskStatus::Success {
        let updates_list = proxy.updates_list().await?;
        let updates_list: OmaOperation = serde_json::from_str(&updates_list)?;
        println!("{}", updates_list);
    } else {
        bail!("Failed to refresh packages metadata: {result:#?}")
    }

    Ok(())
}
