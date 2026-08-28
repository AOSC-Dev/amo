use anyhow::bail;
use oma_fetch::Event as DownloadEvent;
use oma_pm::apt::OmaOperation;
use oma_refresh::db::Event;

#[path = "common/mod.rs"]
mod common;
use common::{TaskStatus, TransactionClient, TxEvent};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = TransactionClient::connect().await?;

    // 创建事务对象并订阅信号（此时休眠），再调用 Refresh 开工。
    let mut tx = client.create().await?;
    println!("[INFO] Triggering refresh...");
    tx.proxy.refresh().await?;

    println!("[INFO] Waiting for status updates via D-Bus Signals...\n");
    while let Some(event) = tx.next_event().await? {
        match event {
            TxEvent::Status(event) => {
                let msg: Event = serde_json::from_value(event)?;
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
                    }
                }
            }
            TxEvent::Result(report) => {
                match report.status {
                    TaskStatus::Success => {}
                    TaskStatus::Failed(e) => bail!("Failed to refresh packages metadata: {e}"),
                }
                break;
            }
            TxEvent::State(state) => {
                println!("[INFO] State: {:?}", state);
                if state == common::TxState::Cancelled {
                    bail!("refresh transaction cancelled");
                }
            }
        }
    }

    // 更新列表也是事务：从它的 ResultReport.result 里取 OmaOperation。
    // 先检查 status：UpdatesList 失败时 result 为 None，直接 expect 会 panic。
    let mut tx = client.create().await?;
    tx.proxy.updates_list().await?;
    let report = tx.wait_result().await?;
    let op: OmaOperation = match report.status {
        TaskStatus::Success => {
            serde_json::from_value(report.result.expect("updates list missing"))?
        }
        TaskStatus::Failed(e) => bail!("Failed to fetch updates list: {e}"),
    };
    println!("{}", op);

    Ok(())
}
