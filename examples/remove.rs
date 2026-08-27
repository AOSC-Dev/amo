use anyhow::bail;
use serde::{Deserialize, Serialize};

#[path = "common/mod.rs"]
mod common;
use common::{TransactionClient, TxEvent};

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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Connecting to System D-Bus...");
    let client = TransactionClient::connect().await?;
    let mut tx = client.create().await?;

    let packages_to_remove = vec!["fish"];
    println!(
        "[Step 1] Requesting install marking for: {:?}",
        packages_to_remove
    );

    println!("[Step 2] Triggering transaction commit...");
    if let Err(e) = tx.proxy.apply_changes(vec![], packages_to_remove, false).await {
        bail!("[Step 2 Failed] Failed to trigger commit: {}", e);
    }
    println!("[Step 2 Dispatched] Commit request accepted by server.");
    println!(
        "The server is processing the download/installation asynchronously in the background."
    );

    println!("[Signal Listener] Thread started, waiting for progress events...");
    while let Some(event) = tx.next_event().await? {
        match event {
            TxEvent::Status(event) => {
                let status: Progress = serde_json::from_value(event)?;
                println!("Status: {:?}", status);
                if let Progress::Done { status, request_id } = status {
                    let date = request_id >> 32;
                    let seq = request_id & 0xFFFFFFFF;
                    println!(
                        "Status: {}({}) date: {}, seq: {}",
                        status, request_id, date, seq
                    );
                }
            }
            TxEvent::Result(report) => {
                println!("Client finished successfully.");
                println!("{:#?}", report);
                return Ok(());
            }
        }
    }

    Ok(())
}
