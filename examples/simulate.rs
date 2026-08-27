use anyhow::bail;
use oma_pm::apt::OmaOperation;

#[path = "common/mod.rs"]
mod common;
use common::{TaskStatus, TransactionClient};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = TransactionClient::connect().await?;

    // 模拟也是一个排队执行的事务：创建事务对象并订阅信号（此时休眠），
    // 再调用 Simulate 开工。
    let mut tx = client.create().await?;
    println!("simulate: transaction created");
    tx.proxy.simulate(vec!["gnome-base"], vec![], false).await?;

    let report = tx.wait_result().await?;
    match report.status {
        TaskStatus::Success => {
            let op: OmaOperation =
                serde_json::from_value(report.result.expect("simulate result missing"))?;
            println!("{op}");
        }
        TaskStatus::Failed(e) => bail!("simulate failed: {e}"),
    }

    Ok(())
}
