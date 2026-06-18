use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
// 需要引入这个 Trait 来使用 .next() 遍历信号流
use zbus::{proxy, zvariant::Type};

#[derive(Serialize, Deserialize, Type, Clone, Debug)]
pub enum RefreshStateClient {
    Started,
    Progress,
    Finished,
    Failed,
}

#[derive(Serialize, Deserialize, Type, Clone, Debug)]
pub struct RefreshStatusClient {
    pub state: RefreshStateClient,
    pub message: String,
}

#[proxy(
    interface = "io.aosc.Amo1",
    default_service = "io.aosc.Amo",
    default_path = "/io/aosc/Amo"
)]
trait Amo {
    /// 触发刷新的异步方法
    fn refresh(&self) -> zbus::Result<()>;

    /// 声明客户端需要接收的信号
    /// zbus 会自动在生成的 AmoProxy 中包含一个 `receive_refresh_status` 的方法
    #[zbus(signal)]
    fn refresh_status(&self, status: RefreshStatusClient) -> zbus::Result<()>;
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let connection = zbus::Connection::system().await?;
    let proxy = AmoProxy::new(&connection).await?;

    let mut status_stream = proxy.receive_refresh_status().await?;

    println!("Triggering refresh...");
    proxy.refresh().await?;

    println!("Waiting for status updates via D-Bus Signals...\n");
    while let Some(signal) = status_stream.next().await {
        // 从信号中提取我们自定义的负载数据 RefreshStatus
        let RefreshStatusArgs { status, ..  } = signal.args()?;

        match status.state {
            RefreshStateClient::Started => {
                println!("[STATUS] 🚀 Refresh task started on server.");
            }
            RefreshStateClient::Progress => {
                // 实时打印服务端传过来的日志/进度
                println!("[PROGRESS] {}", status.message);
            }
            RefreshStateClient::Finished => {
                println!("\n[STATUS] 🎉 Work is finished successfully!");
                break; // 任务圆满结束，退出监听循环
            }
            RefreshStateClient::Failed => {
                eprintln!(
                    "\n[ERROR] ❌ Work finished with error: {}",
                    status.message
                );
                break; // 任务失败，退出监听循环
            }
        }
    }

    Ok(())
}
