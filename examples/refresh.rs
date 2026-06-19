use futures_util::StreamExt;
use zbus::proxy;

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
    fn refresh_status(&self, status: String) -> zbus::Result<()>;
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
        dbg!(signal.args().unwrap().status);
    }

    Ok(())
}
