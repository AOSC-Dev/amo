use zbus::proxy;

#[proxy(
    interface = "io.aosc.Amo1",
    default_service = "io.aosc.Amo",
    default_path = "/io/aosc/Amo"
)]
trait Amo {
    fn refresh(&mut self) -> zbus::Result<()>;
    fn work(&self) -> zbus::Result<String>;
    fn get_result(&mut self) -> zbus::Result<String>;
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let connection = zbus::Connection::system().await?;
    let mut proxy = AmoProxy::new(&connection).await?;

    proxy.refresh().await?;

    loop {
        let result = proxy.get_result().await?;

        match result.as_str() {
            "none" => println!("work is still running..."),
            "ok" => {
                println!("work is finished successfully");
                break;
            }
            _ => {
                println!("work is finished with error: {}", result);
                break;
            }
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    Ok(())
}
