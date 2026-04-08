use zbus::proxy;

#[proxy(
    interface = "io.aosc.Amo1",
    default_service = "io.aosc.Amo",
    default_path = "/io/aosc/Amo"
)]
trait Amo {
    fn refresh(&mut self) -> zbus::Result<()>;
    fn work(&self) -> zbus::Result<String>;
    fn is_finished(&self) -> zbus::Result<bool>;
    fn get_error(&mut self) -> zbus::Result<String>;
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let connection = zbus::Connection::system().await?;
    let mut proxy = AmoProxy::new(&connection).await?;

    proxy.refresh().await?;

    loop {
        if proxy.is_finished().await? {
            let error = proxy.get_error().await?;
            if error == "none" {
                println!("work is finished successfully");
            } else {
                println!("work is finished with error: {}", error);
            }
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    Ok(())
}
