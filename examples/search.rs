use oma_pm::{PackageStatus, search::SearchResult};
use zbus::{Connection, proxy};

#[proxy(
    interface = "io.aosc.Amo1",
    default_service = "io.aosc.Amo",
    default_path = "/io/aosc/Amo"
)]
trait Amo {
    async fn search(&self, query: String) -> zbus::Result<String>;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Connecting to System Bus...");
    let connection = Connection::system().await?;
    let proxy = AmoProxy::new(&connection).await?;

    let query_keyword = "telegram".to_string();
    println!("Searching for package: '{}'...", query_keyword);

    match proxy.search(query_keyword).await {
        Ok(json_reply) => {
            let results: Vec<SearchResult> = serde_json::from_str(&json_reply)?;

            if results.is_empty() {
                println!("No packages found matched the query.");
                return Ok(());
            }

            println!("\n Found {} results:", results.len());
            println!("{:-<80}", "");
            
            for pkg in results {
                let status_str = match pkg.status {
                    PackageStatus::Upgrade => " [Upgradable] ",
                    PackageStatus::Installed => " [Installed]  ",
                    PackageStatus::Avail => " [Available]  ",
                };

                println!(
                    "{:<30} {:<15} -> {:<15} {}",
                    pkg.name,
                    pkg.old_version.unwrap_or_else(|| "N/A".to_string()),
                    pkg.new_version,
                    status_str
                );
                println!("   Description: {}", pkg.desc);
                println!("   Base Metapackage: {}, Has DBG: {}", pkg.is_base, pkg.dbg_package);
                println!("{:-<80}", "");
            }
        }
        Err(zbus::Error::FDO(fdo_err)) => {
            eprintln!("D-Bus Service Error: {}", fdo_err);
        }
        Err(e) => {
            eprintln!("Unexpected Error: {}", e);
        }
    }

    Ok(())
}
