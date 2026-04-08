use std::path::PathBuf;

use apt_auth_config::AuthConfig;
use oma_pm::apt::AptConfig;
use oma_refresh::db::OmaRefresh;
use oma_utils::dpkg::dpkg_arch;
use reqwest::ClientBuilder;

use crate::{USER_AGENT, server::auth};

pub async fn refresh_impl() -> zbus::fdo::Result<()> {
    auth().await?;

    let client = ClientBuilder::new()
        .user_agent(USER_AGENT)
        .build()
        .map_err(|e| zbus::fdo::Error::Failed(e.to_string()))?;

    let auth_config = AuthConfig::system("/").ok();
    let config = AptConfig::new();

    let r = OmaRefresh::builder()
        .download_dir(PathBuf::from(
            AptConfig::new().dir("Dir::State::lists", "lists/"),
        ))
        .source(PathBuf::from("/"))
        .threads(4)
        .arch(dpkg_arch("/").map_err(|e| zbus::fdo::Error::Failed(e.to_string()))?)
        .client(&client)
        .maybe_auth_config(auth_config.as_ref())
        .refresh_topics(true)
        .apt_config(&config)
        .topic_msg("")
        .build();

    r.start(|ev| async {
        match ev {
            oma_refresh::db::Event::DownloadEvent(event) => {
                println!("Download event: {event:#?}");
            }
            oma_refresh::db::Event::ScanningTopic => {
                println!("Scanning topic...");
            }
            oma_refresh::db::Event::ClosingTopic(name) => {
                println!("Closing topic: {name}");
            }
            oma_refresh::db::Event::TopicNotInMirror { topic, mirror } => {
                println!("Topic {topic} not in mirror {mirror}");
            }
            oma_refresh::db::Event::RunInvokeScript => {
                println!("Running invoke script...");
            }
            oma_refresh::db::Event::SourceListFileNotSupport { path } => {
                println!("Source list file not support: {}", path.display());
            }
            oma_refresh::db::Event::Done => {
                println!("Done!");
            }
        }
    })
    .await
    .map_err(|e| zbus::fdo::Error::Failed(e.to_string()))?;

    Ok(())
}
