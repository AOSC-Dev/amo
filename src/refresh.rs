use oma_pm::apt::AptConfig;
use oma_refresh::db::OmaRefresh;
use oma_utils::dpkg::dpkg_arch;
use reqwest::ClientBuilder;
use std::path::PathBuf;
use tokio::sync::mpsc::UnboundedSender;

use crate::USER_AGENT;

pub fn refresh_impl(tx: UnboundedSender<String>) -> Result<(), String> {
    let client = ClientBuilder::new()
        .user_agent(USER_AGENT)
        .build()
        .map_err(|e| e.to_string())?;

    let r = OmaRefresh::builder()
        .download_dir(PathBuf::from(
            AptConfig::new().dir("Dir::State::lists", "lists/"),
        ))
        .source(PathBuf::from("/"))
        .threads(4)
        .arch(dpkg_arch("/").map_err(|e| e.to_string())?)
        .client(client.into())
        .refresh_topics(true)
        .topic_msg("".into())
        .build();

    r.start(move |ev| {
        let s = match serde_json::to_string(&ev) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Failed serialize event: {e}");
                return;
            }
        };

        if let Err(e) = tx.send(s) {
            eprintln!("Failed to send msg: {e}");
        }
    })
    .map_err(|e| e.to_string())?;

    Ok(())
}
