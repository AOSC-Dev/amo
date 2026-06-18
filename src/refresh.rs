use oma_pm::apt::AptConfig;
use oma_refresh::db::{Event, OmaRefresh};
use oma_utils::dpkg::dpkg_arch;
use reqwest::ClientBuilder;
use std::path::PathBuf;
use tokio::sync::mpsc::{UnboundedSender};

use crate::{
    USER_AGENT,
    server::{RefreshState, RefreshStatus},
};

pub fn refresh_impl(tx: UnboundedSender<RefreshStatus>) -> Result<(), String> {
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
        let msg = match ev {
            Event::DownloadEvent(event) => format!("Downloading: {event:#?}"),
            Event::ScanningTopic => "Scanning topic...".to_string(),
            Event::ClosingTopic(name) => format!("Closing topic: {name}"),
            Event::TopicNotInMirror { topic, mirror } => {
                format!("Topic {topic} not in mirror {mirror}")
            }
            Event::RunInvokeScript => "Running invoke script...".to_string(),
            Event::SourceListFileNotSupport { path } => {
                format!("Source list file not support: {}", path.display())
            }
            Event::Done => "Done!".to_string(),
        };

        println!("{}", msg);

        let _ = tx.send(RefreshStatus {
            state: RefreshState::Progress,
            message: msg,
        });
    })
    .map_err(|e| e.to_string())?;

    Ok(())
}
