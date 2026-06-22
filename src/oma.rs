use oma_pm::{
    apt::{AptConfig, OmaApt, OmaAptArgs, OmaOperation}, sort::SummarySort,
};
use oma_refresh::db::OmaRefresh;
use oma_utils::dpkg::dpkg_arch;
use reqwest::ClientBuilder;
use std::path::PathBuf;
use tokio::sync::mpsc::UnboundedSender;
use tracing::error;

use crate::USER_AGENT;

pub fn refresh_impl(tx: UnboundedSender<String>) -> anyhow::Result<()> {
    let client = ClientBuilder::new().user_agent(USER_AGENT).build()?;

    let r = OmaRefresh::builder()
        .download_dir(PathBuf::from(
            AptConfig::new().dir("Dir::State::lists", "lists/"),
        ))
        .source(PathBuf::from("/"))
        .threads(4)
        .arch(dpkg_arch("/")?)
        .client(client.into())
        .refresh_topics(true)
        .topic_msg("".into())
        .build();

    r.start(move |ev| {
        let s = match serde_json::to_string(&ev) {
            Ok(s) => s,
            Err(e) => {
                error!(error = e.to_string(), "Failed to send error channel");
                return;
            }
        };

        if let Err(e) = tx.send(s) {
            error!(error = e.to_string(), "Failed to send message channel");
        }
    })?;

    Ok(())
}

pub fn updates_list() -> anyhow::Result<OmaOperation> {
    let mut apt = OmaApt::new(
        vec![],
        OmaAptArgs::builder().build(),
        false,
    )?;

    apt.upgrade(oma_pm::apt::Upgrade::FullUpgrade)?;
    apt.resolve(false, false)?;

    let op = apt.build_transaction(
        SummarySort::default().names().operation(),
        |_| false,
        |_| false,
    )?;

    Ok(op)
}
