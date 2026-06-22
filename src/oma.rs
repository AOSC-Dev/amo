use oma_pm::{
    CommitConfig,
    apt::{AptConfig, OmaApt, OmaAptArgs, OmaOperation},
    matches::PackagesMatcher,
    progress::InstallProgressManager,
    sort::SummarySort,
};
use oma_refresh::db::OmaRefresh;
use oma_utils::dpkg::dpkg_arch;
use reqwest::Client;
use reqwest_middleware::ClientBuilder;
use std::path::PathBuf;
use tokio::sync::mpsc::UnboundedSender;
use tracing::error;

use crate::USER_AGENT;

pub fn refresh_impl(tx: UnboundedSender<String>) -> anyhow::Result<()> {
    let client = reqwest::ClientBuilder::new()
        .user_agent(USER_AGENT)
        .build()?;

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

struct AmoInstallPM;

impl InstallProgressManager for AmoInstallPM {
    fn status_change(&self, _pkgname: &str, _steps_done: u64, _total_steps: u64) {}

    fn no_interactive(&self) -> bool {
        true
    }

    fn use_pty(&self) -> bool {
        false
    }
}

pub struct OmaClient {
    pub apt: OmaApt,
}

impl OmaClient {
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self {
            apt: OmaApt::new(vec![], OmaAptArgs::builder().build(), false)?,
        })
    }

    pub fn install(&mut self, items: Vec<String>) -> anyhow::Result<()> {
        let matcher = PackagesMatcher::builder().cache(&self.apt.cache).build();

        let (pkgs, _no_marked_install) =
            matcher.match_pkgs_and_versions(items.iter().map(|s| s.as_str()))?;

        self.apt.install(&pkgs, true)?;

        Ok(())
    }

    pub fn commit(mut self, progress_tx: UnboundedSender<String>) -> anyhow::Result<()> {
        self.apt.resolve(false, false)?;

        let op = self
            .apt
            .build_transaction(SummarySort::default(), |_| false, |_| false)?;

        let client =
            ClientBuilder::new(Client::builder().user_agent("oma/1.14.514").build()?).build();

        let tx_for_event = progress_tx.clone();

        self.apt.commit(
            oma_pm::apt::InstallProgressOpt::TermLike(Box::new(AmoInstallPM)),
            &op,
            &client,
            CommitConfig {
                network_thread: None,
                download_only: false,
            },
            None,
            async move |event| {
                let _ = tx_for_event.send(serde_json::to_string(&event).unwrap());
            },
        )?;

        Ok(())
    }

    pub fn updates_list(&mut self) -> anyhow::Result<OmaOperation> {
        self.apt.upgrade(oma_pm::apt::Upgrade::FullUpgrade)?;
        self.apt.resolve(false, false)?;

        let op = self.apt.build_transaction(
            SummarySort::default().names().operation(),
            |_| false,
            |_| false,
        )?;

        Ok(op)
    }
}
