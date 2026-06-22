use oma_pm::{
    CommitConfig,
    apt::{AptConfig, OmaApt, OmaAptArgs, OmaOperation},
    matches::PackagesMatcher,
    sort::SummarySort,
};
use oma_refresh::db::OmaRefresh;
use oma_utils::dpkg::dpkg_arch;
use reqwest::Client;
use reqwest_middleware::ClientBuilder;
use serde::{Deserialize, Serialize};
use std::{io::BufRead, os::fd::AsRawFd, path::PathBuf};
use tokio::sync::mpsc::UnboundedSender;
use tracing::error;

use crate::USER_AGENT;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DpkgProgress {
    pub stage: String,
    pub package: String,
    pub percent: f32,
    pub description: String,
}

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

    pub fn commit(
        mut self,
        progress_tx: UnboundedSender<String>,
        version: u64,
    ) -> anyhow::Result<()> {
        self.apt.resolve(false, false)?;

        let op = self
            .apt
            .build_transaction(SummarySort::default(), |_| false, |_| false)?;

        let client =
            ClientBuilder::new(Client::builder().user_agent("oma/1.14.514").build()?).build();

        let tx_for_event = progress_tx.clone();
        let tx_for_dpkg = progress_tx.clone();
        let tx = progress_tx.clone();
        let (pipe_reader, pipe_writer) = os_pipe::pipe()?;

        std::thread::spawn(move || {
            let reader = std::io::BufReader::new(pipe_reader);
            for line in reader.lines() {
                if let Ok(progress_line) = line {
                    if progress_line.starts_with("pmstatus:") {
                        let parts: Vec<&str> = progress_line.split(':').collect();

                        if parts.len() >= 4 {
                            let package = parts[1].to_string();
                            let percent = parts[2].parse::<f32>().unwrap_or(0.0);
                            let description = parts[3..].join(":");

                            let progress_obj = DpkgProgress {
                                stage: "dpkg".to_string(),
                                package,
                                percent,
                                description,
                            };

                            if let Ok(json_str) = serde_json::to_string(&progress_obj) {
                                let _ = tx_for_dpkg.send(json_str);
                            }
                            continue;
                        }
                    }

                    let fallback_obj = DpkgProgress {
                        stage: "dpkg_raw".to_string(),
                        package: "unknown".to_string(),
                        percent: 0.0,
                        description: progress_line,
                    };
                    if let Ok(json_str) = serde_json::to_string(&fallback_obj) {
                        let _ = tx_for_dpkg.send(json_str);
                    }
                } else {
                    break;
                }
            }
        });

        self.apt.commit(
            oma_pm::apt::InstallProgressOpt::Fd(pipe_writer.as_raw_fd()),
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

        let _ = tx.send(serde_json::json!({"status": "finished", "version": version}).to_string());

        Ok(())
    }

    pub fn updates_list(&mut self) -> anyhow::Result<OmaOperation> {
        let op = self.upgrade_inner()?;
        self.apt.cache.depcache().clear_marked()?;

        Ok(op)
    }

    pub fn upgrade_all(&mut self) -> anyhow::Result<OmaOperation> {
        self.upgrade_inner()
    }

    fn upgrade_inner(&mut self) -> anyhow::Result<OmaOperation> {
        self.apt.upgrade(oma_pm::apt::Upgrade::FullUpgrade)?;
        self.apt.resolve(false, false)?;
        let op = self.apt.build_transaction(
            SummarySort::default().names().operation(),
            |_| false,
            |_| false,
        )?;

        Ok(op)
    }

    pub fn remove(&mut self, packages: Vec<String>) -> anyhow::Result<()> {
        let matcher = PackagesMatcher::builder().cache(&self.apt.cache).build();
        let mut no_result = vec![];
        let mut pkgs = vec![];

        for i in &packages {
            let res = matcher.match_pkgs_from_glob(i)?;
            if res.is_empty() {
                no_result.push(i.as_str());
            } else {
                pkgs.extend(res);
            }
        }

        self.apt.remove(pkgs, false, true)?;

        Ok(())
    }
}
