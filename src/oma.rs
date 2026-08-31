use oma_history::HistoryInfo;
use oma_pm::{
    CommitConfig,
    apt::{AptConfig, InstallProgressOpt, OmaApt, OmaAptArgs, OmaOperation},
    matches::PackagesMatcher,
    sort::SummarySort,
};
use oma_refresh::db::OmaRefresh;
use oma_utils::dpkg::dpkg_arch;
use reqwest_middleware::ClientWithMiddleware;
use serde::{Deserialize, Serialize};
use std::{env, io::BufRead, os::fd::AsRawFd, path::PathBuf, time::{SystemTime, UNIX_EPOCH}};
use tokio::sync::mpsc::UnboundedSender;
use tracing::error;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DpkgProgress {
    pub status: String,
    pub stage: String,
    pub package_or_dpkg_exec: String,
    pub percent: f32,
    pub description: String,
}

pub fn refresh_impl(
    tx: UnboundedSender<String>,
    client: ClientWithMiddleware,
) -> anyhow::Result<()> {
    let r = OmaRefresh::builder()
        .download_dir(PathBuf::from(
            AptConfig::new().dir("Dir::State::lists", "lists/"),
        ))
        .source(PathBuf::from("/"))
        .threads(4)
        .arch(dpkg_arch("/")?)
        .client(client)
        .refresh_topics(true)
        .topic_msg("".into())
        .build();

    r.start(move |ev| {
        let s = match serde_json::to_string(&ev) {
            Ok(s) => s,
            Err(e) => {
                error!(error = e.to_string(), "Failed to send error channel!");
                return;
            }
        };

        if let Err(e) = tx.send(s) {
            error!(error = e.to_string(), "Failed to send message channel!");
        }
    })?;

    Ok(())
}

pub struct OmaClient {
    pub apt: OmaApt,
    client: ClientWithMiddleware,
}

impl OmaClient {
    pub fn new(client: ClientWithMiddleware, local_debs: Vec<String>) -> anyhow::Result<Self> {
        Ok(Self {
            apt: OmaApt::new(local_debs, OmaAptArgs::builder().build(), false)?,
            client,
        })
    }

    pub fn install(&mut self, items: Vec<String>) -> anyhow::Result<()> {
        let matcher = PackagesMatcher::builder().cache(&self.apt.cache).build();

        let (pkgs, _no_marked_install) =
            matcher.match_pkgs_and_versions(items.iter().map(|s| s.as_str()))?;

        self.apt.install(&pkgs, true)?;

        Ok(())
    }

    pub fn summary(
        &mut self,
        install: Vec<String>,
        remove: Vec<String>,
        upgrade: bool,
    ) -> anyhow::Result<OmaOperation> {
        if !install.is_empty() {
            self.install(install)?;
        }

        if !remove.is_empty() {
            self.remove(remove)?;
        }

        if upgrade {
            self.upgrade_all()?;
        }

        self.apt.resolve(false, false)?;

        let op = self
            .apt
            .build_transaction(SummarySort::default(), |_| false, |_| false)?;

        self.apt.cache.depcache().clear_marked()?;

        Ok(op)
    }

    pub fn commit(
        mut self,
        progress_tx: UnboundedSender<String>,
        request_id: u64,
    ) -> anyhow::Result<()> {
        unsafe {
            env::set_var("DEBIAN_FRONTEND", "passthrough");
            env::set_var("DEBCONF_PIPE", "/tmp/amo-debconf-sock");
        }

        self.apt.resolve(false, false)?;

        let op = self
            .apt
            .build_transaction(SummarySort::default(), |_| false, |_| false)?;

        if op.install.is_empty() && op.remove.is_empty() {
            return Ok(());
        }

        let mut history = match oma_history::History::new("/var/lib/oma/history.db", true, false) {
            Ok(h) => Some(h),
            Err(e) => {
                error!("Failed to open oma history database: {e}");
                None
            }
        };

        let history_id = match history.as_mut() {
            Some(h) => match h.write(HistoryInfo {
                summary: &op,
                start_time: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0),
                success: false,
                is_fix_broken: false,
                is_undo: false,
                topics_enabled: Vec::new(),
                topics_disabled: Vec::new(),
            }) {
                Ok(id) => Some(id),
                Err(e) => {
                    error!("Failed to write oma history entry: {e}");
                    None
                }
            },
            None => None,
        };

        let tx_for_event = progress_tx.clone();
        let tx_for_dpkg = progress_tx.clone();
        let tx = progress_tx.clone();
        let (pipe_reader, pipe_writer) = os_pipe::pipe()?;

        std::thread::spawn(move || {
            let reader = std::io::BufReader::new(pipe_reader);
            for line in reader.lines() {
                if let Ok(progress_line) = line {
                    let parts: Vec<&str> = progress_line.split(':').collect();

                    if parts.len() >= 4 {
                        let status = parts[0].to_string();
                        let package = parts[1].to_string();
                        let percent = parts[2].parse::<f32>().unwrap_or(0.0);
                        let description = parts[3..].join(":");

                        let progress_obj = DpkgProgress {
                            status,
                            stage: "dpkg".to_string(),
                            package_or_dpkg_exec: package,
                            percent,
                            description,
                        };

                        if let Ok(json_str) = serde_json::to_string(&progress_obj)
                            && let Err(e) = tx_for_dpkg.send(json_str)
                        {
                            error!("Failed to send dpkg progress: {e}");
                        }

                        continue;
                    }

                    let fallback = DpkgProgress {
                        status: "".to_string(),
                        stage: "dpkg_raw".to_string(),
                        package_or_dpkg_exec: "unknown".to_string(),
                        percent: 0.0,
                        description: progress_line,
                    };
                    if let Ok(json_str) = serde_json::to_string(&fallback)
                        && let Err(e) = tx_for_dpkg.send(json_str)
                    {
                        error!("Failed to send raw dpkg progress: {e}");
                    }
                } else {
                    break;
                }
            }
        });

        let commit_result = self.apt.commit(
            InstallProgressOpt::Fd(pipe_writer.as_raw_fd()),
            &op,
            &self.client,
            CommitConfig {
                network_thread: None,
                download_only: false,
            },
            None,
            move |event| {
                if let Ok(json_str) = serde_json::to_string(&event) {
                    if let Err(e) = tx_for_event.send(json_str) {
                        error!("Failed to send commit event: {e}");
                    }
                } else {
                    error!("Failed to serialize commit event");
                }
            },
        );

        if let (Some(h), Some(id)) = (history.as_mut(), history_id)
            && let Err(e) = h.edit_status(id, commit_result.is_ok())
        {
            error!("Failed to update oma history status: {e}");
        }

        commit_result?;

        if let Err(e) =
            tx.send(serde_json::json!({"status": "finished", "request_id": request_id}).to_string())
        {
            error!("Failed to send completion signal: {e}");
        }

        Ok(())
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
