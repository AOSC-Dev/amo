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

/// One parsed line from apt's install-progress status-fd stream.
///
/// `status` is the line type (`pmstatus`, `pmerror`, `pmconffile`,
/// `dlstatus`, or `dpkg_raw` for anything unrecognised); `percent` is the
/// overall progress in [0,100] and never decreases.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DpkgProgress<'a> {
    pub status: &'a str,
    pub stage: &'a str,
    pub package_or_dpkg_exec: &'a str,
    pub percent: f32,
    pub description: &'a str,
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

            // This fd is wired to apt's APT::Progress::PackageManagerProgressFd
            // (oma-apt's do_install_fd:
            //   https://github.com/AOSC-Dev/oma-apt/blob/v0.13.0/apt-pkg-c/pkgmanager.h#L51),
            // so the stream is apt's install-progress format, not raw dpkg
            // output. Every line is:  TYPE:ARG1:ARG2:MSG  (MSG may contain ':').
            //
            // Reference implementations (apt 3.3.3):
            //   - format writer   GetProgressFdString(),
            //     https://salsa.debian.org/apt-team/apt/-/blob/3.3.3/apt-pkg/install-progress.cc#L82-90
            //     (std::fixed, precision 4, classic locale)
            //   - pmstatus        StatusChanged(),
            //     https://salsa.debian.org/apt-team/apt/-/blob/3.3.3/apt-pkg/install-progress.cc#L131-140
            //     ARG2 = StepsDone/StepsTotal*100 over the whole operation,
            //     so it is already monotonic
            //   - pmstatus:dpkg-exec  StartDpkg(),
            //     https://salsa.debian.org/apt-team/apt/-/blob/3.3.3/apt-pkg/install-progress.cc#L94-105
            //     ("dpkg-exec" is apt's pseudo-package)
            //   - pmerror         Error(),
            //     https://salsa.debian.org/apt-team/apt/-/blob/3.3.3/apt-pkg/install-progress.cc#L112-118
            //   - pmconffile      ConffilePrompt(),
            //     https://salsa.debian.org/apt-team/apt/-/blob/3.3.3/apt-pkg/install-progress.cc#L121-127
            //   - dlstatus        https://salsa.debian.org/apt-team/apt/-/blob/3.3.3/apt-pkg/acquire.cc#L1512
            //     (download progress; not on this fd in oma's flow)
            //   - media-change    https://salsa.debian.org/apt-team/apt/-/blob/3.3.3/apt-pkg/acquire-worker.cc#L816-822
            //     note the literal "media-change: " prefix (with a space) and
            //     that ARG2 is the drive, not a percent
            //   - apt turns raw dpkg output into pmstatus/pmerror/pmconffile
            //     in pkgDPkgPM::ProcessDpkgStatusLine(),
            //     https://salsa.debian.org/apt-team/apt/-/blob/3.3.3/apt-pkg/deb/dpkgpm.cc#L573
            //   - format documented in
            //     https://salsa.debian.org/apt-team/apt/-/blob/3.3.3/doc/progress-reporting.md
            //     real captured samples in
            //     https://salsa.debian.org/apt-team/apt/-/blob/3.3.3/test/integration/test-apt-progress-fd
            //
            // Example lines:
            //   pmstatus:testing:20.0000:Unpacking testing (amd64)
            //   pmstatus:dpkg-exec:0.0000:Running dpkg
            //   pmerror:testing:40.0000:error message...
            //   pmconffile:testing:40.0000:'/etc/foo'...
            //   dlstatus:1:100.0000:Retrieving file 1 of 1
            //   media-change: cdrom:/media/cdrom:Please insert...
            //
            // Note: raw dpkg --status-fd (1.23.7) itself never emits a
            // percent. It only sends status/error/conffile/processing lines:
            //   - error            https://salsa.debian.org/dpkg-team/dpkg/-/blob/1.23.7/src/main/errors.c#L90
            //                      https://salsa.debian.org/dpkg-team/dpkg/-/blob/1.23.7/src/main/errors.c#L103
            //   - conffile prompt  https://salsa.debian.org/dpkg-team/dpkg/-/blob/1.23.7/src/main/configure.c#L292
            //   - processing       https://salsa.debian.org/dpkg-team/dpkg/-/blob/1.23.7/src/main/help.c#L352
            //   - status state     https://salsa.debian.org/dpkg-team/dpkg/-/blob/1.23.7/lib/dpkg/dbmodify.c#L538
            //   all written by statusfd_send(),
            //   https://salsa.debian.org/dpkg-team/dpkg/-/blob/1.23.7/lib/dpkg/log.c#L105
            // The percent is synthesised by apt. Every dpkg re-run
            // (unpack/configure/triggers blocks) can emit "dpkg-exec" again,
            // sometimes at 0.0%, so the bar must never move backwards: keep
            // `overall` monotonic.
            let mut overall: f32 = 0.0;

            for line in reader.lines() {
                let Ok(progress_line) = line else { break };

                // Split into at most 4 fields; the message tail may itself
                // contain ':'. Trim the leading fields: media-change is
                // emitted with a literal "media-change: " prefix.
                let mut fields = progress_line.splitn(4, ':');
                let Some(kind) = fields.next() else { continue };
                let kind = kind.trim();
                let arg1 = fields.next().unwrap_or("").trim();
                let arg2 = fields.next().unwrap_or("").trim();
                let msg = fields.next().unwrap_or("");

                let (status, stage, package_or_dpkg_exec, description) = match kind {
                    // Progress: arg1 = package (or the "dpkg-exec" pseudo
                    // package), arg2 = the overall percent apt already
                    // computed. Take it verbatim, only clamped monotonic.
                    "pmstatus" => {
                        let percent = arg2.parse::<f32>().unwrap_or(overall);
                        overall = overall.max(percent.clamp(0.0, 100.0));
                        ("pmstatus", "dpkg", arg1, msg)
                    }
                    // Errors carry a percent but must not advance the bar;
                    // relay the message so the UI can surface the failure.
                    "pmerror" => ("pmerror", "dpkg", arg1, msg),
                    // Conffile prompts: same treatment as errors.
                    "pmconffile" => ("pmconffile", "dpkg", arg1, msg),
                    // Download progress. It carries a percent, but that is
                    // download progress, not dpkg progress; and it normally
                    // does not even appear on this fd (downloads go through a
                    // separate channel in oma). Relay it without moving the
                    // dpkg bar, so a download phase can't push the bar to
                    // 100 prematurely.
                    "dlstatus" => ("dlstatus", "download", arg1, msg),
                    // media-change and anything unrecognised: hold the bar
                    // and relay the raw line so the UI can show what's up.
                    _ => ("dpkg_raw", "dpkg_raw", arg1, progress_line.as_str()),
                };

                let progress_obj = DpkgProgress {
                    status,
                    stage,
                    package_or_dpkg_exec,
                    percent: overall,
                    description,
                };

                if let Ok(json_str) = serde_json::to_string(&progress_obj)
                    && let Err(e) = tx_for_dpkg.send(json_str)
                {
                    error!("Failed to send dpkg progress: {e}");
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
