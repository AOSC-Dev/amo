use crate::oma::{OmaClient, refresh_impl};
use anyhow::anyhow;
use apt_auth_config::{AuthConfig, reqwuest::AuthMiddleware};
use chrono::Datelike;
use notify::{
    EventKind, Watcher,
    event::{AccessKind, AccessMode},
};
use oma_apt_pkg::{AptDb, DpkgState, IndiciumSearch, OmaSearch, SearchType};
use oma_fetch::reqwest::ClientBuilder;
use oma_pm::apt::AptConfig;
use reqwest_middleware::ClientWithMiddleware;
use serde::{Deserialize, Serialize};
use std::{
    path::Path,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::{Mutex, watch};
use tracing::{error, info};
use zbus::{Connection, fdo, interface, names::BusName, object_server::SignalEmitter};
use zbus_polkit::policykit1::{AuthorityProxy, CheckAuthorizationFlags, Subject};

pub struct Amo {
    run_lock: Arc<Mutex<()>>,
    current_report_rx: watch::Receiver<Option<ResultReport>>,
    current_report_tx: watch::Sender<Option<ResultReport>>,
    searcher: Arc<RwLock<IndiciumSearch>>,
    client: ClientWithMiddleware,
    request_id_state: AtomicU64,
    client_ptr: ClientWithMiddleware,
}

impl Amo {
    pub fn new() -> anyhow::Result<Self> {
        let apt_config = AptConfig::new();
        apt_config.set("Dir", "/");
        apt_config.set("RootDir", "/");
        let lists_dir = apt_config.dir("Dir::State::lists", "lists/");

        let apt_db = AptDb::load_or_build(
            apt_config.file("Dir::Cache::oma-aptdb", "var/cache/apt/oma-aptdb.bincode"),
            &lists_dir,
        )
        .map_err(|e| anyhow::anyhow!("Failed to build oma packages database: {e}"))?;

        let dpkg_path = apt_config.file("Dir::State::status", "var/lib/dpkg/status");
        let dpkg = DpkgState::from_file(&dpkg_path)
            .map_err(|e| anyhow::anyhow!("Failed to parse dpkg status: {e}"))?;

        let searcher = Arc::new(RwLock::new(
            IndiciumSearch::new_with_cache(
                &apt_db,
                &dpkg,
                &lists_dir,
                apt_config.file("Dir::Cache::oma-search", "var/cache/apt/oma-search.bincode"),
                SearchType::Live,
                |_| {},
            )
            .map_err(|e| anyhow::anyhow!("Failed to build search index: {e}"))?,
        ));

        let (current_report_tx, current_report_rx) = watch::channel(None);

        let client = ClientBuilder::new().user_agent("oma/1.14.514").build()?;
        let client = reqwest_middleware::ClientBuilder::new(client)
            .with_init(AuthMiddleware::new(AuthConfig::system("/")?))
            .build();

        let client_ptr = client.clone();
        let searcher_for_watcher = searcher.clone();

        std::thread::spawn(move || {
            let dpkg_status_dir = "/var/lib/dpkg";
            let apt_lists_path = "/var/lib/apt/lists";

            let (event_tx, event_rx) = std::sync::mpsc::channel();

            let mut watcher = match notify::RecommendedWatcher::new(
                move |res| {
                    if let Ok(event) = res
                        && let Err(e) = event_tx.send(event)
                    {
                        error!("File watcher event channel closed: {e}");
                    }
                },
                notify::Config::default(),
            ) {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to create watcher: {e}");
                    return;
                }
            };

            let _ = watcher.watch(
                Path::new(dpkg_status_dir),
                notify::RecursiveMode::NonRecursive,
            );
            let _ = watcher.watch(
                Path::new(apt_lists_path),
                notify::RecursiveMode::NonRecursive,
            );

            while let Ok(event) = event_rx.recv() {
                let relevant = event.paths.iter().any(|path| {
                    let s = path.to_string_lossy();
                    if s.contains("/var/lib/dpkg/") {
                        return s.ends_with("/status");
                    }
                    if s.contains("/apt/lists/") {
                        return !s.contains("/partial/")
                            && !s.contains("_InRelease")
                            && !s.contains("_Release")
                            && !s.ends_with("/lock");
                    }
                    true
                });

                if !relevant {
                    continue;
                }

                if event.kind == EventKind::Access(AccessKind::Close(AccessMode::Write))
                    || matches!(
                        event.kind,
                        EventKind::Modify(notify::event::ModifyKind::Name(_))
                    )
                    || matches!(event.kind, EventKind::Remove(_))
                {
                    info!("File changed, refreshing cache ...");
                    update_cache(&searcher_for_watcher);
                }
            }
        });

        Ok(Self {
            run_lock: Arc::new(Mutex::new(())),
            current_report_rx,
            current_report_tx,
            searcher,
            client: client.clone(),
            request_id_state: AtomicU64::new(current_date_val()),
            client_ptr,
        })
    }

    fn generate_next_request_id(&self) -> u64 {
        let current_date_val = current_date_val();
        let mut old_state = self.request_id_state.load(Ordering::Relaxed);

        loop {
            // 右移 32 位拿日期，与掩码做按位与拿低 32 位序列号
            let old_date = old_state >> 32;
            let old_seq = old_state & 0xFFFFFFFF;

            let (new_date, new_seq) = if old_date != current_date_val {
                // 跨天了：重置序列号为 1
                (current_date_val, 1)
            } else {
                // 同一天：序列号直接自增（64 位下上限 4,294,967,295）
                (old_date, old_seq + 1)
            };

            // 重新拼装成一个
            let target_state = (new_date << 32) | new_seq;

            match self.request_id_state.compare_exchange_weak(
                old_state,
                target_state,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => return target_state,
                Err(actual) => old_state = actual, // 说明被其他线程做了这件事
            }
        }
    }
}

fn current_date_val() -> u64 {
    let now = chrono::Local::now();
    let yy = now.year() as u64;
    let mm = now.month() as u64;
    let dd = now.day() as u64;

    yy * 10000 + mm * 100 + dd
}

fn update_cache(searcher: &Arc<RwLock<IndiciumSearch>>) {
    let apt_config = AptConfig::new();
    let lists_dir = apt_config.dir("Dir::State::lists", "lists/");
    let cache_path = apt_config.file("Dir::Cache::oma-aptdb", "var/cache/apt/oma-aptdb.bincode");
    let dpkg_path = apt_config.file("Dir::State::status", "var/lib/dpkg/status");

    let apt_db = match AptDb::load_or_build(&cache_path, &lists_dir) {
        Ok(db) => db,
        Err(e) => {
            error!("Failed to rebuild AptDb: {e}");
            return;
        }
    };
    let dpkg = match DpkgState::from_file(&dpkg_path) {
        Ok(state) => state,
        Err(e) => {
            error!("Failed to read dpkg status: {e}");
            return;
        }
    };

    let mut searcher = searcher.write().unwrap();
    searcher.refresh_from(&apt_db, &dpkg);

    info!("Search index status refreshed");
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ResultReport {
    pub request_id: u64,
    pub status: TaskStatus,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub enum TaskStatus {
    Success,
    Failed(String),
}

#[interface(name = "io.aosc.Amo1")]
impl Amo {
    #[tracing::instrument(ret, skip(self, ctxt, conn))]
    async fn refresh(
        &self,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(signal_context)] ctxt: SignalEmitter<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
    ) -> zbus::fdo::Result<u64> {
        auth(header, conn, "io.aosc.amo.refresh").await?;

        let run_lock = self.run_lock.clone();
        let Ok(guard) = run_lock.try_lock_owned() else {
            return Err(zbus::fdo::Error::Failed(
                "Another task is already running!".to_string(),
            ));
        };

        let request_id = self.generate_next_request_id();

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let ctxt_owned = ctxt.to_owned();

        tokio::spawn(async move {
            while let Some(status) = rx.recv().await {
                if let Err(e) = ctxt_owned.status(status.clone()).await {
                    error!(
                        msg = status,
                        error = e.to_string(),
                        "Failed to send refresh_status request!"
                    );
                }
            }
        });

        let client = self.client.clone();
        let searcher = self.searcher.clone();
        let report_tx = self.current_report_tx.clone();

        tokio::task::spawn_blocking(move || {
            let _keep_lock_alive = guard;

            let outcome = refresh_impl(tx, client.clone());
            let _ = OmaClient::new(client, vec![]);
            update_cache(&searcher);

            let status = match outcome {
                Ok(_) => TaskStatus::Success,
                Err(e) => TaskStatus::Failed(e.to_string()),
            };

            if let Err(e) = report_tx.send(Some(ResultReport { request_id, status })) {
                error!("Failed to broadcast refresh result: {e}");
            }
        });

        Ok(request_id)
    }

    async fn get_last_result(&self, expected_request_id: u64) -> zbus::fdo::Result<String> {
        let mut rx = self.current_report_rx.clone();

        loop {
            if let Some(ref report) = *rx.borrow()
                && (expected_request_id == 0 || report.request_id >= expected_request_id)
            {
                return serde_json::to_string(report)
                    .map_err(|e| zbus::fdo::Error::Failed(format!("Serialization error: {e}")));
            }

            if rx.changed().await.is_err() {
                return Err(zbus::fdo::Error::Failed(
                    "Internal report channel closed".to_string(),
                ));
            }
        }
    }

    #[tracing::instrument(ret, skip(self))]
    async fn updates_list(&self) -> zbus::fdo::Result<String> {
        let run_lock = self.run_lock.clone();
        let Ok(guard) = run_lock.try_lock_owned() else {
            return Err(zbus::fdo::Error::Failed(
                "Another task is already running!".to_string(),
            ));
        };

        let client = self.client_ptr.clone();

        let result = tokio::task::spawn_blocking(move || {
            let _guard = guard;
            let mut apt = OmaClient::new(client, vec![])?;
            apt.summary(vec![], vec![], true)
                .map_err(|e| anyhow!("{e}"))
        })
        .await
        .map_err(|e| zbus::fdo::Error::Failed(format!("Task failed: {e}")))?
        .map_err(|e| zbus::fdo::Error::Failed(e.to_string()))?;

        serde_json::to_string(&result).map_err(|e| zbus::fdo::Error::Failed(e.to_string()))
    }

    #[tracing::instrument(ret, skip(self, conn, ctxt), fields(install = ?install, remove = ?remove, upgrade = upgrade))]
    async fn apply_changes(
        &self,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
        #[zbus(signal_context)] ctxt: SignalEmitter<'_>,
        install: Vec<String>,
        remove: Vec<String>,
        upgrade: bool,
    ) -> zbus::fdo::Result<u64> {
        auth(header, conn, "io.aosc.Amo.apply.run").await?;

        let run_lock = self.run_lock.clone();
        let Ok(guard) = run_lock.try_lock_owned() else {
            return Err(zbus::fdo::Error::Failed(
                "Another task is already running!".to_string(),
            ));
        };
        let request_id = self.generate_next_request_id();

        let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let ctxt_owned = ctxt.to_owned();

        tokio::spawn(async move {
            while let Some(event_str) = progress_rx.recv().await {
                if let Err(e) = ctxt_owned.status(event_str).await {
                    error!("Failed to broadcast oma event signal: {}", e);
                }
            }
        });

        let client = self.client_ptr.clone();
        let searcher = self.searcher.clone();
        let report_tx = self.current_report_tx.clone();

        tokio::spawn(async move {
            let _guard = guard;

            let result = (|| -> Result<(), anyhow::Error> {
                let mut current_apt = OmaClient::new(client.clone(), vec![])?;

                if !install.is_empty() {
                    let local_debs = install
                        .iter()
                        .filter(|name| name.ends_with(".deb"))
                        .cloned()
                        .collect::<Vec<_>>();

                    if !local_debs.is_empty() {
                        current_apt = OmaClient::new(client, local_debs)?;
                    }

                    current_apt.install(install)?;
                }

                if !remove.is_empty() {
                    current_apt.remove(remove)?;
                }

                if upgrade {
                    current_apt.upgrade_all()?;
                }

                current_apt.commit(progress_tx, request_id)?;
                Ok(())
            })();

            update_cache(&searcher);

            let status = match result {
                Ok(_) => TaskStatus::Success,
                Err(e) => TaskStatus::Failed(e.to_string()),
            };

            if let Err(e) = report_tx.send(Some(ResultReport { request_id, status })) {
                error!("Failed to broadcast apply result: {e}");
            }
        });

        Ok(request_id)
    }

    #[tracing::instrument(ret, skip(self), fields(install = ?install, remove = ?remove, upgrade = upgrade))]
    async fn get_transaction(
        &self,
        install: Vec<String>,
        remove: Vec<String>,
        upgrade: bool,
    ) -> zbus::fdo::Result<String> {
        let run_lock = self.run_lock.clone();
        let Ok(guard) = run_lock.try_lock_owned() else {
            return Err(zbus::fdo::Error::Failed(
                "Another task is already running!".to_string(),
            ));
        };

        let client = self.client_ptr.clone();

        let result = tokio::task::spawn_blocking(move || {
            let _guard = guard;
            let mut apt = OmaClient::new(client, vec![])?;
            apt.summary(install, remove, upgrade)
                .map_err(|e| anyhow!("{e}"))
        })
        .await
        .map_err(|e| zbus::fdo::Error::Failed(format!("Task failed: {e}")))?
        .map_err(|e| zbus::fdo::Error::Failed(e.to_string()))?;

        serde_json::to_string(&result).map_err(|e| zbus::fdo::Error::Failed(e.to_string()))
    }

    #[tracing::instrument(ret, skip(self))]
    async fn search(&self, query: String) -> zbus::fdo::Result<String> {
        let engine = self.searcher.read().unwrap();

        match engine.search(&query) {
            Ok(results) => serde_json::to_string(&results)
                .map_err(|e| zbus::fdo::Error::Failed(format!("Search serialization error: {e}"))),
            Err(e) => Err(zbus::fdo::Error::Failed(e.to_string())),
        }
    }

    #[tracing::instrument(ret, skip(self))]
    async fn get_description(&self, pkg_name: String) -> zbus::fdo::Result<String> {
        let engine = self.searcher.read().unwrap();

        match engine.pkg_map.get(&pkg_name) {
            Some(entry) => Ok(entry.description.clone()),
            None => Ok("No description available.".to_string()),
        }
    }

    #[zbus(signal)]
    async fn status(ctxt: &SignalEmitter<'_>, status: String) -> zbus::Result<()>;
}

pub async fn auth(
    header: zbus::message::Header<'_>,
    conn: &Connection,
    action: &str,
) -> Result<(), fdo::Error> {
    let sender = header
        .sender()
        .ok_or_else(|| fdo::Error::AccessDenied("Unknown sender!".to_string()))?
        .to_owned();

    let dbus_proxy = zbus::fdo::DBusProxy::new(conn).await?;

    let bus_name = BusName::from(sender);
    let real_pid = dbus_proxy
        .get_connection_unix_process_id(bus_name.clone())
        .await?;
    let real_uid = dbus_proxy.get_connection_unix_user(bus_name).await?;

    let proxy = AuthorityProxy::new(conn).await?;
    let subject = Subject::new_for_owner(real_pid, None, Some(real_uid))
        .map_err(|e| fdo::Error::AccessDenied(e.to_string()))?;

    let result = proxy
        .check_authorization(
            &subject,
            action,
            &std::collections::HashMap::new(),
            CheckAuthorizationFlags::AllowUserInteraction.into(),
            "",
        )
        .await?;

    if !result.is_authorized {
        return Err(fdo::Error::AccessDenied("Authorized failed!".to_string()));
    }

    Ok(())
}
