use crate::oma::{OmaClient, refresh_impl};
use anyhow::anyhow;
use apt_auth_config::{AuthConfig, reqwuest::AuthMiddleware};
use notify::{
    EventKind, Watcher,
    event::{AccessKind, AccessMode},
};
use oma_fetch::reqwest::ClientBuilder;
use oma_pm::{
    apt::OmaOperation,
    oma_apt::{Cache, new_cache},
    search::{IndiciumSearch, OmaSearch, SearchType},
};
use reqwest_middleware::ClientWithMiddleware;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Mutex, RwLock, oneshot};
use tracing::{error, info};
use zbus::{Connection, fdo, interface, names::BusName, object_server::SignalEmitter};
use zbus_polkit::policykit1::{AuthorityProxy, CheckAuthorizationFlags, Subject};

pub enum AptTask {
    Apply {
        install_items: Vec<String>,
        remove_items: Vec<String>,
        upgrade_all: bool,
        progress_tx: tokio::sync::mpsc::UnboundedSender<String>,
        result_tx: tokio::sync::oneshot::Sender<Result<(), String>>,
        version: u64,
    },
    UpdateList {
        tx: tokio::sync::oneshot::Sender<Result<OmaOperation, String>>,
    },
    GetTransaction {
        install_items: Vec<String>,
        remove_items: Vec<String>,
        upgrade_all: bool,
        result_tx: tokio::sync::oneshot::Sender<Result<OmaOperation, String>>,
    },
    UpdateCache {
        result_tx: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
}

pub struct Amo {
    run_lock: Arc<Mutex<()>>,
    current_report: Arc<RwLock<Option<ResultReport>>>,
    current_version: AtomicU64,
    apt_task_tx: std::sync::mpsc::Sender<AptTask>,
    searcher: Arc<std::sync::Mutex<Option<Arc<IndiciumSearch>>>>,
    client: ClientWithMiddleware,
    desc_snapshot: Arc<std::sync::Mutex<Option<Arc<HashMap<String, String>>>>>,
}

impl Amo {
    pub fn new() -> anyhow::Result<Self> {
        let (task_tx, task_rx) = std::sync::mpsc::channel::<AptTask>();

        let searcher = Arc::new(std::sync::Mutex::new(Some(Arc::new(IndiciumSearch::new(
            &new_cache!()?,
            SearchType::Live,
            |_| {},
        )?))));

        let updating_cache_count = Arc::new(AtomicUsize::new(0));
        let updating_cache_count_for_watcher = updating_cache_count.clone();

        let task_tx_for_watcher = task_tx.clone();
        std::thread::spawn(move || {
            let apt_lists_path = "/var/lib/apt/lists";
            let dpkg_status_path = "/var/lib/dpkg/status";

            let (event_tx, event_rx) = std::sync::mpsc::channel();

            let mut watcher = notify::RecommendedWatcher::new(
                move |res| {
                    if let Ok(event) = res {
                        let _ = event_tx.send(event);
                    }
                },
                notify::Config::default(),
            )
            .expect("Failed to create native sync watcher");

            if let Err(e) = watcher.watch(
                Path::new(apt_lists_path),
                notify::RecursiveMode::NonRecursive,
            ) {
                error!(
                    "Sync watcher failed to watch remote path {}: {}",
                    apt_lists_path, e
                );
            }

            if let Err(e) = watcher.watch(
                Path::new(dpkg_status_path),
                notify::RecursiveMode::NonRecursive,
            ) {
                error!(
                    "Sync watcher failed to watch local status file {}: {}",
                    dpkg_status_path, e
                );
            }

            info!("Sync file watcher is now tracking BOTH remote lists and local dpkg status.");
            while let Ok(event) = event_rx.recv() {
                // info!("Recv Event: {event:?}");

                if event
                    .paths
                    .iter()
                    .all(|path| path.to_string_lossy().contains("/apt/lists/partial"))
                    || event
                        .paths
                        .iter()
                        .all(|path| path.to_string_lossy().contains("_InRelease"))
                    || event
                        .paths
                        .iter()
                        .all(|path| path.to_string_lossy().contains("_Release"))
                {
                    continue;
                }

                let is_close_write = match event.kind {
                    EventKind::Access(AccessKind::Close(AccessMode::Write)) => true,
                    _ => false,
                };

                if is_close_write {
                    if updating_cache_count_for_watcher
                        .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        info!("Detected via sync inotify, queuing ONE UpdateCache...");
                        let (res_tx, _) = tokio::sync::oneshot::channel();
                        let _ =
                            task_tx_for_watcher.send(AptTask::UpdateCache { result_tx: res_tx });
                    } else {
                        info!(
                            "UpdateCache task already in queue, bypassing redundant inotify event."
                        );
                    }
                }
            }
        });

        let searcher_for_worker = searcher.clone();
        let client = ClientBuilder::new().user_agent("oma/1.14.514").build()?;
        let client = reqwest_middleware::ClientBuilder::new(client)
            .with_init(AuthMiddleware::new(AuthConfig::system("/")?))
            .build();
        let client_ptr = client.clone();
        let desc_snapshot = Arc::new(std::sync::Mutex::new(Some(Arc::new(HashMap::new()))));
        let desc_snapshot_ptr = desc_snapshot.clone();

        std::thread::spawn(move || {
            let mut oma_client_opt = match OmaClient::new(client_ptr.clone(), vec![]) {
                Ok(a) => {
                    let new_map = update_pkg_description_cache(&a.apt.cache);
                    if let Ok(mut write) = desc_snapshot_ptr.lock() {
                        *write = Some(Arc::new(new_map));
                        info!("Package description map cached");
                    }
                    Some(a)
                }
                Err(e) => {
                    error!("Failed to initialize OmaApt in worker thread: {}", e);
                    return;
                }
            };

            while let Ok(task) = task_rx.recv() {
                match task {
                    AptTask::Apply {
                        install_items,
                        remove_items,
                        upgrade_all,
                        progress_tx,
                        result_tx,
                        version,
                    } => {
                        let retained_oma_client = oma_client_opt.take().unwrap();

                        let apply_result = (|| -> Result<(), anyhow::Error> {
                            let mut current_apt = retained_oma_client;

                            if !install_items.is_empty() {
                                info!(
                                    id = version,
                                    "Applying atomic transaction: Installing packages {:?}",
                                    install_items
                                );

                                let local_debs = install_items
                                    .iter()
                                    .filter(|name| name.ends_with(".deb"))
                                    .cloned()
                                    .collect::<Vec<_>>();

                                if !local_debs.is_empty() {
                                    current_apt = OmaClient::new(client_ptr.clone(), local_debs)?;
                                }

                                current_apt.install(install_items)?;
                            }

                            if !remove_items.is_empty() {
                                info!(
                                    id = version,
                                    "Applying atomic transaction: Removing packages {:?}",
                                    remove_items
                                );
                                current_apt.remove(remove_items)?;
                            }

                            if upgrade_all {
                                info!(
                                    id = version,
                                    "Applying atomic transaction: Marking full upgrade"
                                );
                                current_apt.upgrade_all()?;
                            }

                            info!(
                                id = version,
                                "Atomic transaction components staged. Committing change..."
                            );

                            current_apt
                                .commit(progress_tx, version)
                                .inspect(|_| info!(id = version, "apt transaction commit success"))
                                .inspect_err(|e| {
                                    error!(
                                        id = version,
                                        error = e.to_string(),
                                        "apt transaction commit failed"
                                    )
                                })?;

                            Ok(())
                        })();

                        match update_cache(
                            &searcher_for_worker,
                            &client_ptr,
                            &mut oma_client_opt,
                            desc_snapshot_ptr.clone(),
                        ) {
                            Ok(_) => {
                                let _ = result_tx.send(apply_result.map_err(|e| e.to_string()));
                            }
                            Err(e) => {
                                let _ = result_tx.send(Err(e.to_string()));
                            }
                        }
                    }
                    AptTask::UpdateList { tx } => {
                        let Some(ref mut oma_client) = oma_client_opt else {
                            error!("Critical: Apt instance is missing in the loop!");

                            let _ = tx.send(Err(
                                "Critical: Apt instance is missing in the loop!".to_string()
                            ));

                            continue;
                        };

                        let result = oma_client.summary(vec![], vec![], true);
                        let _ = tx.send(result.map_err(|e| e.to_string()));
                    }
                    AptTask::GetTransaction {
                        install_items,
                        remove_items,
                        upgrade_all,
                        result_tx,
                    } => {
                        let Some(ref mut oma_client) = oma_client_opt else {
                            error!("Critical: Apt instance is missing in the loop!");

                            let _ =
                                result_tx
                                    .send(Err("Critical: Apt instance is missing in the loop!"
                                        .to_string()));

                            continue;
                        };

                        let result = oma_client.summary(install_items, remove_items, upgrade_all);
                        let _ = result_tx.send(result.map_err(|e| e.to_string()));
                    }
                    AptTask::UpdateCache { result_tx } => {
                        match update_cache(
                            &searcher_for_worker,
                            &client_ptr,
                            &mut oma_client_opt,
                            desc_snapshot_ptr.clone(),
                        ) {
                            Ok(_) => {
                                let _ = result_tx.send(Ok(()));
                            }
                            Err(e) => {
                                let _ = result_tx.send(Err(e.to_string()));
                            }
                        }
                        updating_cache_count.store(0, Ordering::SeqCst);
                    }
                }
            }
        });

        Ok(Self {
            apt_task_tx: task_tx,
            run_lock: Arc::new(Mutex::new(())),
            current_report: Arc::new(RwLock::new(None)),
            current_version: AtomicU64::new(0),
            searcher,
            client: client.clone(),
            desc_snapshot,
        })
    }
}

fn update_cache(
    searcher_for_worker: &Arc<std::sync::Mutex<Option<Arc<IndiciumSearch>>>>,
    client_ptr: &ClientWithMiddleware,
    oma_client_opt: &mut Option<OmaClient>,
    desc_snapshot_ptr: Arc<std::sync::Mutex<Option<Arc<HashMap<String, String>>>>>,
) -> anyhow::Result<()> {
    let old_searcher = searcher_for_worker.lock().unwrap().take();
    let old_desc = desc_snapshot_ptr.lock().unwrap().take();

    let old_client = oma_client_opt.take();
    drop(old_client);
    let force_reload_cache = new_cache!()?;
    drop(force_reload_cache);

    match OmaClient::new(client_ptr.clone(), vec![]) {
        Ok(new_apt) => {
            let new_map = update_pkg_description_cache(&new_apt.apt.cache);

            info!("Work Thread: Re-build IndiciumSearcher ...");
            match IndiciumSearch::new(&new_apt.apt.cache, SearchType::Live, |_| {}) {
                Ok(new_engine) => {
                    *searcher_for_worker.lock().unwrap() = Some(Arc::new(new_engine));
                    *desc_snapshot_ptr.lock().unwrap() = Some(Arc::new(new_map));
                    info!("Worker Thread: Search index and description cache hot-swapped");
                }
                Err(e) => {
                    error!("Create new searcher failed: {e}");
                    *searcher_for_worker.lock().unwrap() = old_searcher;
                    *desc_snapshot_ptr.lock().unwrap() = old_desc;
                }
            }

            *oma_client_opt = Some(new_apt);

            Ok(())
        }
        Err(e) => {
            *searcher_for_worker.lock().unwrap() = old_searcher;
            *desc_snapshot_ptr.lock().unwrap() = old_desc;
            Err(e.context("Fatal environment reset failure"))
        }
    }
}

fn update_pkg_description_cache(cache: &Cache) -> HashMap<String, String> {
    let mut new_map = HashMap::new();

    for pkg in cache.packages(&Default::default()) {
        if let Some(cand) = pkg.candidate() {
            if let Some(desc) = cand.summary() {
                new_map.insert(pkg.fullname(true), desc.to_string());
            }
        }
    }

    new_map
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ResultReport {
    pub version: u64,
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
        auth(header, conn).await?;

        let Ok(_guard) = self.run_lock.try_lock() else {
            return Err(zbus::fdo::Error::Failed(
                "Another task is already running".to_string(),
            ));
        };

        drop(_guard);

        let next_version = self.current_version.fetch_add(1, Ordering::SeqCst) + 1;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();

        let ctxt_owned = ctxt.to_owned();

        tokio::spawn(async move {
            while let Some(status) = rx.recv().await {
                if let Err(e) = ctxt_owned.status(status.clone()).await {
                    error!(
                        msg = status,
                        error = e.to_string(),
                        "send refresh_satatus request got error"
                    );
                }
            }
        });

        let run_lock_clone = self.run_lock.clone();
        let report_saver = self.current_report.clone();
        let client = self.client.clone();
        let apt_task_tx = self.apt_task_tx.clone();

        tokio::task::spawn_blocking(move || {
            let (update_cache_tx, update_cache_rx) = oneshot::channel();

            let Ok(_keep_lock_alive) = run_lock_clone.try_lock() else {
                return;
            };

            let outcome = refresh_impl(tx.clone(), client);
            let _ = apt_task_tx.send(AptTask::UpdateCache {
                result_tx: update_cache_tx,
            });

            let apt_task_result = update_cache_rx.blocking_recv();
            let apt_task_result = match apt_task_result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(anyhow!("{e}")),
                Err(_) => Err(anyhow!("Worker panic or response dropped")),
            };

            let status = match outcome.and(apt_task_result) {
                Ok(_) => TaskStatus::Success,
                Err(e) => TaskStatus::Failed(e.to_string()),
            };

            tokio::runtime::Handle::current().block_on(async {
                let mut writer = report_saver.write().await;
                *writer = Some(ResultReport {
                    version: next_version,
                    status,
                });
            });
        });

        Ok(next_version)
    }

    async fn get_last_result(&self, expected_version: u64) -> zbus::fdo::Result<String> {
        let mut retry_count = 0;

        loop {
            let reader = self.current_report.read().await;

            match &*reader {
                Some(report) => {
                    if expected_version != 0 && report.version < expected_version {
                        drop(reader);

                        retry_count += 1;
                        if retry_count > 50 {
                            return Err(zbus::fdo::Error::Failed(
                                "Timeout waiting for report to flush".to_string(),
                            ));
                        }

                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                        continue;
                    }

                    let json_str =
                        serde_json::to_string(report).unwrap_or_else(|_| "null".to_string());
                    return Ok(json_str);
                }
                None => {
                    drop(reader);
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
            }
        }
    }

    #[tracing::instrument(ret, skip(self))]
    async fn updates_list(&self) -> zbus::fdo::Result<String> {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.apt_task_tx.send(AptTask::UpdateList { tx }) {
            error!(error = e.to_string(), "Send task channel failed");
        }

        match rx.await {
            Ok(Ok(op)) => {
                Ok(serde_json::to_string(&op)
                    .map_err(|e| zbus::fdo::Error::Failed(e.to_string()))?)
            }
            Ok(Err(err_msg)) => Err(zbus::fdo::Error::Failed(err_msg)),
            Err(_) => Err(zbus::fdo::Error::Failed(
                "Worker panic or response dropped".to_string(),
            )),
        }
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
        auth(header, conn).await?;

        let Ok(_guard) = self.run_lock.try_lock() else {
            return Err(zbus::fdo::Error::Failed(
                "Another task is already running".to_string(),
            ));
        };
        let next_version = self.current_version.fetch_add(1, Ordering::SeqCst) + 1;
        drop(_guard);

        let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let (result_tx, result_rx) = tokio::sync::oneshot::channel::<Result<(), String>>();

        let ctxt_owned = ctxt.to_owned();

        tokio::spawn(async move {
            while let Some(event_str) = progress_rx.recv().await {
                if let Err(e) = ctxt_owned.status(event_str).await {
                    error!("Failed to broadcast oma event signal: {}", e);
                }
            }
        });

        let task = AptTask::Apply {
            install_items: install,
            remove_items: remove,
            upgrade_all: upgrade,
            progress_tx,
            result_tx,
            version: next_version,
        };

        if self.apt_task_tx.send(task).is_err() {
            return Err(zbus::fdo::Error::Failed(
                "Internal worker thread died".to_string(),
            ));
        }

        let run_lock_clone = self.run_lock.clone();
        let report_saver = self.current_report.clone();

        tokio::spawn(async move {
            let Ok(_keep_lock_alive) = run_lock_clone.try_lock() else {
                error!("Failed to get lock");
                return;
            };

            let status = match result_rx.await {
                Ok(Ok(())) => TaskStatus::Success,
                Ok(Err(e)) => TaskStatus::Failed(e),
                Err(_) => TaskStatus::Failed("Worker panic".to_string()),
            };

            let mut writer = report_saver.write().await;
            *writer = Some(ResultReport {
                version: next_version,
                status,
            });
        });

        Ok(next_version)
    }

    #[tracing::instrument(ret, skip(self), fields(install = ?install, remove = ?remove, upgrade = upgrade))]
    async fn get_transaction(
        &self,
        install: Vec<String>,
        remove: Vec<String>,
        upgrade: bool,
    ) -> zbus::fdo::Result<String> {
        let (result_tx, result_rx) = oneshot::channel();
        if let Err(e) = self.apt_task_tx.send(AptTask::GetTransaction {
            install_items: install,
            remove_items: remove,
            upgrade_all: upgrade,
            result_tx,
        }) {
            error!(error = e.to_string(), "Send task channel failed");
        }

        match result_rx.await {
            Ok(Ok(op)) => {
                Ok(serde_json::to_string(&op)
                    .map_err(|e| zbus::fdo::Error::Failed(e.to_string()))?)
            }
            Ok(Err(err_msg)) => Err(zbus::fdo::Error::Failed(err_msg)),
            Err(_) => Err(zbus::fdo::Error::Failed(
                "Worker panic or response dropped".to_string(),
            )),
        }
    }

    #[tracing::instrument(ret, skip(self))]
    fn search(&self, query: String) -> zbus::fdo::Result<String> {
        let mut attempts = 0;

        let engine_snapshot = loop {
            let snapshot = self.searcher.lock().unwrap().clone();

            if let Some(engine) = snapshot {
                break engine;
            }

            attempts += 1;
            if attempts > 40 {
                return Err(zbus::fdo::Error::Failed(
                    "Search engine timeout".to_string(),
                ));
            }

            std::thread::sleep(Duration::from_millis(25));
        };

        match engine_snapshot.search(&query) {
            Ok(results) => Ok(serde_json::to_string(&results).unwrap_or_default()),
            Err(e) => Err(zbus::fdo::Error::Failed(e.to_string())),
        }
    }

    #[tracing::instrument(ret, skip(self))]
    fn get_description(&self, pkg_name: String) -> zbus::fdo::Result<String> {
        let mut attempts = 0;

        let map = loop {
            let snapshot = self.desc_snapshot.lock().unwrap().clone();

            if let Some(engine) = snapshot {
                break engine;
            }

            attempts += 1;
            if attempts > 40 {
                return Err(zbus::fdo::Error::Failed(
                    "Search engine timeout".to_string(),
                ));
            }

            std::thread::sleep(Duration::from_millis(25));
        };

        match map.get(&pkg_name) {
            Some(desc) => Ok(desc.clone()),
            None => Ok("No description available".to_string()),
        }
    }

    #[zbus(signal)]
    async fn status(ctxt: &SignalEmitter<'_>, status: String) -> zbus::Result<()>;
}

pub async fn auth(header: zbus::message::Header<'_>, conn: &Connection) -> Result<(), fdo::Error> {
    let sender = header
        .sender()
        .ok_or_else(|| fdo::Error::AccessDenied("Unknown sender".to_string()))?
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
            "io.aosc.Amo.apply.run",
            &std::collections::HashMap::new(),
            CheckAuthorizationFlags::AllowUserInteraction.into(),
            "",
        )
        .await?;

    if !result.is_authorized {
        return Err(fdo::Error::AccessDenied("not authorized".to_string()));
    }

    Ok(())
}
