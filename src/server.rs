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
use tokio::sync::{Mutex, mpsc::UnboundedSender, oneshot, watch};
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
    current_report_rx: watch::Receiver<Option<ResultReport>>,
    current_report_tx: watch::Sender<Option<ResultReport>>,
    current_version: AtomicU64,
    apt_task_tx: UnboundedSender<AptTask>,
    searcher_rx: watch::Receiver<Option<Arc<IndiciumSearch>>>,
    client: ClientWithMiddleware,
    desc_rx: watch::Receiver<Option<Arc<HashMap<String, String>>>>,
}

impl Amo {
    pub fn new() -> anyhow::Result<Self> {
        let (task_tx, mut task_rx) = tokio::sync::mpsc::unbounded_channel();

        let initial_searcher = Arc::new(IndiciumSearch::new(
            &new_cache!()?,
            SearchType::Live,
            |_| {},
        )?);

        let (searcher_tx, searcher_rx) = watch::channel(Some(initial_searcher));
        let (desc_tx, desc_rx) = watch::channel(Some(Arc::new(HashMap::new())));
        let (current_report_tx, current_report_rx) = watch::channel(None);

        let updating_cache_count = Arc::new(AtomicUsize::new(0));

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis();
        let (apt_cache_version_tx, mut apt_cache_version_rx) = watch::channel(now);

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
            .expect("Failed to create watcher for local apt cache changes");

            if let Err(e) = watcher.watch(
                Path::new(apt_lists_path),
                notify::RecursiveMode::NonRecursive,
            ) {
                error!("Watcher failed to initialise for {}: {}", apt_lists_path, e);
            }

            if let Err(e) = watcher.watch(
                Path::new(dpkg_status_path),
                notify::RecursiveMode::NonRecursive,
            ) {
                error!(
                    "Watcher failed to initialise for {}: {}",
                    dpkg_status_path, e
                );
            }

            while let Ok(event) = event_rx.recv() {
                if event.paths.iter().all(|path| {
                    path.to_string_lossy().contains("/apt/lists/partial")
                        || path.to_string_lossy().contains("_InRelease")
                        || path.to_string_lossy().contains("_Release")
                }) {
                    continue;
                }

                if event.kind == EventKind::Access(AccessKind::Close(AccessMode::Write)) {
                    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
                        Ok(now) => {
                            let timestamp_ms = now.as_millis();
                            let _ = apt_cache_version_tx.send(timestamp_ms);
                        }
                        Err(e) => {
                            error!("Failed to get timestemp: {e}");
                        }
                    }
                }
            }
        });

        let client = ClientBuilder::new().user_agent("oma/1.14.514").build()?;
        let client = reqwest_middleware::ClientBuilder::new(client)
            .with_init(AuthMiddleware::new(AuthConfig::system("/")?))
            .build();

        let task_tx_for_notify_file = task_tx.clone();
        tokio::spawn(async move {
            let mut last_processed_version = now;

            apt_cache_version_rx.mark_unchanged();

            while apt_cache_version_rx.changed().await.is_ok() {
                let mut last_seen_version = *apt_cache_version_rx.borrow();

                if last_seen_version > last_processed_version {
                    loop {
                        tokio::time::sleep(Duration::from_millis(150)).await;

                        let current_disk_version = *apt_cache_version_rx.borrow();
                        if current_disk_version == last_seen_version {
                            break;
                        }
                        last_seen_version = current_disk_version;
                    }

                    info!(
                        "Got apt cache changed, version (timestemp ms): {} -> {}",
                        last_processed_version, last_seen_version
                    );

                    let (result_tx, result_rx) = oneshot::channel();
                    let _ = task_tx_for_notify_file.send(AptTask::UpdateCache { result_tx });

                    match result_rx.await {
                        Ok(Ok(_)) => {
                            last_processed_version = last_seen_version;
                            info!(
                                "Cache synchronized, now version #{}",
                                last_processed_version
                            );
                        }
                        Ok(Err(e)) => {
                            error!("Failed to refresh metadata: {e}");
                        }
                        Err(e) => {
                            error!("Failed to recv result: {e}");
                        }
                    }

                    apt_cache_version_rx.mark_unchanged();
                }
            }
        });

        let client_ptr = client.clone();

        std::thread::spawn(move || {
            let mut oma_client_opt = match OmaClient::new(client_ptr.clone(), vec![]) {
                Ok(a) => {
                    let new_map = update_pkg_description_cache(&a.apt.cache);
                    let _ = desc_tx.send(Some(Arc::new(new_map)));
                    info!("Package description map cached");
                    Some(a)
                }
                Err(e) => {
                    error!("Failed to initialize OmaApt in worker thread: {}", e);
                    return;
                }
            };

            while let Some(task) = task_rx.blocking_recv() {
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
                                    "Running task: Installing packages {:?} ...", install_items
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
                                    "Running task: Removing packages {:?} ...", remove_items
                                );
                                current_apt.remove(remove_items)?;
                            }

                            if upgrade_all {
                                info!(id = version, "Running task: Executing full upgrade ...");
                                current_apt.upgrade_all()?;
                            }

                            info!(id = version, "Committing changes ...");

                            current_apt
                                .commit(progress_tx, version)
                                .inspect(|_| {
                                    info!(id = version, "APT task completed successfully ...")
                                })
                                .inspect_err(|e| {
                                    error!(
                                        id = version,
                                        error = e.to_string(),
                                        "APT task failed to complete!"
                                    )
                                })?;

                            Ok(())
                        })();

                        if let Err(e) =
                            update_cache(&client_ptr, &mut oma_client_opt, &searcher_tx, &desc_tx)
                        {
                            error!("Failed to rebuild apt cache");
                            let _ = result_tx
                                .send(Err(format!("amo: Failed to rebuild apt cache: {e}")));
                            continue;
                        } else {
                            let _ = result_tx.send(apply_result.map_err(|e| e.to_string()));
                        }
                    }
                    AptTask::UpdateList { tx } => {
                        let Some(ref mut oma_client) = oma_client_opt else {
                            error!("Failed to create OmaClient instance!");
                            let _ = tx
                                .send(Err("amo: Failed to create OmaClient instance!".to_string()));
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
                            error!("Failed to create OmaClient instance!");
                            let _ = result_tx
                                .send(Err("amo: Failed to create OmaClient instance!".to_string()));
                            continue;
                        };

                        let result = oma_client.summary(install_items, remove_items, upgrade_all);
                        let _ = result_tx.send(result.map_err(|e| e.to_string()));
                    }
                    AptTask::UpdateCache { result_tx } => {
                        match update_cache(&client_ptr, &mut oma_client_opt, &searcher_tx, &desc_tx)
                        {
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
            current_report_rx,
            current_report_tx,
            current_version: AtomicU64::new(0),
            searcher_rx,
            client: client.clone(),
            desc_rx,
        })
    }
}

fn update_cache(
    client_ptr: &ClientWithMiddleware,
    oma_client_opt: &mut Option<OmaClient>,
    searcher_tx: &watch::Sender<Option<Arc<IndiciumSearch>>>,
    desc_tx: &watch::Sender<Option<Arc<HashMap<String, String>>>>,
) -> anyhow::Result<()> {
    let old_searcher = searcher_tx.borrow().clone();
    let old_desc = desc_tx.borrow().clone();

    let _ = searcher_tx.send(None);
    let _ = desc_tx.send(None);

    let old_client = oma_client_opt.take();
    drop(old_client);
    let force_reload_cache = new_cache!()?;
    drop(force_reload_cache);

    match OmaClient::new(client_ptr.clone(), vec![]) {
        Ok(new_apt) => {
            let new_map = update_pkg_description_cache(&new_apt.apt.cache);

            info!("Rebuilding local database index ...");
            match IndiciumSearch::new(&new_apt.apt.cache, SearchType::Live, |_| {}) {
                Ok(new_engine) => {
                    let _ = searcher_tx.send(Some(Arc::new(new_engine)));
                    let _ = desc_tx.send(Some(Arc::new(new_map)));
                    info!("Worker Thread: Search index and description cache swapped");
                }
                Err(e) => {
                    error!("Create new searcher failed: {e}");
                    let _ = searcher_tx.send(old_searcher);
                    let _ = desc_tx.send(old_desc);
                }
            }

            *oma_client_opt = Some(new_apt);

            Ok(())
        }
        Err(e) => {
            let _ = searcher_tx.send(old_searcher);
            let _ = desc_tx.send(old_desc);
            Err(e.context("Fatal environment reset failure"))
        }
    }
}

fn update_pkg_description_cache(cache: &Cache) -> HashMap<String, String> {
    let mut new_map = HashMap::new();

    for pkg in cache.packages(&Default::default()) {
        if let Some(cand) = pkg.candidate()
            && let Some(desc) = cand.summary()
        {
            new_map.insert(pkg.fullname(true), desc.to_string());
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

        let run_lock = self.run_lock.clone();
        let Ok(guard) = run_lock.try_lock_owned() else {
            return Err(zbus::fdo::Error::Failed(
                "Another task is already running!".to_string(),
            ));
        };

        let next_version = self.current_version.fetch_add(1, Ordering::SeqCst) + 1;

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
        let apt_task_tx = self.apt_task_tx.clone();
        let report_tx = self.current_report_tx.clone();

        tokio::task::spawn_blocking(move || {
            let _keep_lock_alive = guard;
            let (update_cache_tx, update_cache_rx) = oneshot::channel();

            let outcome = refresh_impl(tx.clone(), client);
            let _ = apt_task_tx.send(AptTask::UpdateCache {
                result_tx: update_cache_tx,
            });

            let apt_task_result = update_cache_rx.blocking_recv();
            let apt_task_result = match apt_task_result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(anyhow!("{e}")),
                Err(_) => Err(anyhow!("Unknown error or failed connection from worker!")),
            };

            let status = match outcome.and(apt_task_result) {
                Ok(_) => TaskStatus::Success,
                Err(e) => TaskStatus::Failed(e.to_string()),
            };

            let _ = report_tx.send(Some(ResultReport {
                version: next_version,
                status,
            }));
        });

        Ok(next_version)
    }

    async fn get_last_result(&self, expected_version: u64) -> zbus::fdo::Result<String> {
        let mut rx = self.current_report_rx.clone();

        loop {
            if let Some(ref report) = *rx.borrow()
                && (expected_version == 0 || report.version >= expected_version)
            {
                return Ok(serde_json::to_string(report).unwrap_or_else(|_| "null".to_string()));
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
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.apt_task_tx.send(AptTask::UpdateList { tx }) {
            error!(error = e.to_string(), "Failed to send task channel!");
        }

        match rx.await {
            Ok(Ok(op)) => {
                Ok(serde_json::to_string(&op)
                    .map_err(|e| zbus::fdo::Error::Failed(e.to_string()))?)
            }
            Ok(Err(err_msg)) => Err(zbus::fdo::Error::Failed(err_msg)),
            Err(_) => Err(zbus::fdo::Error::Failed(
                "Unknown error or failed connection from worker!".to_string(),
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

        let run_lock = self.run_lock.clone();
        let Ok(guard) = run_lock.try_lock_owned() else {
            return Err(zbus::fdo::Error::Failed(
                "Another task is already running!".to_string(),
            ));
        };
        let next_version = self.current_version.fetch_add(1, Ordering::SeqCst) + 1;

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
                "Internal worker thread died!".to_string(),
            ));
        }

        let report_tx = self.current_report_tx.clone();

        tokio::spawn(async move {
            let _keep_lock_alive = guard;

            let status = match result_rx.await {
                Ok(Ok(())) => TaskStatus::Success,
                Ok(Err(e)) => TaskStatus::Failed(e),
                Err(_) => TaskStatus::Failed("Worker exited with an error!".to_string()),
            };

            let _ = report_tx.send(Some(ResultReport {
                version: next_version,
                status,
            }));
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
            error!(error = e.to_string(), "Failed to send task channel!");
        }

        match result_rx.await {
            Ok(Ok(op)) => {
                Ok(serde_json::to_string(&op)
                    .map_err(|e| zbus::fdo::Error::Failed(e.to_string()))?)
            }
            Ok(Err(err_msg)) => Err(zbus::fdo::Error::Failed(err_msg)),
            Err(_) => Err(zbus::fdo::Error::Failed(
                "Unknown error or failed connection from worker!".to_string(),
            )),
        }
    }

    #[tracing::instrument(ret, skip(self))]
    async fn search(&self, query: String) -> zbus::fdo::Result<String> {
        let mut rx = self.searcher_rx.clone();

        let engine_snapshot = loop {
            if let Some(ref engine) = *rx.borrow() {
                break engine.clone();
            }

            if rx.changed().await.is_err() {
                return Err(zbus::fdo::Error::Failed(
                    "Internal watch channel closed".to_string(),
                ));
            }
        };

        match engine_snapshot.search(&query) {
            Ok(results) => Ok(serde_json::to_string(&results).unwrap_or_default()),
            Err(e) => Err(zbus::fdo::Error::Failed(e.to_string())),
        }
    }

    #[tracing::instrument(ret, skip(self))]
    async fn get_description(&self, pkg_name: String) -> zbus::fdo::Result<String> {
        let mut rx = self.desc_rx.clone();

        let map = loop {
            if let Some(ref snapshot) = *rx.borrow() {
                break snapshot.clone();
            }

            if rx.changed().await.is_err() {
                return Err(zbus::fdo::Error::Failed(
                    "Internal watch channel closed".to_string(),
                ));
            }
        };

        match map.get(&pkg_name) {
            Some(desc) => Ok(desc.clone()),
            None => Ok("No description available.".to_string()),
        }
    }

    #[zbus(signal)]
    async fn status(ctxt: &SignalEmitter<'_>, status: String) -> zbus::Result<()>;
}

pub async fn auth(header: zbus::message::Header<'_>, conn: &Connection) -> Result<(), fdo::Error> {
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
            "io.aosc.Amo.apply.run",
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
