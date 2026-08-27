//! 单个事务的 D-Bus 对象（PackageKit 风格）。
//!
//! 操作方法（Refresh/ApplyChanges/Simulate/UpdatesList/Cancel/Destroy）与
//! Status/ResultReport/TransactionState 信号都挂在该对象自己的路径上，
//! 信号天然按事务隔离。对象生命周期：CreateTransaction 创建休眠对象 →
//! 客户端订阅信号 → 调用操作方法开工 → 结束（完成/取消）后自动移除。
//! 休眠对象有每用户配额 + 超时清扫器 + 显式 Destroy 回收；所有操作
//! sender 锁定（只有创建那条连接能操作）。

use crate::auth::{auth, peer_identity};
use crate::oma::{OmaClient, refresh_impl};
use crate::refresh::{RefreshContext, refresh_if_stale};
use crate::transaction::{CancelError, EnqueueError, Task, TransactionManager, TransactionRole};
use crate::tum::updates_list_response;
use anyhow::anyhow;
use reqwest_middleware::ClientWithMiddleware;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use tracing::{error, info};
use zbus::{
    Connection, interface,
    object_server::{ObjectServer, SignalEmitter},
};

/// 同时存在的活动事务对象上限（含休眠中未启动的），防止 CreateTransaction
/// 被无限调用耗尽对象服务器内存。
pub(crate) const MAX_LIVE_TRANSACTIONS: usize = 64;
/// 单个 uid 同时拥有的活动事务对象上限（休眠对象不占队列名额，可被
/// 无限创建；每用户配额防止一个用户占满全局槽位）。
pub(crate) const MAX_LIVE_PER_UID: usize = 16;
/// 休眠（未启动）事务对象的回收超时：超过该时间未启动即被清扫器移除。
const DORMANT_TIMEOUT: Duration = Duration::from_secs(60);

/// 活动事务对象注册表条目。
pub(crate) struct LiveTransaction {
    pub(crate) path: String,
    pub(crate) uid: u32,
    pub(crate) created_at: Instant,
    pub(crate) started: bool,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ResultReport {
    pub transaction_id: u64,
    pub role: TransactionRole,
    pub status: TaskStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub enum TaskStatus {
    Success,
    Failed(String),
}

/// 单个事务的 D-Bus 对象（PackageKit 风格）：路径
/// `/io/aosc/Amo/Transaction/<id>`。
///
/// 操作方法（Refresh/ApplyChanges/Simulate/UpdatesList/Cancel）与
/// Status/ResultReport/TransactionState 信号都挂在该对象自己的路径上，
/// 信号天然按事务隔离——客户端无需按 transaction_id 过滤，也不存在
/// "先订阅后调用"的竞态（客户端先 CreateTransaction 拿路径、订阅信号，
/// 再调用操作方法开工）。
pub(crate) struct TransactionObject {
    pub(crate) manager: Arc<TransactionManager>,
    pub(crate) id: u64,
    /// 创建事务对象的连接（sender）唯一名：只有它能操作该对象
    /// （PackageKit 风格，所有方法先校验 sender）。
    pub(crate) sender: String,
    /// 创建者的 uid，记录到事务（GetTransactionList / 队列配额）。
    pub(crate) uid: u32,
    pub(crate) client: ClientWithMiddleware,
    pub(crate) ctx: RefreshContext,
    /// APT lists 目录（TUM 清单读取用）。
    pub(crate) lists_dir: String,
    /// 主接口（/io/aosc/Amo）的信号发射目标，供 UpdatesChanged 等主接口信号用。
    pub(crate) main_emitter: SignalEmitter<'static>,
    /// 动态对象服务器：事务结束时移除自身。
    pub(crate) server: ObjectServer,
    /// 活动事务对象注册表：启动时标记、结束（完成/取消）时移除。
    pub(crate) live: Arc<Mutex<HashMap<u64, LiveTransaction>>>,
    /// 一个事务对象只能启动一次操作。
    pub(crate) started: AtomicBool,
}

impl TransactionObject {
    /// 校验调用者并启动事务：只有创建该对象的连接（sender）能操作
    /// （PackageKit 风格，对象路径可预测、无 ACL，必须自己校验；即使
    /// root 也只能通过自己的连接操作）；一个事务只能启动一次。
    /// `build` 构造任务；任务结束后（或被取消跳过时）移除对象并释放
    /// 注册表槽位。
    async fn begin(
        &self,
        header: &zbus::message::Header<'_>,
        conn: &Connection,
        role: TransactionRole,
        ctxt: SignalEmitter<'_>,
        build: impl FnOnce(SignalEmitter<'static>) -> Task,
    ) -> zbus::fdo::Result<()> {
        let (caller, uid) = peer_identity(header, conn).await?;
        // 只允许创建者（sender）操作：其他连接不能借它提交任务
        // （配额/责任归属），root 也不例外。
        if caller != self.sender {
            return Err(zbus::fdo::Error::AccessDenied(
                "Not the owner of this transaction".to_string(),
            ));
        }
        if self.started.swap(true, Ordering::SeqCst) {
            return Err(zbus::fdo::Error::Failed(format!(
                "Transaction {} already started",
                self.id
            )));
        }
        // 注册表标记已启动：清扫器不再回收它。
        self.live
            .lock()
            .await
            .get_mut(&self.id)
            .map(|t| t.started = true);

        let id = self.id;
        let ctxt_owned = ctxt.to_owned();
        let path = format!("/io/aosc/Amo/Transaction/{id}");
        let server = self.server.clone();
        let live = self.live.clone();
        let task = build(ctxt_owned.clone());
        // 事务结束（完成或取消）后移除对象并释放注册表槽位，避免对象与
        // 槽位无限累积。由 runner 在统一出口触发。
        let on_done = Some(Arc::new(move || {
            let server = server.clone();
            let path = path.clone();
            let live = live.clone();
            tokio::spawn(async move {
                if let Err(e) = server.remove::<TransactionObject, _>(path.as_str()).await {
                    error!("Failed to remove transaction object {path}: {e}");
                }
                live.lock().await.remove(&id);
            });
        }) as Arc<dyn Fn() + Send + Sync>);

        if let Err(e) = self
            .manager
            .enqueue(ctxt_owned, self.id, role, caller, uid, task, on_done)
            .await
        {
            // 入队失败（队列满/配额）：回滚 started，对象回到 dormant，
            // 可被 Destroy 或清扫器回收。
            self.started.store(false, Ordering::SeqCst);
            self.live
                .lock()
                .await
                .get_mut(&self.id)
                .map(|t| t.started = false);
            return Err(enqueue_error(e));
        }
        Ok(())
    }
}

#[interface(name = "io.aosc.Amo.Transaction")]
impl TransactionObject {
    #[tracing::instrument(ret, skip(self, conn, ctxt), fields(transaction_id = self.id))]
    async fn refresh(
        &self,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
        #[zbus(signal_context)] ctxt: SignalEmitter<'_>,
    ) -> zbus::fdo::Result<()> {
        auth(&header, conn, "io.aosc.amo.refresh").await?;
        let id = self.id;
        let client = self.client.clone();
        let ctx = self.ctx.clone();
        let main_emitter = self.main_emitter.clone();
        self.begin(&header, conn, TransactionRole::Refresh, ctxt, move |ctxt| {
            Box::pin(async move {
                let (progress_tx, progress_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
                let ctxt_status = ctxt.clone();
                // 保留转发任务的句柄：生产端关闭后先 await 它，把缓冲的
                // 进度全部发完再发 ResultReport，否则客户端收到报告即
                // 返回，会丢尾部进度。
                let forwarder = tokio::spawn(async move {
                    let mut progress_rx = progress_rx;
                    while let Some(status) = progress_rx.recv().await {
                        if let Err(e) = ctxt_status.status(status).await {
                            error!(
                                error = e.to_string(),
                                "Failed to send refresh_status request!"
                            );
                        }
                    }
                });

                let outcome =
                    tokio::task::spawn_blocking(move || refresh_impl(progress_tx, client.clone()))
                        .await;

                let outcome = match outcome {
                    Ok(r) => r,
                    Err(e) => Err(anyhow!("Refresh task failed to join: {e}")),
                };

                // 生产端已关闭：等转发任务把缓冲的进度全部发完。
                if let Err(e) = forwarder.await {
                    error!("Refresh progress forwarder task failed: {e}");
                }

                // 等缓存刷新完成后再发 result_report，避免客户端收到完成
                // 信号时搜索索引还是旧的。
                let refresh_outcome = refresh_if_stale(main_emitter.clone(), ctx).await;

                let status = match (outcome, refresh_outcome) {
                    (Ok(_), Ok(())) => TaskStatus::Success,
                    (Err(e), _) => TaskStatus::Failed(e.to_string()),
                    (Ok(_), Err(e)) => TaskStatus::Failed(format!(
                        "Package operation succeeded but cache refresh failed: {e}"
                    )),
                };

                let report = ResultReport {
                    transaction_id: id,
                    role: TransactionRole::Refresh,
                    status,
                    result: None,
                };
                if let Ok(json) = serde_json::to_string(&report)
                    && let Err(e) = ctxt.result_report(json).await
                {
                    error!("Failed to emit refresh result signal: {e}");
                }
            })
        })
        .await
    }

    #[tracing::instrument(ret, skip(self, conn, ctxt), fields(transaction_id = self.id, install = ?install, remove = ?remove, upgrade = upgrade))]
    async fn apply_changes(
        &self,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
        #[zbus(signal_context)] ctxt: SignalEmitter<'_>,
        install: Vec<String>,
        remove: Vec<String>,
        upgrade: bool,
    ) -> zbus::fdo::Result<()> {
        auth(&header, conn, "io.aosc.Amo.apply.run").await?;
        let id = self.id;
        let client = self.client.clone();
        let ctx = self.ctx.clone();
        let main_emitter = self.main_emitter.clone();
        self.begin(
            &header,
            conn,
            TransactionRole::ApplyChanges,
            ctxt,
            move |ctxt| {
                Box::pin(async move {
                    let (progress_tx, progress_rx) =
                        tokio::sync::mpsc::unbounded_channel::<String>();
                    let ctxt_status = ctxt.clone();
                    // 保留转发任务的句柄：生产端关闭后先 await 它，把缓冲的
                    // 进度全部发完再发 ResultReport，否则客户端收到报告即
                    // 返回，会丢尾部进度。
                    let forwarder = tokio::spawn(async move {
                        let mut progress_rx = progress_rx;
                        while let Some(event_str) = progress_rx.recv().await {
                            if let Err(e) = ctxt_status.status(event_str).await {
                                error!("Failed to broadcast oma event signal: {}", e);
                            }
                        }
                    });

                    let result = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
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

                        info!("apply_changes: starting commit ...");
                        current_apt.commit(progress_tx, id)?;
                        info!("apply_changes: commit done");

                        Ok(())
                    })
                    .await;

                    let result = match result {
                        Ok(r) => r,
                        Err(e) => Err(anyhow!("Apply task failed to join: {e}")),
                    };

                    // 生产端已关闭：等转发任务把缓冲的进度全部发完。
                    if let Err(e) = forwarder.await {
                        error!("Apply progress forwarder task failed: {e}");
                    }

                    let refresh_outcome = refresh_if_stale(main_emitter.clone(), ctx).await;
                    info!("apply_changes: cache refresh done");

                    let status = match (result, refresh_outcome) {
                        (Ok(_), Ok(())) => TaskStatus::Success,
                        (Err(e), _) => TaskStatus::Failed(e.to_string()),
                        (Ok(_), Err(e)) => TaskStatus::Failed(format!(
                            "Package operation succeeded but cache refresh failed: {e}"
                        )),
                    };

                    let report = ResultReport {
                        transaction_id: id,
                        role: TransactionRole::ApplyChanges,
                        status,
                        result: None,
                    };
                    if let Ok(json) = serde_json::to_string(&report)
                        && let Err(e) = ctxt.result_report(json).await
                    {
                        error!("Failed to emit apply result signal: {e}");
                    }
                })
            },
        )
        .await
    }

    #[tracing::instrument(ret, skip(self, conn, ctxt), fields(transaction_id = self.id, install = ?install, remove = ?remove, upgrade = upgrade))]
    async fn simulate(
        &self,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
        #[zbus(signal_context)] ctxt: SignalEmitter<'_>,
        install: Vec<String>,
        remove: Vec<String>,
        upgrade: bool,
    ) -> zbus::fdo::Result<()> {
        let id = self.id;
        let client = self.client.clone();
        let lists_dir = self.lists_dir.clone();
        self.begin(
            &header,
            conn,
            TransactionRole::Simulate,
            ctxt,
            move |ctxt| {
                Box::pin(async move {
                    let outcome = tokio::task::spawn_blocking(move || {
                        let mut apt = OmaClient::new(client, vec![])?;
                        let operation = apt
                            .summary(install, remove, upgrade)
                            .map_err(|e| anyhow!("{e}"))?;
                        Ok::<_, anyhow::Error>(updates_list_response(&lists_dir, operation))
                    })
                    .await;

                    let (status, result) = match outcome {
                        Ok(Ok(op)) => match serde_json::to_value(&op) {
                            Ok(value) => (TaskStatus::Success, Some(value)),
                            Err(e) => (
                                TaskStatus::Failed(format!("Serialize simulate result: {e}")),
                                None,
                            ),
                        },
                        Ok(Err(e)) => (TaskStatus::Failed(e.to_string()), None),
                        Err(e) => (
                            TaskStatus::Failed(format!("Simulate task failed to join: {e}")),
                            None,
                        ),
                    };

                    let report = ResultReport {
                        transaction_id: id,
                        role: TransactionRole::Simulate,
                        status,
                        result,
                    };
                    if let Ok(json) = serde_json::to_string(&report)
                        && let Err(e) = ctxt.result_report(json).await
                    {
                        error!("Failed to emit simulate result signal: {e}");
                    }
                })
            },
        )
        .await
    }

    #[tracing::instrument(ret, skip(self, conn, ctxt), fields(transaction_id = self.id))]
    async fn updates_list(
        &self,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
        #[zbus(signal_context)] ctxt: SignalEmitter<'_>,
    ) -> zbus::fdo::Result<()> {
        let id = self.id;
        let client = self.client.clone();
        let lists_dir = self.lists_dir.clone();
        self.begin(
            &header,
            conn,
            TransactionRole::UpdatesList,
            ctxt,
            move |ctxt| {
                Box::pin(async move {
                    let outcome = tokio::task::spawn_blocking(move || {
                        let mut apt = OmaClient::new(client, vec![])?;
                        let operation = apt
                            .summary(vec![], vec![], true)
                            .map_err(|e| anyhow!("{e}"))?;
                        Ok::<_, anyhow::Error>(updates_list_response(&lists_dir, operation))
                    })
                    .await;

                    let (status, result) = match outcome {
                        Ok(Ok(summary)) => match serde_json::to_value(&summary) {
                            Ok(value) => (TaskStatus::Success, Some(value)),
                            Err(e) => (
                                TaskStatus::Failed(format!("Serialize updates list: {e}")),
                                None,
                            ),
                        },
                        Ok(Err(e)) => (TaskStatus::Failed(e.to_string()), None),
                        Err(e) => (
                            TaskStatus::Failed(format!("Updates list task failed to join: {e}")),
                            None,
                        ),
                    };

                    let report = ResultReport {
                        transaction_id: id,
                        role: TransactionRole::UpdatesList,
                        status,
                        result,
                    };
                    if let Ok(json) = serde_json::to_string(&report)
                        && let Err(e) = ctxt.result_report(json).await
                    {
                        error!("Failed to emit updates_list result signal: {e}");
                    }
                })
            },
        )
        .await
    }

    /// 取消排队中的本事务（PackageKit 风格，从事务对象上调）。
    #[tracing::instrument(ret, skip(self, conn), fields(transaction_id = self.id))]
    async fn cancel(
        &self,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
    ) -> zbus::fdo::Result<()> {
        let (caller, _uid) = peer_identity(&header, conn).await?;
        // 只允许创建者（sender）取消；manager.cancel 用创建者自己的 uid。
        if caller != self.sender {
            return Err(zbus::fdo::Error::AccessDenied(
                "Not the owner of this transaction".to_string(),
            ));
        }
        self.manager
            .cancel(self.id, self.uid)
            .await
            .map_err(|e| match e {
                CancelError::NotFound => {
                    zbus::fdo::Error::UnknownObject(format!("Transaction {} not found", self.id))
                }
                CancelError::NotOwner => {
                    zbus::fdo::Error::AccessDenied("Not the owner of this transaction".to_string())
                }
                CancelError::Running => {
                    zbus::fdo::Error::Failed(format!("Transaction {} is already running", self.id))
                }
                CancelError::AlreadyCancelled => zbus::fdo::Error::Failed(format!(
                    "Transaction {} is already cancelled",
                    self.id
                )),
            })
    }

    /// 显式销毁尚未启动（dormant）的事务对象，立即释放配额槽位。
    /// 已启动的对象请用 Cancel 或等其自然结束（结束后自动移除）。
    #[tracing::instrument(ret, skip(self, conn), fields(transaction_id = self.id))]
    async fn destroy(
        &self,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
    ) -> zbus::fdo::Result<()> {
        let (caller, _uid) = peer_identity(&header, conn).await?;
        // 只允许创建者（sender）销毁休眠对象。
        if caller != self.sender {
            return Err(zbus::fdo::Error::AccessDenied(
                "Not the owner of this transaction".to_string(),
            ));
        }
        if self.started.load(Ordering::SeqCst) {
            return Err(zbus::fdo::Error::Failed(format!(
                "Transaction {} already started",
                self.id
            )));
        }
        let path = format!("/io/aosc/Amo/Transaction/{}", self.id);
        self.server
            .remove::<TransactionObject, _>(path.as_str())
            .await
            .map_err(|e| zbus::fdo::Error::Failed(format!("Failed to destroy transaction: {e}")))?;
        self.live.lock().await.remove(&self.id);
        Ok(())
    }

    #[zbus(signal)]
    async fn status(ctxt: &SignalEmitter<'_>, status: String) -> zbus::Result<()>;

    #[zbus(signal)]
    async fn result_report(ctxt: &SignalEmitter<'_>, report: String) -> zbus::Result<()>;

    #[zbus(signal)]
    async fn transaction_state(ctxt: &SignalEmitter<'_>, state: String) -> zbus::Result<()>;
}

/// 周期清扫休眠（未启动）超时的事务对象，释放配额槽位。覆盖创建者
/// 断开或放弃对象而不显式销毁的情况。
pub(crate) async fn reclaim_dormant(
    live: Arc<Mutex<HashMap<u64, LiveTransaction>>>,
    server: ObjectServer,
) {
    loop {
        tokio::time::sleep(DORMANT_TIMEOUT / 2).await;
        let now = Instant::now();
        let stale: Vec<String> = {
            let mut map = live.lock().await;
            let mut removed = Vec::new();
            map.retain(|_, t| {
                if !t.started && now.duration_since(t.created_at) >= DORMANT_TIMEOUT {
                    removed.push(t.path.clone());
                    false
                } else {
                    true
                }
            });
            removed
        };
        for path in stale {
            if let Err(e) = server.remove::<TransactionObject, _>(path.as_str()).await {
                error!("Failed to reclaim dormant transaction object {path}: {e}");
            }
        }
    }
}

/// 把入队拒绝映射为 D-Bus 错误：队列满 / 超配额 → `LimitsExceeded`。
fn enqueue_error(e: EnqueueError) -> zbus::fdo::Error {
    match e {
        EnqueueError::QueueFull => zbus::fdo::Error::LimitsExceeded(
            "Transaction queue is full, try again later".to_string(),
        ),
        EnqueueError::QuotaExceeded => zbus::fdo::Error::LimitsExceeded(
            "Too many queued transactions from this user".to_string(),
        ),
    }
}
