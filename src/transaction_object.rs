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
use crate::transaction::{
    CancelError, EnqueueError, Task, TransactionManager, TransactionRole, TransactionStateEvent,
};
use crate::tum::updates_list_response;
use anyhow::anyhow;
use reqwest_middleware::ClientWithMiddleware;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::Arc,
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
    /// 创建者连接的 unique name：对象是 sender 锁定的，只有这条连接能
    /// 操作它；清扫器用它判定"已 claim 但从未入队"的对象是否被放弃
    /// （连接已死 ⇒ 操作永远无法继续 ⇒ 可回收）。
    pub(crate) sender: String,
    pub(crate) created_at: Instant,
    pub(crate) started: bool,
}

/// `begin` 的启动声明（lease）守卫：在 live 锁内标记 `started` 后持有它，
/// 保证任何未入队的退出路径都会回滚 `started`——授权失败、入队失败，乃至
/// 客户端在 polkit 弹窗期间断开导致 begin 的 future 被取消，都不会留下
/// "已启动但从未入队"的对象永久占用槽位（清扫器只回收 dormant 对象）。
struct StartedClaim {
    live: Arc<Mutex<HashMap<u64, LiveTransaction>>>,
    id: u64,
    armed: bool,
}

impl StartedClaim {
    fn new(live: Arc<Mutex<HashMap<u64, LiveTransaction>>>, id: u64) -> Self {
        Self { live, id, armed: true }
    }

    /// 已知失败路径上立即回滚（同步等待，不依赖 Drop 的异步时机）。
    async fn rollback(&mut self) {
        if self.armed {
            self.armed = false;
            self.live
                .lock()
                .await
                .get_mut(&self.id)
                .map(|t| t.started = false);
        }
    }

    /// 入队成功后调用：声明完成，Drop 不再回滚。
    fn commit(&mut self) {
        self.armed = false;
    }
}

impl Drop for StartedClaim {
    fn drop(&mut self) {
        if self.armed {
            // Drop 里不能 await，fire-and-forget 回滚。只在 begin 的
            // future 被取消（客户端断开等）时走到这里；运行时不可用
            // （如关停中）则跳过，进程退出时槽位自然释放。
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let live = self.live.clone();
                let id = self.id;
                handle.spawn(async move {
                    live.lock().await.get_mut(&id).map(|t| t.started = false);
                });
            }
        }
    }
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

/// 单流 `TransactionEvent` 信号的载荷：一个事务的全部事件（进度、状态、
/// 结果）都走这一个信号，服务端保证发射顺序（进度 → 结果 → 终态）。
/// 客户端只需订阅一个流，无需自行合并/排序。
#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TransactionEvent {
    /// 一条进度（原 Status 信号载荷）。
    Progress(serde_json::Value),
    /// 事务状态变更（原 TransactionState 信号载荷）。
    State(TransactionStateEvent),
    /// 事务结束报告（原 ResultReport 信号载荷）。
    Result(ResultReport),
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
    /// 注册表的 `started` 是唯一事实源，启动声明 / 销毁 / 清扫共用同一把锁。
    pub(crate) live: Arc<Mutex<HashMap<u64, LiveTransaction>>>,
}

impl TransactionObject {
    /// 发一条进度事件（单流 TransactionEvent 的 Progress 变体）。
    async fn emit_progress(ctxt: &SignalEmitter<'_>, payload: String) -> zbus::Result<()> {
        let event = TransactionEvent::Progress(serde_json::from_str(&payload).map_err(|e| {
            zbus::Error::Failure(format!("Invalid progress payload: {e}"))
        })?);
        let json = serde_json::to_string(&event)
            .map_err(|e| zbus::Error::Failure(format!("Serialize progress event: {e}")))?;
        TransactionObjectSignals::transaction_event(ctxt, json).await
    }

    /// 发一条结果事件（单流 TransactionEvent 的 Result 变体）。
    async fn emit_result(ctxt: &SignalEmitter<'_>, report: ResultReport) -> zbus::Result<()> {
        let event = TransactionEvent::Result(report);
        let json = serde_json::to_string(&event)
            .map_err(|e| zbus::Error::Failure(format!("Serialize result event: {e}")))?;
        TransactionObjectSignals::transaction_event(ctxt, json).await
    }

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
        auth_action: Option<&str>,
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
        // 启动声明（lease）与清扫器的过期判定共用同一把 live 锁（单一同步
        // 边界）：要么 begin 先声明 started（reaper 之后看到已启动而跳过），
        // 要么 reaper 先移除条目（begin 发现条目不存在而报错、不入队）——
        // 不会出现"操作已入队但 D-Bus 对象已被回收"。StartedClaim 守卫保证
        // 声明后任何未入队的退出路径都会回滚 started。
        let mut claim = {
            let mut live = self.live.lock().await;
            let Some(entry) = live.get_mut(&self.id) else {
                return Err(zbus::fdo::Error::UnknownObject(format!(
                    "Transaction {} no longer exists",
                    self.id
                )));
            };
            if entry.started {
                return Err(zbus::fdo::Error::Failed(format!(
                    "Transaction {} already started",
                    self.id
                )));
            }
            entry.started = true;
            StartedClaim::new(self.live.clone(), self.id)
        };

        // 需要授权的操作（refresh / apply_changes）在声明之后、入队之前
        // 等待 polkit：授权弹窗可能挂起超过 DORMANT_TIMEOUT，若对象仍是
        // dormant，reclaim_dormant 会把它连同 D-Bus 对象一起回收——用户
        // 授权后 begin 会报 UnknownObject，操作永远不入队。声明为 started
        // 后清扫器会跳过它；授权失败则立即回滚声明，对象回到 dormant
        // （可被 Destroy 或清扫器回收）。
        if let Some(action) = auth_action {
            if let Err(e) = auth(header, conn, action).await {
                claim.rollback().await;
                return Err(e);
            }
            // 授权可能耗时超过清扫周期：若期间对象被清扫器回收（创建者
            // 连接断开且事务未入队），不再继续入队——否则操作会在对象
            // 已消失的情况下照常执行。
            if !self.live.lock().await.contains_key(&self.id) {
                return Err(zbus::fdo::Error::UnknownObject(format!(
                    "Transaction {} no longer exists",
                    self.id
                )));
            }
        }

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
            // 入队失败（队列满/配额）：回滚声明，对象回到 dormant，
            // 可被 Destroy 或清扫器回收。
            claim.rollback().await;
            return Err(enqueue_error(e));
        }
        claim.commit();
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
        let id = self.id;
        let client = self.client.clone();
        let ctx = self.ctx.clone();
        let main_emitter = self.main_emitter.clone();
        self.begin(
            &header,
            conn,
            TransactionRole::Refresh,
            Some("io.aosc.amo.refresh"),
            ctxt,
            move |ctxt| {
            Box::pin(async move {
                let (progress_tx, progress_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
                let ctxt_status = ctxt.clone();
                // 保留转发任务的句柄：生产端关闭后先 await 它，把缓冲的
                // 进度全部发完再发 ResultReport，否则客户端收到报告即
                // 返回，会丢尾部进度。
                let forwarder = tokio::spawn(async move {
                    let mut progress_rx = progress_rx;
                    while let Some(status) = progress_rx.recv().await {
                        if let Err(e) = Self::emit_progress(&ctxt_status, status).await {
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
                if let Err(e) = Self::emit_result(&ctxt, report).await {
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
        let id = self.id;
        let client = self.client.clone();
        let ctx = self.ctx.clone();
        let main_emitter = self.main_emitter.clone();
        self.begin(
            &header,
            conn,
            TransactionRole::ApplyChanges,
            Some("io.aosc.Amo.apply.run"),
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
                            if let Err(e) = Self::emit_progress(&ctxt_status, event_str).await {
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
                    if let Err(e) = Self::emit_result(&ctxt, report).await {
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
            None,
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
                    if let Err(e) = Self::emit_result(&ctxt, report).await {
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
            None,
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
                    if let Err(e) = Self::emit_result(&ctxt, report).await {
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
        // 与 begin 的启动声明共用 live 锁：已启动（或已不存在）的对象
        // 不能销毁。先移除注册表条目，再移除 D-Bus 对象。
        {
            let mut live = self.live.lock().await;
            let Some(entry) = live.get_mut(&self.id) else {
                return Err(zbus::fdo::Error::UnknownObject(format!(
                    "Transaction {} no longer exists",
                    self.id
                )));
            };
            if entry.started {
                return Err(zbus::fdo::Error::Failed(format!(
                    "Transaction {} already started",
                    self.id
                )));
            }
            live.remove(&self.id);
        }
        let path = format!("/io/aosc/Amo/Transaction/{}", self.id);
        self.server
            .remove::<TransactionObject, _>(path.as_str())
            .await
            .map_err(|e| zbus::fdo::Error::Failed(format!("Failed to destroy transaction: {e}")))?;
        Ok(())
    }

    #[zbus(signal)]
    async fn transaction_event(ctxt: &SignalEmitter<'_>, event: String) -> zbus::Result<()>;
}

/// 周期清扫超时的事务对象，释放配额槽位。覆盖创建者断开或放弃对象
/// 而不显式销毁的情况。回收两类对象：
/// 1. 休眠（未启动）超过 DORMANT_TIMEOUT 的对象（原行为）；
/// 2. 已 claim（started）但从未入队、且创建者连接已死的对象——
///    授权弹窗被放弃、begin 的 future 在客户端断开后不会被 zbus 取消，
///    若不清扫会永久占用槽位（可被自动化 DoS：CreateTransaction→
///    ApplyChanges→断连×N）。对象是 sender 锁定的，创建者连接已死则
///    操作永远无法继续，回收安全；已入队/运行中的事务（在 manager 里）
///    即使创建者断开也要执行完，不回收。
pub(crate) async fn reclaim_dormant(
    live: Arc<Mutex<HashMap<u64, LiveTransaction>>>,
    server: ObjectServer,
    manager: Arc<TransactionManager>,
    conn: Connection,
) {
    let dbus = match zbus::fdo::DBusProxy::new(&conn).await {
        Ok(p) => p,
        Err(e) => {
            error!("Failed to create DBusProxy for reaper: {e}");
            return;
        }
    };
    loop {
        tokio::time::sleep(DORMANT_TIMEOUT / 2).await;
        let now = Instant::now();

        // Phase 1：锁内快照候选（避免在 live 锁内做异步判定）。
        let candidates: Vec<(u64, String, String, bool, Instant)> = {
            let map = live.lock().await;
            map.iter()
                .map(|(id, t)| {
                    (
                        *id,
                        t.path.clone(),
                        t.sender.clone(),
                        t.started,
                        t.created_at,
                    )
                })
                .collect()
        };

        // Phase 2：锁外异步判定。
        let mut dormant_stale: Vec<String> = Vec::new();
        let mut abandoned: Vec<String> = Vec::new();
        for (id, path, sender, started, created_at) in candidates {
            if !started {
                if now.duration_since(created_at) >= DORMANT_TIMEOUT {
                    dormant_stale.push(path);
                }
            } else if claim_abandoned(&manager, &dbus, id, &sender).await {
                abandoned.push(path);
            }
        }

        // Phase 3：锁内移除。dormant 候选重新确认（防止夹在 begin 声明
        // 之间被误回收——P12 竞态）；abandoned 候选的 sender 已死，unique
        // name 不复用，不可能再被 begin，直接移除。
        let stale: Vec<String> = {
            let mut map = live.lock().await;
            let mut removed = Vec::new();
            map.retain(|_, t| {
                let reclaim = (dormant_stale.contains(&t.path)
                    && !t.started
                    && now.duration_since(t.created_at) >= DORMANT_TIMEOUT)
                    || abandoned.contains(&t.path);
                if reclaim {
                    removed.push(t.path.clone());
                }
                !reclaim
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

/// 判定一个已 claim（started）但未入队的事务对象是否"被放弃"（可回收）：
/// 不在队列/running 且创建者连接已死。创建者连接是唯一能操作该对象的
/// 连接（sender 锁定），它死了操作永远无法继续，回收安全；连接活着时
/// 说明可能正在等 polkit 弹窗，绝不能回收。
async fn claim_abandoned(
    manager: &TransactionManager,
    dbus: &zbus::fdo::DBusProxy<'_>,
    id: u64,
    sender: &str,
) -> bool {
    if manager.contains(id).await {
        // 已入队/运行中的事务即使创建者断开也要执行完，不回收。
        return false;
    }
    // 创建者连接已死；BusName 解析失败时保守地不回收。
    match zbus::names::BusName::try_from(sender) {
        Ok(name) => !dbus.name_has_owner(name).await.unwrap_or(false),
        Err(_) => false,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn live_with(id: u64) -> Arc<Mutex<HashMap<u64, LiveTransaction>>> {
        let live = Arc::new(Mutex::new(HashMap::new()));
        live.try_lock().unwrap().insert(
            id,
            LiveTransaction {
                path: format!("/io/aosc/Amo/Transaction/{id}"),
                uid: 0,
                sender: ":1.999".into(),
                created_at: Instant::now(),
                started: true,
            },
        );
        live
    }

    #[tokio::test]
    async fn claim_rollback_clears_started() {
        let live = live_with(1);
        let mut claim = StartedClaim::new(live.clone(), 1);
        claim.rollback().await;
        assert!(!live.lock().await.get(&1).unwrap().started);
    }

    #[tokio::test]
    async fn claim_commit_keeps_started() {
        let live = live_with(1);
        let mut claim = StartedClaim::new(live.clone(), 1);
        claim.commit();
        drop(claim);
        // 给 fire-and-forget 一点时间，暴露"commit 后仍回滚"的误实现。
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(live.lock().await.get(&1).unwrap().started);
    }

    #[tokio::test]
    async fn claim_drop_without_commit_rolls_back() {
        let live = live_with(1);
        {
            let _claim = StartedClaim::new(live.clone(), 1);
            // 未 commit 直接 drop（模拟 begin future 被取消）→ 异步回滚。
        }
        // 等 fire-and-forget 回滚任务执行。
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!live.lock().await.get(&1).unwrap().started);
    }

    /// 清扫器对"已 claim 未入队"对象的判定：创建者连接活着（可能在等
    /// polkit 弹窗）绝不回收；连接已死才回收；已入队/运行中的事务即使
    /// 创建者断开也不回收。
    #[tokio::test]
    async fn abandoned_requires_dead_sender() {
        let mgr = crate::transaction::TransactionManager::with_limits(10, 10);
        let conn = Connection::session().await.expect("session bus");
        let dbus = zbus::fdo::DBusProxy::new(&conn).await.expect("dbus proxy");

        // 本测试进程的 unique name 活着：即使事务不在队列也不回收
        // （等价于 polkit 弹窗挂起时的状态）。
        let self_name = conn.unique_name().expect("unique name");
        assert!(
            !claim_abandoned(&mgr, &dbus, 1, self_name.as_str()).await,
            "live sender must not be reclaimed"
        );

        // 不存在的 unique name：创建者连接已死 → 可回收。
        assert!(
            claim_abandoned(&mgr, &dbus, 1, ":1.999999999").await,
            "dead sender must be reclaimed"
        );

        // 已入队的事务：即使 sender 已死也不回收（异步事务要执行完）。
        mgr.enqueue(
            None,
            42,
            crate::transaction::TransactionRole::Simulate,
            "tester".into(),
            0,
            Box::pin(async move {}),
            None,
        )
        .await
        .expect("enqueue");
        assert!(
            !claim_abandoned(&mgr, &dbus, 42, ":1.999999999").await,
            "queued transaction must not be reclaimed"
        );
    }
}
