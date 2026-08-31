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
use crate::transaction::TransactionManager;
use crate::transaction::limits::check_arg_size;
use crate::transaction::live::{
    CLAIM_TIMEOUT, ClaimRollback, LiveTransaction, StartedClaim, check_claim_still_active,
    next_cancellation_id, next_claim_generation, remove_for_destroy,
    rollback_claim_if_not_enqueued,
};
use crate::transaction::types::{
    CancelError, EnqueueError, ResultReport, Task, TaskStatus, TransactionEvent, TransactionRole,
};
use crate::tum::updates_list_response;
use anyhow::anyhow;
use reqwest_middleware::ClientWithMiddleware;
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
        let event = TransactionEvent::Progress {
            payload: serde_json::from_str(&payload)
                .map_err(|e| zbus::Error::Failure(format!("Invalid progress payload: {e}")))?,
        };
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

    /// 运行带进度转发 + 事后刷新搜索索引的阻塞任务（refresh / apply_changes
    /// 共享）：`spawn_blocking` 执行任务 → 排空进度转发器 → 刷新索引 →
    /// 组合状态 → 发结果事件。任务经 `UnboundedSender<String>` 上报进度。
    async fn run_progress_and_refresh(
        ctxt: &SignalEmitter<'static>,
        id: u64,
        role: TransactionRole,
        task: impl FnOnce(tokio::sync::mpsc::UnboundedSender<String>) -> anyhow::Result<()>
        + Send
        + 'static,
        ctx: RefreshContext,
        main_emitter: SignalEmitter<'static>,
    ) {
        let (progress_tx, progress_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let ctxt_status = ctxt.clone();
        // 保留转发任务的句柄：生产端关闭后先 await 它，把缓冲的进度全部
        // 发完再发 ResultReport，否则客户端收到报告即返回，会丢尾部进度。
        let forwarder = tokio::spawn(async move {
            let mut progress_rx = progress_rx;
            while let Some(status) = progress_rx.recv().await {
                if let Err(e) = Self::emit_progress(&ctxt_status, status).await {
                    error!(error = e.to_string(), "Failed to forward progress event");
                }
            }
        });

        let outcome = tokio::task::spawn_blocking(move || task(progress_tx)).await;
        let outcome = match outcome {
            Ok(r) => r,
            Err(e) => Err(anyhow!("Task failed to join: {e}")),
        };

        // 生产端已关闭：等转发任务把缓冲的进度全部发完。
        if let Err(e) = forwarder.await {
            error!("Progress forwarder task failed: {e}");
        }

        // 等缓存刷新完成后再发 result_report，避免客户端收到完成信号时
        // 搜索索引还是旧的。
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
            role,
            status,
            result: None,
        };
        if let Err(e) = Self::emit_result(ctxt, report).await {
            error!("Failed to emit {role:?} result signal: {e}");
        }
    }

    /// 运行返回 JSON 结果的阻塞任务（simulate / updates_list 共享）：
    /// `spawn_blocking` 执行 `apt.summary` + TUM 匹配 → 序列化 → 发结果事件。
    #[allow(clippy::too_many_arguments)]
    async fn run_summary(
        ctxt: &SignalEmitter<'static>,
        id: u64,
        role: TransactionRole,
        install: Vec<String>,
        remove: Vec<String>,
        upgrade: bool,
        client: ClientWithMiddleware,
        lists_dir: String,
    ) {
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
                    TaskStatus::Failed(format!("Serialize {role:?} result: {e}")),
                    None,
                ),
            },
            Ok(Err(e)) => (TaskStatus::Failed(e.to_string()), None),
            Err(e) => (
                TaskStatus::Failed(format!("{role:?} task failed to join: {e}")),
                None,
            ),
        };

        let report = ResultReport {
            transaction_id: id,
            role,
            status,
            result,
        };
        if let Err(e) = Self::emit_result(ctxt, report).await {
            error!("Failed to emit {role:?} result signal: {e}");
        }
    }

    /// 等待授权结果，但施加超时上限：超过 `timeout` 仍未响应即放弃
    /// （返回 `TimedOut`）。清扫器到期只会回收注册表条目和 D-Bus 对象，
    /// 不会终止阻塞在 `auth().await` 里的方法 future——若授权等待无上限，
    /// 调用方可每轮超时后重试，无限累积 in-flight 服务端任务与 PolicyKit
    /// 请求。drop 未完成的 auth future 会终止对 PolicyKit 的等待。
    pub(crate) async fn await_auth(
        id: u64,
        timeout: Duration,
        auth_fut: impl std::future::Future<Output = Result<(), zbus::fdo::Error>>,
    ) -> Result<(), zbus::fdo::Error> {
        tokio::time::timeout(timeout, auth_fut).await.map_err(|_| {
            zbus::fdo::Error::TimedOut(format!("Authorization for transaction {id} timed out"))
        })?
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
        // 每次调用（无论是否需授权）都生成唯一 claim 代际：Cancel 回滚后
        // 立即重试时，新 claim 写入新代际，旧调用的复查/回滚比对代际即可
        // 区分——Simulate/UpdatesList 无 polkit cancellation_id，若用
        // cancellation_id 当代际，None 会让新旧调用无法区分（旧调用可把
        // 已取消的操作入队，或回滚清掉新 claim）。需要授权的操作另生成
        // 唯一 polkit cancellation id，claim 时一并存入注册表：声明被清扫
        // 器判定 abandoned（创建者断连/超时）时用它取消远程 PolicyKit
        // 检查——begin 本地超时只覆盖 TimedOut，创建者断连时 begin 的
        // future 不会被 zbus 取消，只有清扫器能回收并取消。
        let claim_generation = next_claim_generation(self.id);
        let cancellation_id = auth_action.map(|_| next_cancellation_id(self.id));
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
            entry.claimed_at = Some(Instant::now());
            entry.claim_generation = claim_generation.clone();
            entry.cancellation_id = cancellation_id.clone();
            // 已 claim：休眠计时暂停（清扫器改按 CLAIM_TIMEOUT 判定）。
            entry.dormant_since = None;
            StartedClaim::new(self.live.clone(), self.id, claim_generation.clone())
        };

        // 需要授权的操作（refresh / apply_changes）在声明之后、入队之前
        // 等待 polkit：授权弹窗可能挂起超过 DORMANT_TIMEOUT，若对象仍是
        // dormant，reclaim_dormant 会把它连同 D-Bus 对象一起回收——用户
        // 授权后 begin 会报 UnknownObject，操作永远不入队。声明为 started
        // 后清扫器会跳过它（但受 CLAIM_TIMEOUT 上限约束）；授权失败则
        // 立即回滚声明，对象回到 dormant（可被 Destroy 或清扫器回收）。
        if let Some(action) = auth_action {
            let cancellation_id = cancellation_id
                .as_deref()
                .expect("cancellation id for auth");
            // 授权等待加 CLAIM_TIMEOUT 上限：授权挂起超过该时限即放弃并
            // 回滚声明（调用方收到 TimedOut 后可重试），与清扫器回收 claim
            // 同步——否则调用方可每 5 分钟重试一次，无限累积 in-flight
            // 服务端任务与 PolicyKit 请求。
            if let Err(e) = Self::await_auth(
                self.id,
                CLAIM_TIMEOUT,
                auth(header, conn, action, cancellation_id),
            )
            .await
            {
                // 超时只 drop 本地 future 只会放弃回复，不会向 polkit 发送
                // 取消（CheckAuthorization 的 cancellation_id 必须非空且
                // 唯一）。超时在此显式取消（保底）；清扫器也会在 abandoned
                // 回收时取消，重复取消无害。
                if matches!(e, zbus::fdo::Error::TimedOut(_)) {
                    crate::auth::cancel_authorization(conn, cancellation_id).await;
                }
                claim.rollback().await;
                return Err(e);
            }
            // 授权期间对象可能被清扫器回收（创建者断开或 claim 超时）；
            // 对象是否仍存在、事务是否入队，在下面与入队同一把 live 锁内
            // 复查（与清扫器互斥），这里不再单独检查。
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

        // 授权成功后，在 live 锁内复查对象仍在并完成入队：与清扫器的移除
        // 判定共用同一把锁，二者互斥（锁序 live→queue，无人 queue→live，
        // 无死锁）——要么清扫器先移除（这里 sees 条目缺失，中止入队），
        // 要么这里先入队（清扫器持锁复查 manager 时 contains 命中，跳过
        // 移除）。不存在"入队成功后条目/对象被清扫器移除"或"清扫器判定
        // 可回收后 begin 仍入队"的窗口（否则已入队/运行中的事务对象消失，
        // 队列中的包操作无法取消）。
        let enqueue_result = {
            let mut live = self.live.lock().await;
            // 授权等待期间对象可能被并发 Cancel 回滚（started=false）、
            // Destroy 移除（条目缺失）、或 Cancel 后立即重试（claim 代际
            // 被新 claim 替换）：中止入队，不执行已取消的操作。
            check_claim_still_active(&live, self.id, &claim_generation)?;
            let result = self
                .manager
                .enqueue(ctxt_owned, self.id, role, caller, uid, task, on_done)
                .await;
            // 入队成功：在 live 锁内标记 enqueued，与 manager 入队原子可见
            // （同一临界区）。Cancel/Destroy/清扫器据此区分"已入队/运行中/
            // 收尾中"与"仅 claim 未入队"——runner 清空 running 槽到 on_done
            // 移除条目之间 manager.contains 短暂 false，enqueued 保持 true
            // 直到条目移除，杜绝把已执行的事务误判为未入队 claim（取消报
            // 成功但操作已提交，或销毁移除未发终态信号的对象）。
            if result.is_ok()
                && let Some(entry) = live.get_mut(&self.id)
            {
                entry.enqueued = true;
            }
            result
        };
        match enqueue_result {
            Ok(_) => claim.commit(),
            Err(e) => {
                // 入队失败（队列满/配额）：回滚声明，对象回到 dormant，
                // 可被 Destroy 或清扫器回收。（此处已释放 live 锁，
                // claim.rollback 再锁一次不会死锁。）
                claim.rollback().await;
                return Err(enqueue_error(e));
            }
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
                    Self::run_progress_and_refresh(
                        &ctxt,
                        id,
                        TransactionRole::Refresh,
                        move |progress_tx| refresh_impl(progress_tx, client.clone()),
                        ctx,
                        main_emitter,
                    )
                    .await;
                })
            },
        )
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
        // 入队前校验参数总量：未授权调用者可用接近系统总线消息上限的
        // 字符串耗尽内存（队列上限只数条目、不数字节）。
        check_arg_size(&install, &remove)?;
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
                    Self::run_progress_and_refresh(
                        &ctxt,
                        id,
                        TransactionRole::ApplyChanges,
                        move |progress_tx| {
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
                        },
                        ctx,
                        main_emitter,
                    )
                    .await;
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
        // 入队前校验参数总量（同 apply_changes）。
        check_arg_size(&install, &remove)?;
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
                    Self::run_summary(
                        &ctxt,
                        id,
                        TransactionRole::Simulate,
                        install,
                        remove,
                        upgrade,
                        client,
                        lists_dir,
                    )
                    .await;
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
                    Self::run_summary(
                        &ctxt,
                        id,
                        TransactionRole::UpdatesList,
                        vec![],
                        vec![],
                        true,
                        client,
                        lists_dir,
                    )
                    .await;
                })
            },
        )
        .await
    }

    /// 取消排队中的本事务（PackageKit 风格，从事务对象上调）。授权等待中
    /// （已 claim 但未入队）的事务同样可取消：取消远程 polkit 检查并回滚
    /// claim，对象回到 dormant（可重试或 Destroy）。
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
        // 授权等待中（claimed-but-not-enqueued）：判定与回滚在 live 锁内
        // 完成（锁序 live→queue，与清扫器 phase 3 一致），cancellation_id
        // 带出锁外取消远程检查。已入队/运行中的走 manager.cancel。
        let outcome = {
            let mut live = self.live.lock().await;
            rollback_claim_if_not_enqueued(&mut live, &self.manager, self.id).await
        };
        if let Some(ClaimRollback {
            rolled_back: true,
            cancellation_id,
        }) = outcome
        {
            // 回滚成功即返回成功：Simulate/UpdatesList 无 polkit
            // cancellation_id（None），但 claim 已被清除、任务已被阻止——
            // 不能落入 manager.cancel（事务从未入队，会报 UnknownObject，
            // 尽管取消实际生效）。有 cancellation_id 的（Refresh/
            // ApplyChanges）带出锁外取消远程检查。
            if let Some(cid) = cancellation_id {
                crate::auth::cancel_authorization(conn, &cid).await;
            }
            return Ok(());
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
    /// 授权等待中（已 claim 但未入队）的对象也可销毁：取消远程 polkit
    /// 检查后移除。已入队/运行中的对象请用 Cancel 或等其自然结束
    /// （结束后自动移除）。
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
        // 与 begin 的启动声明共用 live 锁：dormant 与授权等待中
        // （claimed-but-not-enqueued）的对象可销毁（后者带出
        // cancellation_id 锁外取消远程检查）；已入队/运行中的不能销毁。
        // 先移除注册表条目，再移除 D-Bus 对象。
        let cancel_id = {
            let mut live = self.live.lock().await;
            remove_for_destroy(&mut live, self.id).await?
        };
        if let Some(cid) = cancel_id {
            crate::auth::cancel_authorization(conn, &cid).await;
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
