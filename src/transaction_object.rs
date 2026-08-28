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
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
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
/// 已 claim（started）但未入队（授权等待中）对象的回收超时：超过该时间
/// 仍未入队即被清扫器移除。比休眠超时长得多，给真实授权弹窗留足时间；
/// 但必须有界——否则未授权调用者可对全部 16 个对象并发 ApplyChanges 并
/// 保持连接，让 claim 绕过休眠与 abandoned 回收、长期占住槽位（同 uid
/// 其他应用全被 LimitsExceeded，多个 uid 可耗尽全局 64 槽）。
const CLAIM_TIMEOUT: Duration = Duration::from_secs(300);
/// PolicyKit `CheckAuthorization` 的 cancellation_id 计数器：每个授权检查
/// 用唯一 ID（空 ID 的远程检查不可取消），超时后才能通过
/// `CancelCheckAuthorization` 显式取消远程检查。
static NEXT_CANCEL_ID: AtomicU64 = AtomicU64::new(0);

/// 生成唯一且非空的 PolicyKit cancellation_id。计数器单调递增保证进程内
/// 唯一，带事务 id 便于排查。
fn next_cancellation_id(tx_id: u64) -> String {
    format!(
        "amo-{tx_id}-{}",
        NEXT_CANCEL_ID.fetch_add(1, Ordering::Relaxed)
    )
}

/// 单个事务参数（install + remove 全部字符串）允许的最大总字节数。
/// 未授权调用者可提交接近系统总线消息上限（~128MB）的字符串，且这些
/// 向量被 boxed future 无限制捕获——队列上限只数条目（每 uid 8 + 运行中
/// 1），不数字节，可让守护进程保留近 1GB。入队前必须校验聚合大小。
const MAX_TRANSACTION_ARG_BYTES: usize = 16 * 1024 * 1024; // 16 MiB
/// 单个事务参数（install + remove）允许的最大元素数。字节上限只统计字符串
/// 内容——空串/极短串贡献 0 字节，但每个反序列化的 `String` 都占 24 字节
/// 头 + Vec 容量，数百万空串可绕过字节上限（系统总线消息 ~128MB 可装
/// 上千万空串，内存数百 MB/请求）。元素数必须单独有界。
const MAX_TRANSACTION_ARG_ITEMS: usize = 65_536;

/// 校验事务参数（install + remove）：聚合字节数与元素数任一超限都拒绝
/// （LimitsExceeded），在构造任务/入队之前调用。字节上限防大字符串，
/// 元素上限防空串/极短串的海量条目。
fn check_arg_size(install: &[String], remove: &[String]) -> Result<(), zbus::fdo::Error> {
    let items = install.len().saturating_add(remove.len());
    if items > MAX_TRANSACTION_ARG_ITEMS {
        return Err(zbus::fdo::Error::LimitsExceeded(format!(
            "Too many transaction arguments ({items}, limit {MAX_TRANSACTION_ARG_ITEMS})"
        )));
    }
    let total: usize = install.iter().chain(remove.iter()).map(|s| s.len()).sum();
    if total > MAX_TRANSACTION_ARG_BYTES {
        return Err(zbus::fdo::Error::LimitsExceeded(format!(
            "Transaction arguments too large ({total} bytes, limit {MAX_TRANSACTION_ARG_BYTES})"
        )));
    }
    Ok(())
}

/// 活动事务对象注册表条目。
pub(crate) struct LiveTransaction {
    pub(crate) path: String,
    pub(crate) uid: u32,
    /// 创建者连接的 unique name：对象是 sender 锁定的，只有这条连接能
    /// 操作它；清扫器用它判定"已 claim 但从未入队"的对象是否被放弃
    /// （连接已死 ⇒ 操作永远无法继续 ⇒ 可回收）。
    pub(crate) sender: String,
    pub(crate) created_at: Instant,
    /// 对象处于休眠（未启动）状态的起始时刻：`Some(t)` = 自 t 起休眠，
    /// `None` = 已 claim（started，休眠计时不运行）。创建时与每次回滚
    /// （授权失败/入队失败/Cancel）都重置为当前时刻——否则对象存在超过
    /// DORMANT_TIMEOUT 后回滚会让下一个清扫周期立即回收，回滚承诺的
    /// 重试窗口只有一个清扫间隔。
    pub(crate) dormant_since: Option<Instant>,
    /// 声明（begin 标记 started）的时刻：`None` = 休眠（未声明）。
    /// 清扫器对"已声明但从未入队"的对象同时施加 CLAIM_TIMEOUT 上限，
    /// 防止授权等待中的 claim 绕过休眠与 abandoned 回收长期占槽。
    pub(crate) claimed_at: Option<Instant>,
    /// 本次 claim 对应的 PolicyKit `CheckAuthorization` cancellation_id
    /// （仅需要授权的操作）：声明被清扫器判定 abandoned（创建者断连或
    /// claim 超时）时用它显式取消远程检查——begin 本地超时只覆盖
    /// TimedOut，创建者断连时 begin 的 future 不会被 zbus 取消，只有
    /// 清扫器能回收并取消远程检查。
    pub(crate) cancellation_id: Option<String>,
    pub(crate) started: bool,
}

/// `begin` 的启动声明（lease）守卫：在 live 锁内标记 `started` 后持有它，
/// 保证任何未入队的退出路径都会回滚 `started`——授权失败、入队失败，乃至
/// 客户端在 polkit 弹窗期间断开导致 begin 的 future 被取消，都不会留下
/// "已启动但从未入队"的对象永久占用槽位（清扫器只回收 dormant 对象）。
struct StartedClaim {
    live: Arc<Mutex<HashMap<u64, LiveTransaction>>>,
    id: u64,
    /// 本代 claim 写入条目的 cancellation_id。回滚时只清"自己这一代"的
    /// claim：Cancel 回滚后用户可立即 re-trigger（新 claim、新
    /// cancellation_id），旧 begin 的授权 future 最终失败时若无条件回滚
    /// 会把新 claim 一起清掉（新事务被旧 future 误杀）。比对 cancellation_id
    /// 保证旧 future 的回滚只作用于自己声明的那一代。
    cancellation_id: Option<String>,
    armed: bool,
}

impl StartedClaim {
    fn new(
        live: Arc<Mutex<HashMap<u64, LiveTransaction>>>,
        id: u64,
        cancellation_id: Option<String>,
    ) -> Self {
        Self {
            live,
            id,
            cancellation_id,
            armed: true,
        }
    }

    /// 已知失败路径上立即回滚（同步等待，不依赖 Drop 的异步时机）。
    async fn rollback(&mut self) {
        if self.armed {
            self.armed = false;
            if let Some(t) = self.live.lock().await.get_mut(&self.id) {
                if t.cancellation_id == self.cancellation_id {
                    t.started = false;
                    t.claimed_at = None;
                    t.cancellation_id = None;
                    // 回到休眠：休眠计时从回滚时刻重新起算，否则对象存在
                    // 超过 DORMANT_TIMEOUT 后回滚会被下一个清扫周期立即
                    // 回收，重试窗口只有一个清扫间隔。
                    t.dormant_since = Some(Instant::now());
                }
            }
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
                let cancellation_id = self.cancellation_id.clone();
                handle.spawn(async move {
                    if let Some(t) = live.lock().await.get_mut(&id) {
                        if t.cancellation_id == cancellation_id {
                            t.started = false;
                            t.claimed_at = None;
                            t.cancellation_id = None;
                            t.dormant_since = Some(Instant::now());
                        }
                    }
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
    /// 一条进度（原 Status 信号载荷）。必须是带 `payload` 字段的 struct
    /// 变体：`oma_refresh::db::Event` 的单元变体（ScanningTopic /
    /// RunInvokeScript / Done）序列化为 JSON 标量（如 `"Done"`），内部
    /// 标签（tag="type"）的 newtype 变体要求载荷是 map 才能注入 tag，
    /// 标量会序列化失败、事件被转发器丢弃。struct 变体把载荷放进
    /// 相邻的 `payload` 字段，任意 `serde_json::Value` 都能承载。
    Progress {
        /// 进度载荷（任意 JSON：oma 事件、下载进度、dpkg 进度等）。
        payload: serde_json::Value,
    },
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

    /// 等待授权结果，但施加超时上限：超过 `timeout` 仍未响应即放弃
    /// （返回 `TimedOut`）。清扫器到期只会回收注册表条目和 D-Bus 对象，
    /// 不会终止阻塞在 `auth().await` 里的方法 future——若授权等待无上限，
    /// 调用方可每轮超时后重试，无限累积 in-flight 服务端任务与 PolicyKit
    /// 请求。drop 未完成的 auth future 会终止对 PolicyKit 的等待。
    async fn await_auth(
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
        // 需要授权的操作先生成唯一 cancellation id，claim 时一并存入注册
        // 表：声明被清扫器判定 abandoned（创建者断连/超时）时用它取消远程
        // PolicyKit 检查——begin 本地超时只覆盖 TimedOut，创建者断连时
        // begin 的 future 不会被 zbus 取消，只有清扫器能回收并取消。
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
            entry.cancellation_id = cancellation_id.clone();
            // 已 claim：休眠计时暂停（清扫器改按 CLAIM_TIMEOUT 判定）。
            entry.dormant_since = None;
            StartedClaim::new(self.live.clone(), self.id, cancellation_id.clone())
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
            let live = self.live.lock().await;
            // 授权等待期间对象可能被并发 Cancel 回滚（started=false）或
            // Destroy 移除（条目缺失）：中止入队，不执行已取消的操作。
            check_claim_still_active(&live, self.id)?;
            self.manager
                .enqueue(ctxt_owned, self.id, role, caller, uid, task, on_done)
                .await
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
                    let (progress_tx, progress_rx) =
                        tokio::sync::mpsc::unbounded_channel::<String>();
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

                    let outcome = tokio::task::spawn_blocking(move || {
                        refresh_impl(progress_tx, client.clone())
                    })
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
        let cancel_id = {
            let mut live = self.live.lock().await;
            rollback_claim_if_not_enqueued(&mut live, &self.manager, self.id).await
        };
        if let Some(cid) = cancel_id {
            crate::auth::cancel_authorization(conn, &cid).await;
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
            remove_for_destroy(&mut live, &self.manager, self.id).await?
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

/// begin 授权成功后、入队前的复查（live 锁内调用）：条目必须仍存在且
/// started——授权等待期间被并发 Cancel 回滚（started=false）或 Destroy
/// 移除（条目缺失）时中止入队，不执行已取消的操作。
fn check_claim_still_active(
    live: &HashMap<u64, LiveTransaction>,
    id: u64,
) -> Result<(), zbus::fdo::Error> {
    let Some(t) = live.get(&id) else {
        return Err(zbus::fdo::Error::UnknownObject(format!(
            "Transaction {id} no longer exists"
        )));
    };
    if !t.started {
        return Err(zbus::fdo::Error::Failed(format!(
            "Transaction {id} was cancelled while awaiting authorization"
        )));
    }
    Ok(())
}

/// 在 live 锁内判定"已 claim 但未入队"（授权等待中）并回滚 claim：清
/// started/claimed_at/cancellation_id，返回带出的 cancellation_id（调用方
/// 锁外 `cancel_authorization`）。已入队/运行中（manager 里有）或条目
/// 不存在返回 None——前者走 manager.cancel，后者报 UnknownObject。
/// 锁序 live→queue（与清扫器 phase 3 的 claim_still_abandoned 一致）。
async fn rollback_claim_if_not_enqueued(
    live: &mut HashMap<u64, LiveTransaction>,
    manager: &TransactionManager,
    id: u64,
) -> Option<String> {
    let t = live.get_mut(&id)?;
    if t.started && !manager.contains(id).await {
        let cid = t.cancellation_id.take();
        t.started = false;
        t.claimed_at = None;
        // 回到休眠：休眠计时从回滚时刻重新起算（同 StartedClaim::rollback）。
        t.dormant_since = Some(Instant::now());
        cid
    } else {
        None
    }
}

/// 在 live 锁内判定 destroy 是否可行并移除条目：dormant（未启动）与
/// 授权等待中（claimed-but-not-enqueued）可移除，后者带出 cancellation_id
/// 供调用方锁外取消远程检查；已入队/运行中拒绝（Failed）；条目不存在报
/// UnknownObject。锁序 live→queue。
async fn remove_for_destroy(
    live: &mut HashMap<u64, LiveTransaction>,
    manager: &TransactionManager,
    id: u64,
) -> Result<Option<String>, zbus::fdo::Error> {
    let Some(t) = live.get_mut(&id) else {
        return Err(zbus::fdo::Error::UnknownObject(format!(
            "Transaction {id} no longer exists"
        )));
    };
    if t.started && manager.contains(id).await {
        return Err(zbus::fdo::Error::Failed(format!(
            "Transaction {id} already started"
        )));
    }
    let cid = t.cancellation_id.take();
    live.remove(&id);
    Ok(cid)
}

/// 休眠对象是否已超过 DORMANT_TIMEOUT：以 `dormant_since`（创建或最近
/// 一次回滚的时刻）为基准，未记录时退回 `created_at`。回滚会重置
/// dormant_since，保证"授权/入队失败后回到休眠"的对象获得完整的重试
/// 窗口，而不是在下一个清扫周期被立即回收。
fn dormant_expired(dormant_since: Option<Instant>, created_at: Instant, now: Instant) -> bool {
    now.duration_since(dormant_since.unwrap_or(created_at)) >= DORMANT_TIMEOUT
}

/// 周期清扫超时的事务对象，释放配额槽位。覆盖创建者断开或放弃对象
/// 而不显式销毁的情况。回收两类对象：
/// 1. 休眠（未启动）超过 DORMANT_TIMEOUT 的对象（原行为）；
/// 2. 已 claim（started）但从未入队、且满足回收条件的对象：
///    - 创建者连接已死（sender 锁定，操作永远无法继续）；
///    - 或 claim 超过 CLAIM_TIMEOUT 仍未入队（授权被放弃——即使创建者
///      还连着也不能让 claim 绕过休眠超时长期占槽：否则未授权调用者可
///      对全部 16 个对象并发 ApplyChanges 并保持连接，同 uid 其他应用
///      全被 LimitsExceeded，多个 uid 可耗尽全局 64 槽）。
///    已入队/运行中的事务（在 manager 里）即使创建者断开也要执行完，
///    不回收。
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
        // 元组：(id, path, sender, started, created_at, claimed_at, dormant_since)。
        type Candidate = (u64, String, String, bool, Instant, Option<Instant>, Option<Instant>);
        let candidates: Vec<Candidate> = {
            let map = live.lock().await;
            map.iter()
                .map(|(id, t)| {
                    (
                        *id,
                        t.path.clone(),
                        t.sender.clone(),
                        t.started,
                        t.created_at,
                        t.claimed_at,
                        t.dormant_since,
                    )
                })
                .collect()
        };

        // Phase 2：锁外异步判定。
        let mut dormant_stale: Vec<String> = Vec::new();
        // (path, 快照时的 claimed_at)：phase 3 用快照值做生成校验，防止
        // 误删"回滚后重新 claim"的新声明。
        let mut abandoned: Vec<(String, Option<Instant>)> = Vec::new();
        for (id, path, sender, started, created_at, claimed_at, dormant_since) in candidates {
            if !started {
                if dormant_expired(dormant_since, created_at, now) {
                    dormant_stale.push(path);
                }
            } else if claim_expired(
                &manager,
                &dbus,
                id,
                &sender,
                claimed_at.unwrap_or(created_at),
                now,
            )
            .await
            {
                abandoned.push((path, claimed_at));
            }
        }

        // Phase 3：锁内移除。dormant 候选重新确认（防止夹在 begin 声明
        // 之间被误回收——P12 竞态）；abandoned 候选在 live 锁内复查
        // （锁序 live→queue，无人 queue→live，无死锁）——①生成校验：
        // 当前 claimed_at 必须仍是快照时的值（期间被回滚后重新 claim 的
        // 新声明不删）；②与 begin 入队互斥：begin 在 live 锁内完成入队，
        // 这里持锁复查 manager 时 begin 不可能"刚通过复查后入队"——要么
        // 已入队（contains 命中，跳过），要么未入队且 begin 被本锁挡住
        // （移除后 begin 的入队前提——对象仍存在——不成立，begin 中止）。
        let (stale, cancel_ids): (Vec<String>, Vec<String>) = {
            let mut map = live.lock().await;
            let mut removed = Vec::new();
            let mut cancel_ids = Vec::new();
            map.retain(|_, t| {
                if dormant_stale.contains(&t.path)
                    && !t.started
                    && dormant_expired(t.dormant_since, t.created_at, now)
                {
                    removed.push(t.path.clone());
                    false
                } else {
                    true
                }
            });
            let mut to_remove: Vec<u64> = Vec::new();
            for (path, snap_claimed_at) in &abandoned {
                if let Some((id, t)) = map.iter().find(|(_, t)| &t.path == path) {
                    if claim_still_abandoned(&manager, t.claimed_at, *snap_claimed_at, *id).await {
                        to_remove.push(*id);
                    }
                }
            }
            for id in to_remove {
                if let Some(t) = map.remove(&id) {
                    removed.push(t.path.clone());
                    // 被回收的 claim 若还有未完成的远程 PolicyKit 检查，
                    // 带出 cancellation_id，锁外显式取消。
                    if let Some(cid) = t.cancellation_id {
                        cancel_ids.push(cid);
                    }
                }
            }
            (removed, cancel_ids)
        };
        for path in &stale {
            if let Err(e) = server.remove::<TransactionObject, _>(path.as_str()).await {
                error!("Failed to reclaim dormant transaction object {path}: {e}");
            }
        }
        // 创建者断连/claim 超时被回收的授权声明：远程检查与认证弹窗不会
        // 随本地 future 消失而自动关闭，显式 CancelCheckAuthorization——
        // 否则调用方可每隔一个清扫周期重连，远程检查持续累积（begin 本地
        // 超时只覆盖 TimedOut，这里覆盖 dead-sender 与 reaper 回收路径）。
        for cid in &cancel_ids {
            crate::auth::cancel_authorization(&conn, cid).await;
        }
    }
}

/// 判定一个已 claim（started）但未入队的事务对象是否应被回收：创建者
/// 连接已死（sender 锁定，操作永远无法继续），或 claim 超过 CLAIM_TIMEOUT
/// 仍未入队（授权被放弃——即使创建者还连着，也视为超时，防止 claim 绕过
/// 休眠/abandoned 回收被用来长期占槽）。已入队/运行中的事务即使创建者
/// 断开也执行完，不回收。
async fn claim_expired(
    manager: &TransactionManager,
    dbus: &zbus::fdo::DBusProxy<'_>,
    id: u64,
    sender: &str,
    claimed_at: Instant,
    now: Instant,
) -> bool {
    if manager.contains(id).await {
        // 已入队/运行中：不回收。
        return false;
    }
    if now.duration_since(claimed_at) >= CLAIM_TIMEOUT {
        // claim 超时：无论创建者是否还在线，都视为放弃。
        return true;
    }
    // 创建者连接已死；BusName 解析失败时保守地不回收。
    match zbus::names::BusName::try_from(sender) {
        Ok(name) => !dbus.name_has_owner(name).await.unwrap_or(false),
        Err(_) => false,
    }
}

/// 清扫器 phase 3 对单个 abandoned 候选的移除判定（在 live 锁内调用）：
/// ①生成校验——条目当前的 `claimed_at` 必须仍是快照时的值，期间被回滚后
/// 重新 claim 的新声明（重试）不删；②事务仍未入队才移除（begin 在 live
/// 锁内完成入队，与这里互斥，故这里持锁复查时结果稳定）。
async fn claim_still_abandoned(
    manager: &TransactionManager,
    entry_claimed_at: Option<Instant>,
    snap_claimed_at: Option<Instant>,
    id: u64,
) -> bool {
    if entry_claimed_at != snap_claimed_at {
        // 回滚后重新 claim（claimed_at 变了）：新声明，不删。
        return false;
    }
    !manager.contains(id).await
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
                dormant_since: None,
                claimed_at: Some(Instant::now()),
                cancellation_id: None,
                started: true,
            },
        );
        live
    }

    #[tokio::test]
    async fn claim_rollback_clears_started() {
        let live = live_with(1);
        let mut claim = StartedClaim::new(live.clone(), 1, None);
        claim.rollback().await;
        assert!(!live.lock().await.get(&1).unwrap().started);
    }

    #[tokio::test]
    async fn claim_rollback_clears_cancellation_id() {
        let live = Arc::new(Mutex::new(HashMap::new()));
        live.try_lock().unwrap().insert(
            1,
            LiveTransaction {
                path: "/io/aosc/Amo/Transaction/1".into(),
                uid: 0,
                sender: ":1.999".into(),
                created_at: Instant::now(),
                dormant_since: None,
                claimed_at: Some(Instant::now()),
                cancellation_id: Some("amo-1-0".into()),
                started: true,
            },
        );
        let mut claim = StartedClaim::new(live.clone(), 1, Some("amo-1-0".into()));
        claim.rollback().await;
        let guard = live.lock().await;
        let e = guard.get(&1).unwrap();
        assert!(!e.started);
        assert!(e.claimed_at.is_none());
        assert!(
            e.cancellation_id.is_none(),
            "rolled-back claim must not keep a stale cancellation_id"
        );
    }

    /// 授权等待中（claimed-but-not-enqueued）的 Cancel：回滚 claim（清
    /// started/claimed_at）并带出 cancellation_id 供锁外取消远程检查；
    /// 已入队的事务不动（走 manager.cancel）；条目不存在返回 None。
    #[tokio::test]
    async fn cancel_rolls_back_claim_but_not_enqueued() {
        let mgr = crate::transaction::TransactionManager::with_limits(10, 10);

        // 授权等待中：started + 未入队 → 回滚并带出 cancellation_id。
        let mut live = HashMap::new();
        live.insert(
            1,
            LiveTransaction {
                path: "/io/aosc/Amo/Transaction/1".into(),
                uid: 0,
                sender: ":1.999".into(),
                created_at: Instant::now(),
                dormant_since: None,
                claimed_at: Some(Instant::now()),
                cancellation_id: Some("amo-1-0".into()),
                started: true,
            },
        );
        let cid = rollback_claim_if_not_enqueued(&mut live, &mgr, 1).await;
        assert_eq!(cid.as_deref(), Some("amo-1-0"));
        let e = live.get(&1).unwrap();
        assert!(!e.started, "cancel must roll back the claim");
        assert!(e.claimed_at.is_none());
        assert!(
            e.cancellation_id.is_none(),
            "cancel must clear the stale cancellation_id"
        );

        // 已入队：不动（走 manager.cancel）。
        let (block_tx, _block_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let (release_tx, mut release_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        mgr.enqueue(
            None,
            42,
            crate::transaction::TransactionRole::Simulate,
            "tester".into(),
            0,
            Box::pin(async move {
                let _ = block_tx.send(());
                let _ = release_rx.recv().await;
            }),
            None,
        )
        .await
        .expect("enqueue");
        let mut live = HashMap::new();
        live.insert(
            42,
            LiveTransaction {
                path: "/io/aosc/Amo/Transaction/42".into(),
                uid: 0,
                sender: ":1.999".into(),
                created_at: Instant::now(),
                dormant_since: None,
                claimed_at: Some(Instant::now()),
                cancellation_id: Some("amo-42-0".into()),
                started: true,
            },
        );
        assert!(
            rollback_claim_if_not_enqueued(&mut live, &mgr, 42).await.is_none(),
            "enqueued transaction must not be rolled back by cancel"
        );
        assert!(live.get(&42).unwrap().started);
        let _ = release_tx.send(());
    }

    /// 授权等待中（claimed-but-not-enqueued）的 Destroy：移除条目并带出
    /// cancellation_id；dormant 对象照常移除（无 cancellation_id）；已入队
    /// 拒绝（Failed）；条目不存在报 UnknownObject。
    #[tokio::test]
    async fn destroy_removes_claim_but_not_enqueued() {
        let mgr = crate::transaction::TransactionManager::with_limits(10, 10);

        // 授权等待中：移除并带出 cancellation_id。
        let mut live = HashMap::new();
        live.insert(
            1,
            LiveTransaction {
                path: "/io/aosc/Amo/Transaction/1".into(),
                uid: 0,
                sender: ":1.999".into(),
                created_at: Instant::now(),
                dormant_since: None,
                claimed_at: Some(Instant::now()),
                cancellation_id: Some("amo-1-0".into()),
                started: true,
            },
        );
        let cid = remove_for_destroy(&mut live, &mgr, 1).await.expect("destroy");
        assert_eq!(cid.as_deref(), Some("amo-1-0"));
        assert!(!live.contains_key(&1), "destroy must remove the entry");

        // dormant：照常移除，无 cancellation_id。
        let mut live = HashMap::new();
        live.insert(
            2,
            LiveTransaction {
                path: "/io/aosc/Amo/Transaction/2".into(),
                uid: 0,
                sender: ":1.999".into(),
                created_at: Instant::now(),
                dormant_since: Some(Instant::now()),
                claimed_at: None,
                cancellation_id: None,
                started: false,
            },
        );
        let cid = remove_for_destroy(&mut live, &mgr, 2).await.expect("destroy");
        assert!(cid.is_none());
        assert!(!live.contains_key(&2));

        // 已入队：拒绝。
        let (block_tx, _block_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let (release_tx, mut release_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        mgr.enqueue(
            None,
            42,
            crate::transaction::TransactionRole::Simulate,
            "tester".into(),
            0,
            Box::pin(async move {
                let _ = block_tx.send(());
                let _ = release_rx.recv().await;
            }),
            None,
        )
        .await
        .expect("enqueue");
        let mut live = HashMap::new();
        live.insert(
            42,
            LiveTransaction {
                path: "/io/aosc/Amo/Transaction/42".into(),
                uid: 0,
                sender: ":1.999".into(),
                created_at: Instant::now(),
                dormant_since: None,
                claimed_at: Some(Instant::now()),
                cancellation_id: Some("amo-42-0".into()),
                started: true,
            },
        );
        assert!(matches!(
            remove_for_destroy(&mut live, &mgr, 42).await,
            Err(zbus::fdo::Error::Failed(_))
        ));
        assert!(live.contains_key(&42), "enqueued transaction must survive");
        let _ = release_tx.send(());

        // 条目不存在：UnknownObject。
        assert!(matches!(
            remove_for_destroy(&mut live, &mgr, 999).await,
            Err(zbus::fdo::Error::UnknownObject(_))
        ));
    }

    /// begin 授权成功后、入队前的复查：条目存在且 started 才放行；被并发
    /// Cancel 回滚（started=false）或 Destroy 移除（条目缺失）都中止入队。
    #[test]
    fn begin_enqueue_recheck_requires_active_claim() {
        let mut live = HashMap::new();
        live.insert(
            1,
            LiveTransaction {
                path: "/io/aosc/Amo/Transaction/1".into(),
                uid: 0,
                sender: ":1.999".into(),
                created_at: Instant::now(),
                dormant_since: None,
                claimed_at: Some(Instant::now()),
                cancellation_id: Some("amo-1-0".into()),
                started: true,
            },
        );
        assert!(check_claim_still_active(&live, 1).is_ok());

        // 被 Cancel 回滚：started=false → 中止入队。
        live.get_mut(&1).unwrap().started = false;
        assert!(matches!(
            check_claim_still_active(&live, 1),
            Err(zbus::fdo::Error::Failed(_))
        ));

        // 被 Destroy 移除：条目缺失 → UnknownObject。
        live.remove(&1);
        assert!(matches!(
            check_claim_still_active(&live, 1),
            Err(zbus::fdo::Error::UnknownObject(_))
        ));
    }

    #[tokio::test]
    async fn claim_commit_keeps_started() {
        let live = live_with(1);
        let mut claim = StartedClaim::new(live.clone(), 1, None);
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
            let _claim = StartedClaim::new(live.clone(), 1, None);
            // 未 commit 直接 drop（模拟 begin future 被取消）→ 异步回滚。
        }
        // 等 fire-and-forget 回滚任务执行。
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!live.lock().await.get(&1).unwrap().started);
    }

    /// 回滚（授权失败/入队失败/Cancel）必须重置休眠计时：对象存在超过
    /// DORMANT_TIMEOUT 后回滚，若休眠基准仍是 created_at，下一个清扫周期
    /// 会立即把它当 stale 回收——回滚承诺的"回到 dormant 可重试"窗口只有
    /// 一个清扫间隔。dormant_since 在回滚时重置为当前时刻，重试窗口完整。
    #[tokio::test]
    async fn rollback_resets_dormant_timeout() {
        let live = live_with(1);
        let old_dormant = live.lock().await.get(&1).unwrap().dormant_since;
        assert!(old_dormant.is_none(), "claimed entry has no dormant baseline");

        // StartedClaim::rollback（授权失败/入队失败路径）。
        let mut claim = StartedClaim::new(live.clone(), 1, None);
        claim.rollback().await;
        {
            let guard = live.lock().await;
            let e = guard.get(&1).unwrap();
            assert!(!e.started);
            assert!(
                e.dormant_since.is_some(),
                "rollback must reset the dormant baseline"
            );
        }

        // 重新 claim（begin 再次声明）→ 休眠计时暂停。
        {
            let mut guard = live.lock().await;
            let e = guard.get_mut(&1).unwrap();
            e.started = true;
            e.claimed_at = Some(Instant::now());
            e.dormant_since = None;
        }

        // Cancel 回滚（rollback_claim_if_not_enqueued）→ 同样重置。
        let mgr = crate::transaction::TransactionManager::with_limits(10, 10);
        let mut map = live.try_lock().unwrap();
        let cid = rollback_claim_if_not_enqueued(&mut map, &mgr, 1).await;
        assert!(cid.is_none());
        let e = map.get(&1).unwrap();
        assert!(!e.started);
        assert!(
            e.dormant_since.is_some(),
            "cancel rollback must reset the dormant baseline"
        );
    }

    /// 休眠超时判定以 dormant_since（创建或最近回滚）为基准：未记录时退回
    /// created_at；回滚重置后即使对象已存在很久也不判 stale。
    #[test]
    fn dormant_expired_uses_reset_baseline() {
        let now = Instant::now();
        let created = now - DORMANT_TIMEOUT - Duration::from_secs(30);

        // 从未回滚（dormant_since=None）：按 created_at 判 stale。
        assert!(dormant_expired(None, created, now));

        // 回滚重置了 dormant_since：即使 created_at 早已超时也不 stale。
        let reset = now - Duration::from_secs(10);
        assert!(!dormant_expired(Some(reset), created, now));

        // 重置后再次超过 DORMANT_TIMEOUT：判 stale。
        let stale_reset = now - DORMANT_TIMEOUT - Duration::from_secs(1);
        assert!(dormant_expired(Some(stale_reset), created, now));
    }

    /// 代际校验：Cancel 回滚旧 claim 后用户 re-trigger（新 claim、新
    /// cancellation_id），旧 begin 的授权 future 最终失败时其 rollback
    /// 不得清掉新 claim——否则新事务被旧 future 误杀。
    #[tokio::test]
    async fn stale_claim_rollback_does_not_clear_fresh_claim() {
        let live = Arc::new(Mutex::new(HashMap::new()));
        live.try_lock().unwrap().insert(
            1,
            LiveTransaction {
                path: "/io/aosc/Amo/Transaction/1".into(),
                uid: 0,
                sender: ":1.999".into(),
                created_at: Instant::now(),
                dormant_since: None,
                claimed_at: Some(Instant::now()),
                cancellation_id: Some("amo-1-0".into()),
                started: true,
            },
        );

        // 旧 claim（cancellation_id "amo-1-0"）回滚：应清掉自己这一代。
        let mut old_claim = StartedClaim::new(live.clone(), 1, Some("amo-1-0".into()));
        old_claim.rollback().await;
        {
            let guard = live.lock().await;
            let e = guard.get(&1).unwrap();
            assert!(!e.started);
            assert!(e.claimed_at.is_none());
            assert!(e.cancellation_id.is_none());
        }

        // 用户 re-trigger：新 claim（cancellation_id "amo-1-1"）。
        {
            let mut guard = live.lock().await;
            let e = guard.get_mut(&1).unwrap();
            e.started = true;
            e.claimed_at = Some(Instant::now());
            e.cancellation_id = Some("amo-1-1".into());
        }

        // 旧 begin 的授权 future 最终失败 → 旧 claim 再次 rollback（模拟
        // 旧 future 的 Drop 或显式 rollback 迟到）：不得清掉新 claim。
        let mut stale = StartedClaim::new(live.clone(), 1, Some("amo-1-0".into()));
        stale.rollback().await;
        {
            let guard = live.lock().await;
            let e = guard.get(&1).unwrap();
            assert!(
                e.started,
                "stale rollback must not clear the fresh claim's started"
            );
            assert!(
                e.claimed_at.is_some(),
                "stale rollback must not clear the fresh claim's claimed_at"
            );
            assert_eq!(
                e.cancellation_id.as_deref(),
                Some("amo-1-1"),
                "stale rollback must not clear the fresh claim's cancellation_id"
            );
        }
    }

    /// 清扫器对"已 claim 未入队"对象的判定：创建者连接活着（可能在等
    /// polkit 弹窗）且 claim 未超时不回收；连接已死或 claim 超时才回收；
    /// 已入队/运行中的事务即使创建者断开也不回收。
    #[tokio::test]
    async fn claim_expired_requires_dead_sender_or_timeout() {
        let mgr = crate::transaction::TransactionManager::with_limits(10, 10);
        let conn = Connection::session().await.expect("session bus");
        let dbus = zbus::fdo::DBusProxy::new(&conn).await.expect("dbus proxy");

        // 本测试进程的 unique name 活着：事务不在队列 + 刚 claim → 不回收
        // （等价于 polkit 弹窗挂起时的状态）。
        let self_name = conn.unique_name().expect("unique name");
        let now = Instant::now();
        assert!(
            !claim_expired(&mgr, &dbus, 1, self_name.as_str(), now, now).await,
            "live sender + fresh claim must not be reclaimed"
        );

        // 超过 CLAIM_TIMEOUT + 活 sender → 回收（核心新行为：防止 claim
        // 绕过休眠/abandoned 回收长期占槽）。
        assert!(
            claim_expired(
                &mgr,
                &dbus,
                1,
                self_name.as_str(),
                now - CLAIM_TIMEOUT - Duration::from_secs(1),
                now,
            )
            .await,
            "claim past CLAIM_TIMEOUT must be reclaimed even with live sender"
        );

        // 未超时但 sender 已死 → 回收。
        assert!(
            claim_expired(&mgr, &dbus, 1, ":1.999999999", now, now).await,
            "dead sender must be reclaimed"
        );

        // 已入队的事务：即使 sender 已死且 claim 超时也不回收（异步事务
        // 要执行完）。
        let (block_tx, _block_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let (release_tx, mut release_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        mgr.enqueue(
            None,
            42,
            crate::transaction::TransactionRole::Simulate,
            "tester".into(),
            0,
            Box::pin(async move {
                let _ = block_tx.send(());
                let _ = release_rx.recv().await;
            }),
            None,
        )
        .await
        .expect("enqueue");
        assert!(
            !claim_expired(
                &mgr,
                &dbus,
                42,
                ":1.999999999",
                now - CLAIM_TIMEOUT - Duration::from_secs(1),
                now,
            )
            .await,
            "queued transaction must not be reclaimed"
        );
        let _ = release_tx.send(());
    }

    /// 授权等待的超时语义：挂起的授权 future 超过时限被放弃（TimedOut，
    /// drop 掉对 PolicyKit 的等待）；失败与成功的 future 原样透传。
    #[tokio::test]
    async fn auth_timeout_aborts_pending_auth() {
        // 永不 resolve 的授权 future → 超时返回 TimedOut。
        let err =
            TransactionObject::await_auth(1, Duration::from_millis(50), std::future::pending())
                .await
                .expect_err("pending auth must time out");
        assert!(
            matches!(err, zbus::fdo::Error::TimedOut(_)),
            "expected TimedOut, got {err:?}"
        );

        // 快速失败的授权 → 原样返回错误。
        let err = TransactionObject::await_auth(1, Duration::from_secs(5), async {
            Err(zbus::fdo::Error::AccessDenied("no".into()))
        })
        .await
        .expect_err("denied auth must return its error");
        assert!(
            matches!(err, zbus::fdo::Error::AccessDenied(_)),
            "expected AccessDenied, got {err:?}"
        );

        // 快速成功的授权 → Ok。
        TransactionObject::await_auth(1, Duration::from_secs(5), async { Ok(()) })
            .await
            .expect("approved auth must succeed");
    }

    /// 清扫器 phase 3 的移除判定：claim 被回滚后重新声明（claimed_at 变）
    /// 的新声明不删；claimed_at 未变且未入队才删；已入队即使 claimed_at
    /// 未变也不删（begin 在 live 锁内入队，与清扫器互斥）。
    #[tokio::test]
    async fn expired_claim_not_removed_after_fresh_retry() {
        let mgr = crate::transaction::TransactionManager::with_limits(10, 10);
        let now = Instant::now();

        // 快照是旧 claim，当前条目是新 claim（回滚后重试）→ 不删。
        assert!(
            !claim_still_abandoned(&mgr, Some(now), Some(now - Duration::from_secs(1)), 1).await,
            "fresh retry claim must not be removed"
        );
        // 条目已被回滚为休眠（claimed_at=None），快照是旧 claim → 不删。
        assert!(
            !claim_still_abandoned(&mgr, None, Some(now - Duration::from_secs(1)), 1).await,
            "rolled-back entry must not be removed as abandoned"
        );
        // claimed_at 一致且未入队 → 删。
        assert!(
            claim_still_abandoned(&mgr, Some(now), Some(now), 1).await,
            "same-generation expired claim must be removed"
        );

        // 已入队的事务：claimed_at 一致也不删（异步事务要执行完、可取消）。
        let (block_tx, _block_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let (release_tx, mut release_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        mgr.enqueue(
            None,
            42,
            crate::transaction::TransactionRole::Simulate,
            "tester".into(),
            0,
            Box::pin(async move {
                let _ = block_tx.send(());
                let _ = release_rx.recv().await;
            }),
            None,
        )
        .await
        .expect("enqueue");
        assert!(
            !claim_still_abandoned(&mgr, Some(now), Some(now), 42).await,
            "queued transaction must not be removed"
        );
        let _ = release_tx.send(());
    }

    /// PolicyKit cancellation_id：非空且唯一（空 ID 的远程检查不可通过
    /// CancelCheckAuthorization 取消）。
    #[test]
    fn cancellation_ids_are_unique_and_nonempty() {
        let a = next_cancellation_id(1);
        let b = next_cancellation_id(1);
        assert!(!a.is_empty() && !b.is_empty());
        assert!(a.starts_with("amo-1-"), "unexpected id {a}");
        assert_ne!(a, b, "counter must yield unique ids");
    }

    /// 事务参数校验：字节上限防大字符串；元素上限防空串/极短串的海量
    /// 条目（每个 String 都占 24 字节头，空串绕过字节上限）。正常大小
    /// 通过；单个超字节、合计超字节、超元素数都被拒绝。
    #[test]
    fn oversized_transaction_arguments_rejected() {
        // 正常大小通过。
        assert!(check_arg_size(&["fish".into()], &["vim".into()]).is_ok());
        // install 单个超字节拒绝。
        let big = "x".repeat(MAX_TRANSACTION_ARG_BYTES + 1);
        assert!(matches!(
            check_arg_size(&[big], &[]),
            Err(zbus::fdo::Error::LimitsExceeded(_))
        ));
        // install + remove 合计超字节也拒绝。
        let half = MAX_TRANSACTION_ARG_BYTES / 2 + 1;
        assert!(check_arg_size(&["a".repeat(half)], &["b".repeat(half)]).is_err());
        // 恰好等于字节上限允许。
        assert!(check_arg_size(&["y".repeat(MAX_TRANSACTION_ARG_BYTES)], &[]).is_ok());
        // 超元素数（全部空串，字节数=0 但内存可观）拒绝。
        let many = vec![String::new(); MAX_TRANSACTION_ARG_ITEMS + 1];
        assert!(matches!(
            check_arg_size(&many, &[]),
            Err(zbus::fdo::Error::LimitsExceeded(_))
        ));
        // 恰好等于元素上限允许（空串）。
        let exactly = vec![String::new(); MAX_TRANSACTION_ARG_ITEMS];
        assert!(check_arg_size(&exactly, &[]).is_ok());
        // install + remove 合计超元素数也拒绝。
        let half_items = MAX_TRANSACTION_ARG_ITEMS / 2 + 1;
        assert!(
            check_arg_size(&vec![String::new(); half_items], &vec![String::new(); half_items])
                .is_err()
        );
    }

    /// Progress 事件必须能承载标量载荷（oma_refresh::db::Event 的单元变体
    /// 如 Done/ScanningTopic 序列化为 `"Done"` 这类 JSON 标量；内部标签的
    /// newtype 无法承载标量，payload 字段则任意值都行）。同时验证 map
    /// 载荷与客户端可反序列化。
    #[test]
    fn progress_event_carries_scalar_and_map_payloads() {
        // 标量：oma 事件单元变体（Done）。
        let event = TransactionEvent::Progress {
            payload: serde_json::json!("Done"),
        };
        let json = serde_json::to_string(&event).expect("scalar progress must serialize");
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["type"], "progress");
        assert_eq!(v["payload"], "Done");
        // 反序列化回枚举（客户端同构结构）。
        assert!(matches!(
            serde_json::from_str::<TransactionEvent>(&json).unwrap(),
            TransactionEvent::Progress { payload } if payload == "Done"
        ));

        // map 载荷：oma 事件 struct 变体（如 DownloadEvent）。
        let event = TransactionEvent::Progress {
            payload: serde_json::json!({"DownloadEvent": {"AllDone": {}}}),
        };
        let json = serde_json::to_string(&event).expect("map progress must serialize");
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["type"], "progress");
        assert_eq!(
            v["payload"]["DownloadEvent"]["AllDone"],
            serde_json::json!({})
        );
    }

    /// 对未知 cancellation_id 调用取消是无操作（验证
    /// CancelCheckAuthorization 的 D-Bus 线路可用且不会报错）。
    #[tokio::test]
    async fn cancel_unknown_polkit_check_is_noop() {
        let Ok(conn) = Connection::system().await else {
            eprintln!("no system bus, skipping");
            return;
        };
        crate::auth::cancel_authorization(&conn, "amo-test-does-not-exist").await;
    }
}
