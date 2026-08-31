//! 事务系统：所有操作（刷新、装包、模拟、查更新）都排成队列，一次只跑
//! 一个，谁先来谁先执行。
//!
//! 每个事务是独立的 D-Bus 对象（PackageKit 风格），状态变化
//! （排队 → 运行 → 完成/取消）从该事务自己的对象路径发出 `TransactionState`
//! 信号；发射目标（SignalEmitter）在入队时由调用方提供（测试传 `None`）。
//!
//! 取消只对还在排队的有效：已经开跑的事务不能打断（dpkg 正在改系统，
//! 中途停很危险）。
//!
//! 模块结构：
//! - [`types`]：共享类型（角色/状态/事件/错误）
//! - [`limits`]：事务参数大小校验
//! - [`live`]：活动事务对象注册表 + claim 生命周期 + 清扫器
//! - [`object`]：单个事务的 D-Bus 对象（PackageKit 风格）

mod limits;
pub(crate) mod live;
pub(crate) mod object;
mod types;

#[cfg(test)]
mod tests;

pub(crate) use live::{
    LiveTransaction, MAX_LIVE_PER_UID, MAX_LIVE_TRANSACTIONS, reclaim_dormant,
};
pub(crate) use object::TransactionObject;
pub use types::{
    CancelError, EnqueueError, ResultReport, Task, TaskStatus, TransactionEvent,
    TransactionRole, TransactionState, TransactionStateEvent,
};

use crate::transaction::object::TransactionObjectSignals;
use serde::Serialize;
use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, Notify};
use tracing::error;
use zbus::object_server::SignalEmitter;

pub struct Transaction {
    pub id: u64,
    pub role: TransactionRole,
    pub(crate) state: Mutex<TransactionState>,
    pub(crate) cancelled: AtomicBool,
    pub caller: String,
    pub uid: u32,
    pub(crate) created_at: u64,
    /// 本事务对象路径上的信号发射目标（None 表示测试等不需要发信号）。
    pub(crate) emitter: Option<SignalEmitter<'static>>,
    pub(crate) task: Mutex<Option<Task>>,
    /// 事务结束（完成或取消）后的清理回调，如移除对应的 D-Bus 对象。
    pub(crate) on_done: Option<Arc<dyn Fn() + Send + Sync>>,
}

#[derive(Clone, Serialize)]
pub struct TransactionInfo {
    pub transaction_id: u64,
    pub role: TransactionRole,
    pub state: TransactionState,
    pub caller: String,
    pub uid: u32,
    pub created_at: u64,
}

/// PackageKit 风格的事务调度器：FIFO 队列 + 单一 runner 串行执行。
///
/// 队列有界：`Simulate` / `UpdatesList` 等入口免 polkit，任何本地调用者
/// 都能提交任务，若无上限可无限堆积 boxed future 耗尽内存、饿死后续
/// 授权请求。因此入队时在 queue 锁内检查全局上限与每用户配额
/// （与 runner 的 pop_front 互斥，检查与入队原子完成）。
pub struct TransactionManager {
    /// 等待执行的事务队列（FIFO）
    queue: Mutex<VecDeque<Arc<Transaction>>>,
    /// 当前正在执行的事务
    running: Mutex<Option<Arc<Transaction>>>,
    /// 唤醒 runner
    notify: Notify,
    /// 队列中允许的最大事务数（含 running 槽）。
    max_queued: usize,
    /// 单个 uid 在队列中允许的最大事务数。
    max_per_uid: usize,
}

impl TransactionManager {
    /// 创建管理器并启动 runner。
    pub fn new() -> Arc<Self> {
        Self::with_limits(64, 8)
    }

    /// 创建管理器并启动 runner，指定队列上限与每用户配额。
    pub fn with_limits(max_queued: usize, max_per_uid: usize) -> Arc<Self> {
        let mgr = Arc::new(Self {
            queue: Mutex::new(VecDeque::new()),
            running: Mutex::new(None),
            notify: Notify::new(),
            max_queued,
            max_per_uid,
        });
        let runner = mgr.clone();
        tokio::spawn(runner.run());
        mgr
    }

    /// 把任务排进队列，返回对应的事务；`ctxt` 是本事务对象路径上的
    /// 信号发射目标（测试传 `None`），`on_done` 在事务结束时调用。
    ///
    /// 队列已满或该 uid 已占用过多排队事务时返回 [`EnqueueError`]，
    /// 此时任务不会入队、不会执行，也不会发出任何信号。
    pub async fn enqueue(
        &self,
        ctxt: impl Into<Option<SignalEmitter<'static>>>,
        id: u64,
        role: TransactionRole,
        caller: String,
        uid: u32,
        task: Task,
        on_done: Option<Arc<dyn Fn() + Send + Sync>>,
    ) -> Result<Arc<Transaction>, EnqueueError> {
        // 限额检查与 push_back 都在 queue 锁内完成，与 runner 的
        // pop_front 互斥：要么本事务入队（占用一个名额），要么 runner
        // 先出队（释放名额），不会出现"检查通过但入队时已超限"。
        let tx = Arc::new(Transaction {
            id,
            role,
            state: Mutex::new(TransactionState::Queued),
            cancelled: AtomicBool::new(false),
            caller,
            uid,
            created_at: now_epoch(),
            emitter: ctxt.into(),
            task: Mutex::new(Some(task)),
            on_done,
        });
        {
            let mut queue = self.queue.lock().await;
            // 全局上限含 running 槽：排队中 + 正在执行的总数。
            let running = self.running.lock().await;
            let in_flight = queue.len() + usize::from(running.is_some());
            if in_flight >= self.max_queued {
                return Err(EnqueueError::QueueFull);
            }
            let per_uid = queue.iter().filter(|t| t.uid == uid).count();
            if per_uid >= self.max_per_uid {
                return Err(EnqueueError::QuotaExceeded);
            }
            queue.push_back(tx.clone());
            // 在 queue 锁内、事务已入队时发出 Queued：runner 无法出队、
            // cancel 无法标记，保证 Queued 一定是该事务的第一个信号——
            // 否则 runner 可能在 Queued 完成前弹出并发出 Running/Finished，
            // 监听者会看到完成的事务"倒退回 queued"；cancel 也可能先发
            // Cancelled。
            self.emit_event(&tx, TransactionState::Queued).await;
        }
        self.notify.notify_one();

        Ok(tx)
    }

    /// 取消一个仍在排队中的事务。运行中或已结束的事务不可取消。
    /// 只有事务所有者（uid 匹配）或 root（uid 0）可以取消，
    /// 防止其他用户取消已通过 polkit 授权的 ApplyChanges 等事务。
    /// 失败时返回具体原因（见 [`CancelError`]）。
    ///
    /// 注意：所有权只按 uid 判断，不比较 caller——caller 是 D-Bus
    /// unique name，每次连接都会变，不能作为稳定身份。
    pub async fn cancel(&self, id: u64, uid: u32) -> Result<(), CancelError> {
        // 查找 + 所有权检查 + 标记取消 + 从队列移除都在 queue 锁内完成，
        // 与 runner 的 pop_front 互斥：要么 cancel 先移除（runner 之后
        // 找不到它），要么 runner 先出队（cancel 找不到，返回
        // NotFound/Running）。这样"取消成功"与"任务执行"不可能同时发生
        // ——否则 runner 可能在 cancel 检查 state 之后、设置 cancelled
        // 之前弹出事务并执行任务，导致 CancelTransaction 返回成功但任务
        // 照跑（对 ApplyChanges 尤其危险）。
        //
        // 被取消的事务必须立即从队列移除：它不再占用全局上限与每用户
        // 配额（否则长任务期间多个成功取消的请求会让后续请求一直
        // LimitsExceeded），也不再保留任务与清理回调（on_done 在此触发，
        // 而不是等 runner 排到它）。
        let tx = {
            let mut queue = self.queue.lock().await;
            let Some(pos) = queue.iter().position(|tx| tx.id == id) else {
                // 队列里没有：可能是正在运行的事务（在 running 槽里）。
                let running = self.running.lock().await;
                if running.as_ref().is_some_and(|tx| tx.id == id) {
                    return Err(CancelError::Running);
                }
                return Err(CancelError::NotFound);
            };
            let tx = queue.remove(pos).unwrap();
            // 所有权检查：root 可取消任意事务；否则必须是事务所有者（同 uid）。
            if uid != 0 && tx.uid != uid {
                // 不是所有者：把事务放回原位，当作没取消过。
                queue.insert(pos, tx.clone());
                return Err(CancelError::NotOwner);
            }
            let mut state = tx.state.lock().await;
            match *state {
                TransactionState::Queued => {}
                TransactionState::Running => return Err(CancelError::Running),
                TransactionState::Cancelled => return Err(CancelError::AlreadyCancelled),
                TransactionState::Finished => return Err(CancelError::NotFound),
            }
            *state = TransactionState::Cancelled;
            tx.cancelled.store(true, Ordering::SeqCst);
            drop(state);
            tx
        }; // 释放 queue 锁
        // 信号与清理在锁外执行：不持锁做 D-Bus 广播 / 回调。
        self.emit_event(&tx, TransactionState::Cancelled).await;
        tx.on_done();
        Ok(())
    }

    /// 当前所有进行中事务（queued + running），按 id 排序。
    ///
    /// 队列快照与 running 槽读取必须在同一把 queue 锁内完成（锁序
    /// queue→running，与 runner 出队/cancel/enqueue 一致）：runner 是
    /// 持 queue 锁把队头移进 running 槽的，若此处先释放 queue 锁再读
    /// running，就会把"已从队列克隆、随后被移入 running"的同一事务
    /// 计两次——GetTransactionList 出现重复记录，在飞采样器也会间歇性
    /// 数到超过配置上限。
    ///
    /// 每个事务的 state 也必须在持锁时读取：锁外读的话，克隆的排队事务
    /// 可能在读之前被 cancel 置为 Cancelled、克隆的运行事务可能在读之前
    /// 变成 Finished——API 承诺只返回 queued + running，不能把终态事务
    /// 带出去。锁序 queue→running→state，与 cancel（queue→state）一致，
    /// 无死锁。
    pub async fn list(&self) -> Vec<TransactionInfo> {
        let mut out = Vec::new();
        {
            let queue = self.queue.lock().await;
            for tx in queue.iter() {
                out.push(tx.info(*tx.state.lock().await));
            }
            if let Some(tx) = self.running.lock().await.as_ref() {
                out.push(tx.info(*tx.state.lock().await));
            }
        }
        out.sort_by_key(|t| t.transaction_id);
        out
    }

    /// 该事务是否在队列中或正在运行（尚未结束）。对象清扫器用它区分
    /// "已 claim 但尚未入队"（授权等待中/被放弃）与"已入队执行中"：
    /// 只有前者在创建者连接断开后可被回收。
    pub(crate) async fn contains(&self, id: u64) -> bool {
        let queue = self.queue.lock().await;
        if queue.iter().any(|t| t.id == id) {
            return true;
        }
        self.running
            .lock()
            .await
            .as_ref()
            .is_some_and(|t| t.id == id)
    }

    /// runner 主循环：串行弹出队列事务并执行，队列空时等待唤醒。
    async fn run(self: Arc<Self>) {
        loop {
            // 出队与装进 running 槽在同一把 queue 锁内完成（锁序 queue→
            // running，与 cancel/enqueue 一致），避免窗口期事务同时不在
            // queue 也不在 running：否则 GetTransactionList 会短暂看不到
            // 它、cancel 误报 NotFound、enqueue 的在飞计数少算一个导致
            // 超限入队。
            let tx = {
                let mut queue = self.queue.lock().await;
                let Some(tx) = queue.pop_front() else {
                    drop(queue);
                    self.notify.notified().await;
                    continue;
                };

                // 排队期间被取消：不执行任务，直接丢弃（cancel 已发
                // Cancelled 事件）。检查仍在 queue 锁内，与 cancel 互斥。
                if tx.cancelled.load(Ordering::SeqCst) {
                    drop(queue);
                    // 取消的事务不会执行任务，在此触发清理（如移除
                    // 对应的 D-Bus 事务对象）。
                    tx.on_done();
                    continue;
                }

                *self.running.lock().await = Some(tx.clone());
                tx
            }; // 释放 queue 锁

            self.set_state(&tx, TransactionState::Running).await;

            if let Some(task) = tx.task.lock().await.take() {
                // 用一层 spawn 包裹，隔离任务 panic，防止 runner 循环终止。
                if let Err(e) = tokio::task::spawn(task).await {
                    let detail = panic_detail(e);
                    error!(transaction_id = tx.id, "Transaction task panicked: {detail}");
                    // 任务 panic 时不会有正常 Result（结果由任务内部在收尾
                    // 时发射）：若不补发失败结果，客户端 wait_result 只看到
                    // Finished（被忽略）而永远等不到 Result（移除路径对象
                    // 也不会关闭连接级信号流）。这里从该事务自己的路径补发
                    // Failed 结果，随后进入终态——结果 → 终态顺序与单流
                    // 协议一致。
                    self.emit_failure(&tx, detail).await;
                }
            }

            *self.running.lock().await = None;
            self.set_state(&tx, TransactionState::Finished).await;
            tx.on_done();
        }
    }

    async fn set_state(&self, tx: &Transaction, state: TransactionState) {
        *tx.state.lock().await = state;
        self.emit_event(tx, state).await;
    }

    /// 广播一次状态变更（TransactionEvent 的 State 变体），从该事务
    /// 自己的对象路径发出，客户端按路径订阅即可收到。
    async fn emit_event(&self, tx: &Transaction, state: TransactionState) {
        let Some(ctxt) = tx.emitter.as_ref() else {
            return;
        };
        let event = TransactionStateEvent {
            transaction_id: tx.id,
            role: tx.role,
            state,
        };
        let event = TransactionEvent::State(event);
        if let Ok(json) = serde_json::to_string(&event)
            && let Err(e) = TransactionObjectSignals::transaction_event(ctxt, json).await
        {
            error!("Failed to emit TransactionState signal: {e}");
        }
    }

    /// 事务任务 panic（无正常 Result）时广播失败结果：从该事务自己的对象
    /// 路径发出 `TransactionEvent::Result`（Failed，result=None），客户端
    /// wait_result 才能收到失败而不是永远等待。
    async fn emit_failure(&self, tx: &Transaction, detail: String) {
        let Some(ctxt) = tx.emitter.as_ref() else {
            return;
        };
        let Ok(json) = failure_result_json(tx, detail) else {
            error!("Failed to serialize failure Result event");
            return;
        };
        if let Err(e) = TransactionObjectSignals::transaction_event(ctxt, json).await {
            error!("Failed to emit failure Result event: {e}");
        }
    }
}

/// 构造"任务失败"结果事件的 JSON：`{"type":"result","status":{"Failed":...}}`
/// 且无 result 字段。单独抽出以便单测载荷形状。
pub(crate) fn failure_result_json(tx: &Transaction, detail: String) -> serde_json::Result<String> {
    let report = ResultReport {
        transaction_id: tx.id,
        role: tx.role,
        status: TaskStatus::Failed(detail),
        result: None,
    };
    let event = TransactionEvent::Result(report);
    serde_json::to_string(&event)
}

/// 提取任务 panic 的载荷消息（`panic!("literal")` 是 `&str`，
/// `panic!(format!(...))` 是 `String`），供失败结果与日志使用。
pub(crate) fn panic_detail(e: tokio::task::JoinError) -> String {
    if !e.is_panic() {
        return "transaction task was cancelled".to_string();
    }
    let payload = e.into_panic();
    if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else {
        "transaction task panicked".to_string()
    }
}

impl Transaction {
    /// 事务结束（完成或取消）后的清理回调，如移除对应的 D-Bus 对象。
    fn on_done(&self) {
        if let Some(f) = self.on_done.as_ref() {
            f();
        }
    }

    /// 构造当前快照；state 由调用方读取后传入，锁在调用处显式持有。
    fn info(&self, state: TransactionState) -> TransactionInfo {
        TransactionInfo {
            transaction_id: self.id,
            role: self.role,
            state,
            caller: self.caller.clone(),
            uid: self.uid,
            created_at: self.created_at,
        }
    }
}

fn now_epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}