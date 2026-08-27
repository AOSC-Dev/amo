//! 事务调度：所有操作（刷新、装包、模拟、查更新）都排成队列，一次只跑
//! 一个，谁先来谁先执行。
//!
//! 每个事务的状态变化（排队 → 运行 → 完成/取消）都会广播 `TransactionState`
//! 信号；发射目标在入队时由调用方提供（首次设置后忽略）。
//!
//! 取消只对还在排队的有效：已经开跑的事务不能打断（dpkg 正在改系统，
//! 中途停很危险）。

use crate::server::AmoSignals;
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, Notify};
use tracing::error;
use zbus::object_server::SignalEmitter;

/// 事务角色：对应 amo 的各个操作入口。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionRole {
    /// 刷新软件源索引
    Refresh,
    /// 安装 / 移除 / 升级
    ApplyChanges,
    /// 模拟事务（预览将发生的变更）
    Simulate,
    /// 获取可更新列表
    UpdatesList,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionState {
    /// 已入队，等待执行
    Queued,
    /// 正在执行
    Running,
    /// 已完成
    Finished,
    /// 排队期间被取消，未执行
    Cancelled,
}

pub type Task = Pin<Box<dyn Future<Output = ()> + Send>>;

pub struct Transaction {
    pub id: u64,
    pub role: TransactionRole,
    state: Mutex<TransactionState>,
    cancelled: AtomicBool,
    pub caller: String,
    pub uid: u32,
    created_at: u64,
    task: Mutex<Option<Task>>,
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

/// `TransactionState` 信号的 JSON 载荷。
#[derive(Clone, Serialize)]
pub struct TransactionStateEvent {
    pub transaction_id: u64,
    pub role: TransactionRole,
    pub state: TransactionState,
}

/// 取消事务失败的原因。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CancelError {
    /// 事务不存在（或已结束，不再保留在列表里）。
    NotFound,
    /// 调用者不是事务所有者，且不是 root。
    NotOwner,
    /// 事务已开始运行，不能取消。
    Running,
    /// 事务已处于取消状态。
    AlreadyCancelled,
}

/// 入队被拒绝的原因。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnqueueError {
    /// 队列已满（全局上限）。
    QueueFull,
    /// 该调用者（uid）已占用过多排队中的事务（每用户配额）。
    QuotaExceeded,
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
    /// TransactionState 信号的发射目标（首次 enqueue 时设置，之后忽略）。
    emitter: OnceLock<SignalEmitter<'static>>,
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
            emitter: OnceLock::new(),
            max_queued,
            max_per_uid,
        });
        let runner = mgr.clone();
        tokio::spawn(runner.run());
        mgr
    }

    /// 把任务排进队列，返回对应的事务；`ctxt` 用来广播状态信号，
    /// 不需要发信号时（如测试）传 `None`。
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
    ) -> Result<Arc<Transaction>, EnqueueError> {
        if let Some(ctxt) = ctxt.into() {
            let _ = self.emitter.get_or_init(|| ctxt);
        }

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
            task: Mutex::new(Some(task)),
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
        }
        self.emit_event(&tx, TransactionState::Queued).await;
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
        // 查找 + 所有权检查 + 标记取消都在 queue 锁内完成，与 runner 的
        // pop_front 互斥：要么 cancel 先标记（runner 之后看到 cancelled 跳过），
        // 要么 runner 先出队（cancel 找不到，返回 NotFound/Running）。
        // 这样"取消成功"与"任务执行"不可能同时发生——否则 runner 可能在
        // cancel 检查 state 之后、设置 cancelled 之前弹出事务并执行任务，
        // 导致 CancelTransaction 返回成功但任务照跑（对 ApplyChanges 尤其危险）。
        let tx = {
            let queue = self.queue.lock().await;
            let Some(tx) = queue.iter().find(|tx| tx.id == id).cloned() else {
                // 队列里没有：可能是正在运行的事务（在 running 槽里）。
                let running = self.running.lock().await;
                if running.as_ref().is_some_and(|tx| tx.id == id) {
                    return Err(CancelError::Running);
                }
                return Err(CancelError::NotFound);
            };
            // 所有权检查：root 可取消任意事务；否则必须是事务所有者（同 uid）。
            if uid != 0 && tx.uid != uid {
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
        self.emit_event(&tx, TransactionState::Cancelled).await;
        Ok(())
    }

    /// 当前所有进行中事务（queued + running），按 id 排序。
    pub async fn list(&self) -> Vec<TransactionInfo> {
        let mut txs = Vec::new();
        {
            let queue = self.queue.lock().await;
            txs.extend(queue.iter().cloned());
        }
        if let Some(tx) = self.running.lock().await.as_ref() {
            txs.push(tx.clone());
        }
        let mut out = Vec::with_capacity(txs.len());
        for tx in &txs {
            out.push(tx.info(*tx.state.lock().await));
        }
        out.sort_by_key(|t| t.transaction_id);
        out
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
                    continue;
                }

                *self.running.lock().await = Some(tx.clone());
                tx
            }; // 释放 queue 锁

            self.set_state(&tx, TransactionState::Running).await;

            if let Some(task) = tx.task.lock().await.take() {
                // 用一层 spawn 包裹，隔离任务 panic，防止 runner 循环终止。
                if tokio::task::spawn(task).await.is_err() {
                    error!(transaction_id = tx.id, "Transaction task panicked");
                }
            }

            *self.running.lock().await = None;
            self.set_state(&tx, TransactionState::Finished).await;
        }
    }

    async fn set_state(&self, tx: &Transaction, state: TransactionState) {
        *tx.state.lock().await = state;
        self.emit_event(tx, state).await;
    }

    /// 广播一次状态变更（TransactionState 信号）。
    async fn emit_event(&self, tx: &Transaction, state: TransactionState) {
        let Some(ctxt) = self.emitter.get() else {
            return;
        };
        let event = TransactionStateEvent {
            transaction_id: tx.id,
            role: tx.role,
            state,
        };
        if let Ok(json) = serde_json::to_string(&event)
            && let Err(e) = AmoSignals::transaction_state(ctxt, json).await
        {
            error!("Failed to emit TransactionState signal: {e}");
        }
    }
}

impl Transaction {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::Future,
        sync::atomic::{AtomicBool, AtomicU64},
        time::Duration,
    };
    use tokio::sync::mpsc;

    /// 轮询列表直到指定事务到达指定状态（仅对仍在列表中的状态有效，
    /// 如 Running / Queued；Finished / Cancelled 会从列表移除）。
    async fn wait_until_state(mgr: &TransactionManager, id: u64, state: TransactionState) {
        for _ in 0..200 {
            let list = mgr.list().await;
            if list
                .iter()
                .any(|t| t.transaction_id == id && t.state == state)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timeout waiting for transaction {id} to reach {state:?}");
    }

    /// 轮询直到条件成立。
    async fn wait_until<F, Fut>(mut cond: F)
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = bool>,
    {
        for _ in 0..200 {
            if cond().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timeout waiting for condition");
    }

    #[tokio::test]
    async fn runs_in_fifo_order() {
        let mgr = TransactionManager::new();
        let order = Arc::new(AtomicU64::new(0));

        let o1 = order.clone();
        mgr.enqueue(
            None,
            1,
            TransactionRole::Refresh,
            "c1".into(),
            1000,
            Box::pin(async move {
                o1.store(1, Ordering::SeqCst);
            }),
        )
        .await
        .unwrap();

        let o2 = order.clone();
        mgr.enqueue(
            None,
            2,
            TransactionRole::ApplyChanges,
            "c2".into(),
            1000,
            Box::pin(async move {
                o2.store(2, Ordering::SeqCst);
            }),
        )
        .await
        .unwrap();

        // 单一 runner 串行执行：等两个任务都跑完（最后写入的是 2）。
        wait_until(|| async { order.load(Ordering::SeqCst) == 2 }).await;
        assert_eq!(order.load(Ordering::SeqCst), 2);

        // 已结束的事务不再保留在列表里（与 PackageKit 一致）。
        assert!(mgr.list().await.is_empty());
    }

    #[tokio::test]
    async fn cancel_queued_but_not_running() {
        let mgr = TransactionManager::new();
        let (block_tx, _block_rx) = mpsc::unbounded_channel::<()>();
        let (release_tx, mut release_rx) = mpsc::unbounded_channel::<()>();
        let ran2 = Arc::new(AtomicBool::new(false));
        let ran3 = Arc::new(AtomicBool::new(false));

        // t1 阻塞在任务里，让 t2/t3 在队列中排队。
        let t1 = mgr
            .enqueue(
                None,
                1,
                TransactionRole::Refresh,
                "c1".into(),
                1000,
                Box::pin(async move {
                    let _ = block_tx.send(());
                    let _ = release_rx.recv().await; // 等测试释放
                }),
            )
            .await
            .unwrap();
        wait_until_state(&mgr, t1.id, TransactionState::Running).await;

        let ran2_clone = ran2.clone();
        let t2 = mgr
            .enqueue(
                None,
                2,
                TransactionRole::UpdatesList,
                "c2".into(),
                1000,
                Box::pin(async move {
                    ran2_clone.store(true, Ordering::SeqCst);
                }),
            )
            .await
            .unwrap();
        let ran3_clone = ran3.clone();
        let t3 = mgr
            .enqueue(
                None,
                3,
                TransactionRole::Simulate,
                "c3".into(),
                1000,
                Box::pin(async move {
                    ran3_clone.store(true, Ordering::SeqCst);
                }),
            )
            .await
            .unwrap();

        // 排队中 + 运行中的事务都在列表里可见。
        {
            let list = mgr.list().await;
            assert_eq!(list.len(), 3);
            let by_id = |id: u64| list.iter().find(|t| t.transaction_id == id).unwrap();
            assert_eq!(by_id(t1.id).state, TransactionState::Running);
            assert_eq!(by_id(t2.id).state, TransactionState::Queued);
            assert_eq!(by_id(t3.id).state, TransactionState::Queued);
        }

        // 排队中的 t2 可取消；运行中的 t1 不可取消。
        assert_eq!(mgr.cancel(t2.id, 1000).await, Ok(()));
        assert_eq!(mgr.cancel(t1.id, 1000).await, Err(CancelError::Running));
        // 已取消的事务再取消返回 AlreadyCancelled。
        assert_eq!(
            mgr.cancel(t2.id, 1000).await,
            Err(CancelError::AlreadyCancelled)
        );

        // 释放 t1；runner 应跳过 t2 直接执行 t3。
        let _ = release_tx.send(());
        wait_until(|| async { ran3.load(Ordering::SeqCst) }).await;
        // 被取消的 t2 从未执行。
        assert!(!ran2.load(Ordering::SeqCst));
        // 全部结束后列表清空（finished 与 cancelled 都不保留）。
        wait_until(|| async { mgr.list().await.is_empty() }).await;
    }

    #[tokio::test]
    async fn cancel_requires_ownership() {
        let mgr = TransactionManager::new();
        let (block_tx, _block_rx) = mpsc::unbounded_channel::<()>();
        let (release_tx, mut release_rx) = mpsc::unbounded_channel::<()>();
        let ran = Arc::new(AtomicBool::new(false));

        // t1 阻塞在任务里，让 t2 在队列中排队。
        let t1 = mgr
            .enqueue(
                None,
                1,
                TransactionRole::Refresh,
                "c1".into(),
                1000,
                Box::pin(async move {
                    let _ = block_tx.send(());
                    let _ = release_rx.recv().await; // 等测试释放
                }),
            )
            .await
            .unwrap();
        wait_until_state(&mgr, t1.id, TransactionState::Running).await;

        let ran_clone = ran.clone();
        let t2 = mgr
            .enqueue(
                None,
                2,
                TransactionRole::ApplyChanges,
                "alice".into(),
                1000,
                Box::pin(async move {
                    ran_clone.store(true, Ordering::SeqCst);
                }),
            )
            .await
            .unwrap();

        // 其他用户（不同 uid）不能取消。
        assert_eq!(
            mgr.cancel(t2.id, 1001).await,
            Err(CancelError::NotOwner)
        );
        // 事务所有者（同 uid）可以取消。
        assert_eq!(mgr.cancel(t2.id, 1000).await, Ok(()));
        // root（uid 0）可以取消任意事务。
        let t3 = mgr
            .enqueue(
                None,
                3,
                TransactionRole::UpdatesList,
                "carol".into(),
                1002,
                Box::pin(async move {}),
            )
            .await
            .unwrap();
        assert_eq!(mgr.cancel(t3.id, 0).await, Ok(()));
        // 不存在的事务返回 NotFound。
        assert_eq!(mgr.cancel(999, 1000).await, Err(CancelError::NotFound));

        // 释放 t1；runner 应跳过被取消的 t2/t3。
        let _ = release_tx.send(());
        wait_until(|| async { mgr.list().await.is_empty() }).await;
        assert!(!ran.load(Ordering::SeqCst));
    }

    /// 回归测试：cancel 与 runner 出队互斥。
    ///
    /// 旧实现里 cancel 先克隆 tx 再释放 queue 锁，runner 可能在这之间
    /// pop_front 并检查 cancelled（false），随后 cancel 标记取消并返回
    /// 成功——但任务照跑。修复后查找+标记都在 queue 锁内完成，与
    /// pop_front 互斥，因此"cancel 返回 Ok"与"任务执行"不可能同时发生。
    ///
    /// 竞态窗口极小（runner 检查 cancelled 与设置 Running 之间只有两个
    /// 原子操作），单轮很难触发；这里用多事务 + 并发 cancel 压测，
    /// 并断言不变量：任何 cancel 返回 Ok 的事务，其任务绝不能执行。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancel_and_dequeue_are_mutually_exclusive() {
        for round in 0..20 {
            let mgr = TransactionManager::new();
            // 每个事务独立的执行标志。
            let ran: Vec<Arc<AtomicBool>> = (0..8).map(|_| Arc::new(AtomicBool::new(false))).collect();

            // 排 8 个事务，每个任务设置自己的标志。
            let mut txs = Vec::new();
            for i in 0..8u64 {
                let ran_clone = ran[i as usize].clone();
                let tx = mgr
                    .enqueue(
                        None,
                        i + 1,
                        TransactionRole::ApplyChanges,
                        "alice".into(),
                        1000,
                        Box::pin(async move {
                            ran_clone.store(true, Ordering::SeqCst);
                        }),
                    )
                    .await
                    .unwrap();
                txs.push(tx);
            }

            // 并发取消所有事务，同时 runner 也在出队执行。
            let mut handles = Vec::new();
            for tx in &txs {
                let cancel_mgr = mgr.clone();
                let cancel_tx = tx.clone();
                handles.push(tokio::spawn(async move {
                    cancel_mgr.cancel(cancel_tx.id, 1000).await
                }));
            }
            let results: Vec<Result<(), CancelError>> = {
                let mut results = Vec::with_capacity(handles.len());
                for h in handles {
                    results.push(h.await.unwrap());
                }
                results
            };

            // 等 runner 处理完所有事务。
            wait_until(|| async { mgr.list().await.is_empty() }).await;

            // 不变量：cancel 返回 Ok 的事务，任务绝不能执行。
            for (i, result) in results.iter().enumerate() {
                if result.is_ok() {
                    assert!(
                        !ran[i].load(Ordering::SeqCst),
                        "round {round}: tx{} cancel returned Ok but task executed",
                        i + 1
                    );
                }
            }
        }
    }

    /// 队列有界：全局上限与每用户配额在入队时强制，超限拒绝入队。
    #[tokio::test]
    async fn enqueue_respects_limits() {
        // 上限 5，每用户 2。
        let mgr = TransactionManager::with_limits(5, 2);
        let (block_tx, _block_rx) = mpsc::unbounded_channel::<()>();
        let (release_tx, mut release_rx) = mpsc::unbounded_channel::<()>();

        // t1 阻塞在任务里，让后续事务在队列中排队。
        let t1 = mgr
            .enqueue(
                None,
                1,
                TransactionRole::Refresh,
                "c1".into(),
                1000,
                Box::pin(async move {
                    let _ = block_tx.send(());
                    let _ = release_rx.recv().await; // 等测试释放
                }),
            )
            .await
            .unwrap();
        wait_until_state(&mgr, t1.id, TransactionState::Running).await;

        // 同一 uid 排满配额（2 个排队中）。
        for i in 2..=3u64 {
            mgr.enqueue(
                None,
                i,
                TransactionRole::UpdatesList,
                "alice".into(),
                1000,
                Box::pin(async move {}),
            )
            .await
            .unwrap();
        }
        // 第 3 个排队事务超配额（全局上限 5 未满，先命中配额检查）。
        assert!(matches!(
            mgr.enqueue(
                None,
                4,
                TransactionRole::UpdatesList,
                "alice".into(),
                1000,
                Box::pin(async move {}),
            )
            .await,
            Err(EnqueueError::QuotaExceeded)
        ));
        // 其他 uid 不受 alice 配额影响，可继续入队。
        for (i, uid) in [(5u64, 1001u32), (6, 1002)] {
            mgr.enqueue(
                None,
                i,
                TransactionRole::Simulate,
                "other".into(),
                uid,
                Box::pin(async move {}),
            )
            .await
            .unwrap();
        }
        // 全局上限（1 running + 4 queued = 5）已满，任何 uid 都被拒绝。
        assert!(matches!(
            mgr.enqueue(
                None,
                7,
                TransactionRole::Simulate,
                "dave".into(),
                1003,
                Box::pin(async move {}),
            )
            .await,
            Err(EnqueueError::QueueFull)
        ));

        // 释放 t1：runner 依次执行排队事务，队列腾出空间后可再入队。
        let _ = release_tx.send(());
        wait_until(|| async { mgr.list().await.is_empty() }).await;
        mgr.enqueue(
            None,
                8,
            TransactionRole::UpdatesList,
            "alice".into(),
            1000,
            Box::pin(async move {}),
        )
        .await
        .unwrap();
        wait_until(|| async { mgr.list().await.is_empty() }).await;
    }

    /// 回归测试：出队与装进 running 槽原子完成，任何时刻在飞事务数
    /// （queue + running）不超过全局上限。
    ///
    /// 旧实现里 pop_front 释放 queue 锁后才装 running，窗口期事务同时
    /// 不在 queue 也不在 running：enqueue 的在飞计数少算一个，可能超限
    /// 入队；GetTransactionList 短暂看不到它；cancel 误报 NotFound。
    /// 该窗口只有微秒级，压测不保证必现旧 bug，但持续验证不变量。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn in_flight_never_exceeds_global_limit() {
        for _ in 0..30 {
            // 上限 3，每用户配额足够大。
            let mgr = TransactionManager::with_limits(3, 100);
            let (block_tx, _block_rx) = mpsc::unbounded_channel::<()>();
            let (release_tx, mut release_rx) = mpsc::unbounded_channel::<()>();

            // t1 阻塞在任务里，占用 running 槽。
            mgr.enqueue(
                None,
                1,
                TransactionRole::Refresh,
                "c1".into(),
                1000,
                Box::pin(async move {
                    let _ = block_tx.send(());
                    let _ = release_rx.recv().await; // 等测试释放
                }),
            )
            .await
            .unwrap();
            wait_until_state(&mgr, 1, TransactionState::Running).await;

            // 并发入队填满队列（上限 3 = 1 running + 2 queued，最多 2 个成功）。
            let mut fill = Vec::new();
            for i in 0..8 {
                let m = mgr.clone();
                fill.push(tokio::spawn(async move {
                    m.enqueue(
                        None,
                        i + 2,
                        TransactionRole::UpdatesList,
                        "alice".into(),
                        1000,
                        Box::pin(async move {}),
                    )
                    .await
                    .is_ok()
                }));
            }
            let mut admitted = 0usize;
            for h in fill {
                if h.await.unwrap() {
                    admitted += 1;
                }
            }
            assert!(admitted <= 2, "admitted {admitted} with capacity 2");

            // 释放 t1，同时并发入队与 runner 出队竞争；采样器记录
            // 可见在飞数峰值，不得超上限。
            let sampler_mgr = mgr.clone();
            let sampler = tokio::spawn(async move {
                let mut peak = 0usize;
                for _ in 0..200 {
                    let n = sampler_mgr.list().await.len();
                    peak = peak.max(n);
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                peak
            });
            let mut racers = Vec::new();
            for i in 0..16 {
                let m = mgr.clone();
                racers.push(tokio::spawn(async move {
                    m.enqueue(
                        None,
                        100 + i,
                        TransactionRole::UpdatesList,
                        "alice".into(),
                        1000,
                        Box::pin(async move {}),
                    )
                    .await
                    .is_ok()
                }));
            }
            let _ = release_tx.send(());
            for h in racers {
                let _ = h.await.unwrap();
            }
            let peak = sampler.await.unwrap();
            assert!(peak <= 3, "peak in-flight {peak} exceeds limit 3");
            wait_until(|| async { mgr.list().await.is_empty() }).await;
        }
    }
}
