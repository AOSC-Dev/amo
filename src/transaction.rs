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

/// PackageKit 风格的事务调度器：FIFO 队列 + 单一 runner 串行执行。
pub struct TransactionManager {
    /// 等待执行的事务队列（FIFO）
    queue: Mutex<VecDeque<Arc<Transaction>>>,
    /// 当前正在执行的事务
    running: Mutex<Option<Arc<Transaction>>>,
    /// 唤醒 runner
    notify: Notify,
    /// TransactionState 信号的发射目标（首次 enqueue 时设置，之后忽略）。
    emitter: OnceLock<SignalEmitter<'static>>,
}

impl TransactionManager {
    /// 创建管理器并启动 runner。
    pub fn new() -> Arc<Self> {
        let mgr = Arc::new(Self {
            queue: Mutex::new(VecDeque::new()),
            running: Mutex::new(None),
            notify: Notify::new(),
            emitter: OnceLock::new(),
        });
        let runner = mgr.clone();
        tokio::spawn(runner.run());
        mgr
    }

    /// 把任务排进队列，返回对应的事务；`ctxt` 用来广播状态信号，
    /// 不需要发信号时（如测试）传 `None`。
    pub async fn enqueue(
        &self,
        ctxt: impl Into<Option<SignalEmitter<'static>>>,
        id: u64,
        role: TransactionRole,
        caller: String,
        uid: u32,
        task: Task,
    ) -> Arc<Transaction> {
        if let Some(ctxt) = ctxt.into() {
            let _ = self.emitter.get_or_init(|| ctxt);
        }

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
        self.emit_event(&tx, TransactionState::Queued).await;
        self.queue.lock().await.push_back(tx.clone());
        self.notify.notify_one();

        tx
    }

    /// 取消一个仍在排队中的事务。运行中或已结束的事务不可取消。
    /// 只有事务所有者（uid 匹配）或 root（uid 0）可以取消，
    /// 防止其他用户取消已通过 polkit 授权的 ApplyChanges 等事务。
    /// 失败时返回具体原因（见 [`CancelError`]）。
    ///
    /// 注意：所有权只按 uid 判断，不比较 caller——caller 是 D-Bus
    /// unique name，每次连接都会变，不能作为稳定身份。
    pub async fn cancel(&self, id: u64, uid: u32) -> Result<(), CancelError> {
        // 先在队列里找到目标，克隆后释放队列锁，避免持锁跨 await。
        let target = {
            let queue = self.queue.lock().await;
            queue.iter().find(|tx| tx.id == id).cloned()
        };
        let Some(tx) = target else {
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
            let next = self.queue.lock().await.pop_front();
            let Some(tx) = next else {
                self.notify.notified().await;
                continue;
            };

            // 排队期间被取消：不执行任务，直接丢弃（cancel 已发 Cancelled 事件）。
            if tx.cancelled.load(Ordering::SeqCst) {
                continue;
            }

            *self.running.lock().await = Some(tx.clone());
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
        .await;

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
        .await;

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
            .await;
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
            .await;
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
            .await;

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
            .await;
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
            .await;

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
            .await;
        assert_eq!(mgr.cancel(t3.id, 0).await, Ok(()));
        // 不存在的事务返回 NotFound。
        assert_eq!(mgr.cancel(999, 1000).await, Err(CancelError::NotFound));

        // 释放 t1；runner 应跳过被取消的 t2/t3。
        let _ = release_tx.send(());
        wait_until(|| async { mgr.list().await.is_empty() }).await;
        assert!(!ran.load(Ordering::SeqCst));
    }
}
