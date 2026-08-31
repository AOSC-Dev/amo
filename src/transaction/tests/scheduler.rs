//! 调度器（`TransactionManager` + runner）的单元测试。

use crate::transaction::{
    CancelError, EnqueueError, Transaction, TransactionManager, TransactionRole, TransactionState,
    failure_result_json, panic_detail,
};
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Mutex, mpsc};

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

/// 任务 panic 时：失败结果 JSON 形状正确（type=result, status=Failed,
/// 无 result 字段，客户端 wait_result 可收到）；panic 消息可从 String
/// 与 &str 载荷提取；runner 不崩、后续事务照常执行。
#[tokio::test]
async fn panicking_task_emits_failure_result_and_runner_survives() {
    // 直接构造事务测 JSON 形状（emitter 为 None 时不发信号）。
    let tx = Transaction {
        id: 7,
        role: TransactionRole::ApplyChanges,
        state: Mutex::new(TransactionState::Queued),
        cancelled: AtomicBool::new(false),
        caller: "tester".into(),
        uid: 0,
        created_at: 0,
        emitter: None,
        task: Mutex::new(None),
        on_done: None,
    };
    let json = failure_result_json(&tx, "boom".into()).unwrap();
    let v: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(v["type"], "result");
    assert_eq!(v["transaction_id"], 7);
    assert_eq!(v["role"], "apply_changes");
    assert_eq!(v["status"], serde_json::json!({ "Failed": "boom" }));
    assert!(
        v.get("result").is_none(),
        "panic failure must have no result payload"
    );

    // panic_detail：String 载荷（panic!(format!())）与 &str 载荷（panic!("literal")）。
    let msg_str = panic_detail(
        tokio::task::spawn(async { panic!("{}", "kaboom") })
            .await
            .unwrap_err(),
    );
    assert_eq!(msg_str, "kaboom");
    let msg_lit = panic_detail(
        tokio::task::spawn(async { panic!("literal-boom") })
            .await
            .unwrap_err(),
    );
    assert_eq!(msg_lit, "literal-boom");

    // runner 韧性：panic 任务后 runner 继续处理后续任务，事务都到终态。
    let mgr = TransactionManager::new();
    let done = Arc::new(AtomicU64::new(0));
    let done_clone = done.clone();
    mgr.enqueue(
        None,
        1,
        TransactionRole::UpdatesList,
        "tester".into(),
        0,
        Box::pin(async { panic!("intentional panic") }),
        None,
    )
    .await
    .unwrap();
    mgr.enqueue(
        None,
        2,
        TransactionRole::UpdatesList,
        "tester".into(),
        0,
        Box::pin(async move {
            done_clone.fetch_add(1, Ordering::SeqCst);
        }),
        None,
    )
    .await
    .unwrap();
    wait_until(|| async { done.load(Ordering::SeqCst) == 1 }).await;
    wait_until(|| async { mgr.list().await.is_empty() }).await;
    assert_eq!(done.load(Ordering::SeqCst), 1);
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
        None,
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
        None,
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
            None,
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
            None,
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
            None,
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
    // 已取消的事务已从队列移除，再取消返回 NotFound（不再占用配额）。
    assert_eq!(mgr.cancel(t2.id, 1000).await, Err(CancelError::NotFound));

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
            None,
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
            None,
        )
        .await
        .unwrap();

    // 其他用户（不同 uid）不能取消。
    assert_eq!(mgr.cancel(t2.id, 1001).await, Err(CancelError::NotOwner));
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
            None,
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

/// 回归测试：取消立即释放队列槽位与配额。
///
/// 旧实现只标记取消、不移除条目：被取消的事务继续占用全局上限与
/// 每用户配额，任务与 on_done 也一直保留到 runner 排到它——长任务
/// 期间多个成功取消的请求会让后续请求一直 LimitsExceeded。修复后
/// cancel 在 queue 锁内移除条目，锁外发信号 + 触发 on_done。
#[tokio::test]
async fn cancel_frees_queue_slot_immediately() {
    // 上限 3（1 running + 2 queued），每用户 2。
    let mgr = TransactionManager::with_limits(3, 2);
    let (block_tx, _block_rx) = mpsc::unbounded_channel::<()>();
    let (release_tx, mut release_rx) = mpsc::unbounded_channel::<()>();

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
            None,
        )
        .await
        .unwrap();
    wait_until_state(&mgr, t1.id, TransactionState::Running).await;

    let done = Arc::new(AtomicU64::new(0));
    let done_clone = done.clone();
    let t2 = mgr
        .enqueue(
            None,
            2,
            TransactionRole::UpdatesList,
            "alice".into(),
            1000,
            Box::pin(async move {}),
            Some(Arc::new(move || {
                done_clone.fetch_add(1, Ordering::SeqCst);
            })),
        )
        .await
        .unwrap();
    let _t3 = mgr
        .enqueue(
            None,
            3,
            TransactionRole::UpdatesList,
            "alice".into(),
            1000,
            Box::pin(async move {}),
            None,
        )
        .await
        .unwrap();

    // 队列已满：新入队被拒。
    assert!(matches!(
        mgr.enqueue(
            None,
            4,
            TransactionRole::UpdatesList,
            "alice".into(),
            1000,
            Box::pin(async move {}),
            None,
        )
        .await,
        Err(EnqueueError::QueueFull)
    ));

    // 取消 t2：立即从队列移除、释放配额、触发 on_done。
    assert_eq!(mgr.cancel(t2.id, 1000).await, Ok(()));
    assert_eq!(done.load(Ordering::SeqCst), 1, "on_done must run on cancel");
    // 列表里不再有 t2。
    assert!(!mgr.list().await.iter().any(|t| t.transaction_id == t2.id));
    // 槽位立即释放：t1 仍在运行，但队列只剩 t3，可再入队一个。
    mgr.enqueue(
        None,
        4,
        TransactionRole::UpdatesList,
        "alice".into(),
        1000,
        Box::pin(async move {}),
        None,
    )
    .await
    .unwrap();

    // 释放 t1；runner 依次执行 t3、t4。
    let _ = release_tx.send(());
    wait_until(|| async { mgr.list().await.is_empty() }).await;
    // t2 的 on_done 只被 cancel 触发一次（runner 不会再碰到它）。
    assert_eq!(done.load(Ordering::SeqCst), 1);
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
                    None,
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
            None,
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
            None,
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
            None,
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
            None,
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
            None,
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
        None,
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
            None,
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
                    None,
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
                    None,
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

/// 回归测试：list() 的快照不得把同一事务计两次。
///
/// 旧实现先克隆 queue、释放 queue 锁，再读 running 槽；runner 是持
/// queue 锁把队头移进 running 的，若恰好在这两步之间出队，list()
/// 就会把该事务既算在队列里又算在 running 里——GetTransactionList
/// 出现重复记录，在飞采样器也会间歇性数到超过上限。修复后快照与
/// running 读取在同一把 queue 锁内完成，重复不可能出现。
///
/// 窗口极窄，单次很难命中；这里用 feeder 持续入队 + runner 持续
/// 出队 + 采样器紧循环（无 sleep）长时间对撞，断言不变量。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_never_duplicates_transactions() {
    for round in 0..10 {
        // 上限 4（1 running + 3 queued），任务即时完成，runner 高速出队。
        let mgr = TransactionManager::with_limits(4, 100);
        let stop = Arc::new(AtomicBool::new(false));

        // feeder：持续入队，队列满则重试。
        let feeder_mgr = mgr.clone();
        let feeder_stop = stop.clone();
        let feeder = tokio::spawn(async move {
            let mut i = 0u64;
            while !feeder_stop.load(Ordering::SeqCst) {
                let id = i;
                i += 1;
                let _ = feeder_mgr
                    .enqueue(
                        None,
                        id,
                        TransactionRole::UpdatesList,
                        "alice".into(),
                        1000,
                        Box::pin(async move {}),
                        None,
                    )
                    .await;
            }
        });

        // 采样器：紧循环 list()，记录峰值并检测重复 id。
        let sampler_mgr = mgr.clone();
        let sampler_stop = stop.clone();
        let sampler = tokio::spawn(async move {
            let mut peak = 0usize;
            let mut dups = 0usize;
            while !sampler_stop.load(Ordering::SeqCst) {
                let txs = sampler_mgr.list().await;
                peak = peak.max(txs.len());
                let mut ids: Vec<u64> = txs.iter().map(|t| t.transaction_id).collect();
                ids.sort_unstable();
                if ids.windows(2).any(|w| w[0] == w[1]) {
                    dups += 1;
                    // 命中一次即可；继续采样以同时统计峰值。
                }
            }
            (peak, dups)
        });

        // 让三者竞争一段时间，然后停止。
        tokio::time::sleep(Duration::from_millis(300)).await;
        stop.store(true, Ordering::SeqCst);
        let _ = feeder.await;
        let (peak, dups) = sampler.await.unwrap();
        assert_eq!(
            dups, 0,
            "round {round}: list() returned a duplicate transaction {dups} times"
        );
        assert!(
            peak <= 4,
            "round {round}: peak in-flight {peak} exceeds limit 4"
        );
    }
}

/// 回归测试：list() 不得返回终态（Finished/Cancelled）——API 承诺
/// 只返回 queued + running。
///
/// 旧实现克隆 queue/running 条目后释放调度器锁，再逐个读 state：
/// 排队事务可能在读之前被 cancel 置为 Cancelled、运行事务可能在读
/// 之前变成 Finished，违反承诺。修复后在持有 queue/running 锁时读
/// state（锁内排队事务恒为 Queued；running 槽内事务至多 Running，
/// 不会读到终态）。
///
/// 窗口极窄，单次很难命中；这里用 feeder 入队 + 取消器并发取消 +
/// 长任务占 running 槽并延迟释放 + 采样器紧循环对撞，断言不变量。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_never_returns_terminal_states() {
    for _ in 0..10 {
        let mgr = TransactionManager::with_limits(8, 100);
        let (block_tx, _block_rx) = mpsc::unbounded_channel::<()>();
        let (release_tx, mut release_rx) = mpsc::unbounded_channel::<()>();
        // 长任务占住 running 槽；稍后释放，让排队事务快速完成。
        mgr.enqueue(
            None,
            1,
            TransactionRole::Refresh,
            "c".into(),
            0,
            Box::pin(async move {
                let _ = block_tx.send(());
                let _ = release_rx.recv().await;
            }),
            None,
        )
        .await
        .unwrap();

        // 采样器：紧循环 list()，断言任何条目都不是终态。
        let sampler_mgr = mgr.clone();
        let sampler = tokio::spawn(async move {
            for _ in 0..300 {
                for t in sampler_mgr.list().await {
                    assert!(
                        !matches!(
                            t.state,
                            TransactionState::Finished | TransactionState::Cancelled
                        ),
                        "list returned terminal state {:?} for tx {}",
                        t.state,
                        t.transaction_id
                    );
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });

        // 取消器：反复取消排队事务，制造 queued -> cancelled 窗口。
        let cancel_mgr = mgr.clone();
        let canceller = tokio::spawn(async move {
            for _ in 0..50 {
                let list = cancel_mgr.list().await;
                for t in list {
                    if matches!(t.state, TransactionState::Queued) {
                        let _ = cancel_mgr.cancel(t.transaction_id, 0).await;
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });

        // feeder：持续入队（即时完成的任务），制造 running -> finished。
        let feeder_mgr = mgr.clone();
        let feeder = tokio::spawn(async move {
            for i in 0..40u64 {
                let _ = feeder_mgr
                    .enqueue(
                        None,
                        i + 2,
                        TransactionRole::Simulate,
                        "c".into(),
                        0,
                        Box::pin(async {}),
                        None,
                    )
                    .await;
            }
        });

        // 延迟释放长任务，让排队任务快速完成。
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = release_tx.send(());
        feeder.await.unwrap();
        sampler.await.unwrap();
        canceller.await.unwrap();
        wait_until(|| async { mgr.list().await.is_empty() }).await;
    }
}
