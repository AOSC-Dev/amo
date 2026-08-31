//! 活动事务注册表（claim 生命周期 / 清扫器 / 取消与销毁）的单元测试。

use crate::auth::cancel_authorization;
use crate::transaction::live::{
    CLAIM_TIMEOUT, DORMANT_TIMEOUT, LiveTransaction, StartedClaim, check_claim_still_active,
    claim_expired, claim_still_abandoned, dormant_expired, next_cancellation_id,
    next_claim_generation, remove_for_destroy, rollback_claim_if_not_enqueued,
};
use crate::transaction::TransactionManager;
use crate::transaction::TransactionRole;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use zbus::Connection;

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
            claim_generation: format!("amo-{id}-0"),
            cancellation_id: None,
            started: true,
        },
    );
    live
}

#[tokio::test]
async fn claim_rollback_clears_started() {
    let live = live_with(1);
    let mut claim = StartedClaim::new(live.clone(), 1, "amo-1-0".into());
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
            claim_generation: "amo-1-0".into(),
            cancellation_id: Some("amo-1-0".into()),
            started: true,
        },
    );
    let mut claim = StartedClaim::new(live.clone(), 1, "amo-1-0".into());
    claim.rollback().await;
    let guard = live.lock().await;
    let e = guard.get(&1).unwrap();
    assert!(!e.started);
    assert!(e.claimed_at.is_none());
    assert!(
        e.claim_generation.is_empty(),
        "rolled-back claim must not keep a stale claim_generation"
    );
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
    let mgr = TransactionManager::with_limits(10, 10);

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
            claim_generation: "amo-1-0".into(),
            cancellation_id: Some("amo-1-0".into()),
            started: true,
        },
    );
    let outcome = rollback_claim_if_not_enqueued(&mut live, &mgr, 1).await.unwrap();
    assert!(outcome.rolled_back, "cancel must roll back the claim");
    assert_eq!(outcome.cancellation_id.as_deref(), Some("amo-1-0"));
    let e = live.get(&1).unwrap();
    assert!(!e.started, "cancel must roll back the claim");
    assert!(e.claimed_at.is_none());
    assert!(
        e.claim_generation.is_empty(),
        "cancel must clear the stale claim_generation"
    );
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
        TransactionRole::Simulate,
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
            claim_generation: "amo-42-0".into(),
            cancellation_id: Some("amo-42-0".into()),
            started: true,
        },
    );
    let outcome = rollback_claim_if_not_enqueued(&mut live, &mgr, 42).await.unwrap();
    assert!(
        !outcome.rolled_back,
        "enqueued transaction must not be rolled back by cancel"
    );
    assert!(live.get(&42).unwrap().started);
    let _ = release_tx.send(());
}

/// 回归：Simulate/UpdatesList 无 polkit cancellation_id（None），Cancel
/// 在 claim 后、入队前到达时回滚成功但 cid 为 None——rolled_back 必须
/// 为 true，调用方据此返回成功，而不是落入 manager.cancel 报
/// UnknownObject（尽管任务已被阻止）。
#[tokio::test]
async fn cancel_rollback_without_cancellation_id_reports_success() {
    let mgr = TransactionManager::with_limits(10, 10);
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
            // Simulate/UpdatesList：无授权，claim 没有 cancellation_id，
            // 但代际独立分配（非空），Cancel 回滚后重试可区分新旧调用。
            claim_generation: "amo-1-0".into(),
            cancellation_id: None,
            started: true,
        },
    );
    let outcome = rollback_claim_if_not_enqueued(&mut live, &mgr, 1).await.unwrap();
    assert!(
        outcome.rolled_back,
        "rollback without a cancellation id must still report success"
    );
    assert!(
        outcome.cancellation_id.is_none(),
        "no cancellation id to carry out"
    );
    let e = live.get(&1).unwrap();
    assert!(!e.started, "claim must be cleared");
    assert!(e.claimed_at.is_none());
    assert!(
        e.claim_generation.is_empty(),
        "rollback must clear the claim generation"
    );
}

/// 授权等待中（claimed-but-not-enqueued）的 Destroy：移除条目并带出
/// cancellation_id；dormant 对象照常移除（无 cancellation_id）；已入队
/// 拒绝（Failed）；条目不存在报 UnknownObject。
#[tokio::test]
async fn destroy_removes_claim_but_not_enqueued() {
    let mgr = TransactionManager::with_limits(10, 10);

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
            claim_generation: "amo-1-0".into(),
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
            claim_generation: String::new(),
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
        TransactionRole::Simulate,
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
            claim_generation: "amo-42-0".into(),
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

/// begin 授权成功后、入队前的复查：条目存在、started、且 claim 仍是
/// 本调用的那一代（claim_generation 一致）才放行；被并发 Cancel 回滚
/// （started=false）、Destroy 移除（条目缺失）、或 Cancel 后立即重试
/// （claim_generation 被替换）都中止入队。
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
            claim_generation: "amo-1-0".into(),
            cancellation_id: Some("amo-1-0".into()),
            started: true,
        },
    );
    // 本调用的 claim 仍活跃（claim_generation 一致）→ 放行。
    assert!(check_claim_still_active(&live, 1, "amo-1-0").is_ok());

    // 被 Cancel 回滚：started=false → 中止入队。
    live.get_mut(&1).unwrap().started = false;
    assert!(matches!(
        check_claim_still_active(&live, 1, "amo-1-0"),
        Err(zbus::fdo::Error::Failed(_))
    ));

    // 被 Destroy 移除：条目缺失 → UnknownObject。
    live.remove(&1);
    assert!(matches!(
        check_claim_still_active(&live, 1, "amo-1-0"),
        Err(zbus::fdo::Error::UnknownObject(_))
    ));
}

/// 竞态回归：Cancel 回滚后调用者立即重试（新 claim、新代际），旧调用
/// 的授权成功时复查必须拒绝——否则旧调用把 Cancel 已报告取消的操作
/// 入队，与重试操作同 ID 排队，第一个完成移除对象后重复项仍活跃。
/// 覆盖 Simulate/UpdatesList：无 polkit cancellation_id，代际独立分配
/// 才能区分新旧调用。
#[test]
fn begin_enqueue_recheck_rejects_replaced_claim() {
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
            // 重试的新 claim 已写入新代际（Simulate：无 cancellation_id）。
            claim_generation: "amo-1-1".into(),
            cancellation_id: None,
            started: true,
        },
    );
    // 旧调用（claim_generation "amo-1-0"）复查：started 为真但代际不符
    // → 拒绝，不把已取消的操作入队。
    assert!(matches!(
        check_claim_still_active(&live, 1, "amo-1-0"),
        Err(zbus::fdo::Error::Failed(_))
    ));
    // 新调用（claim_generation "amo-1-1"）复查：放行。
    assert!(check_claim_still_active(&live, 1, "amo-1-1").is_ok());
}

#[tokio::test]
async fn claim_commit_keeps_started() {
    let live = live_with(1);
    let mut claim = StartedClaim::new(live.clone(), 1, "amo-1-0".into());
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
        let _claim = StartedClaim::new(live.clone(), 1, "amo-1-0".into());
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
    let mut claim = StartedClaim::new(live.clone(), 1, "amo-1-0".into());
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
        e.claim_generation = "amo-1-1".into();
        e.dormant_since = None;
    }

    // Cancel 回滚（rollback_claim_if_not_enqueued）→ 同样重置。
    let mgr = TransactionManager::with_limits(10, 10);
    let mut map = live.try_lock().unwrap();
    let outcome = rollback_claim_if_not_enqueued(&mut map, &mgr, 1).await.unwrap();
    assert!(outcome.rolled_back);
    assert!(outcome.cancellation_id.is_none());
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

/// 代际校验：Cancel 回滚旧 claim 后用户 re-trigger（新 claim、新代际），
/// 旧 begin 的授权 future 最终失败时其 rollback 不得清掉新 claim——
/// 否则新事务被旧 future 误杀。覆盖 Simulate/UpdatesList：无 polkit
/// cancellation_id，代际独立分配才能区分新旧调用。
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
            claim_generation: "amo-1-0".into(),
            cancellation_id: Some("amo-1-0".into()),
            started: true,
        },
    );

    // 旧 claim（claim_generation "amo-1-0"）回滚：应清掉自己这一代。
    let mut old_claim = StartedClaim::new(live.clone(), 1, "amo-1-0".into());
    old_claim.rollback().await;
    {
        let guard = live.lock().await;
        let e = guard.get(&1).unwrap();
        assert!(!e.started);
        assert!(e.claimed_at.is_none());
        assert!(e.claim_generation.is_empty());
        assert!(e.cancellation_id.is_none());
    }

    // 用户 re-trigger：新 claim（claim_generation "amo-1-1"）。
    {
        let mut guard = live.lock().await;
        let e = guard.get_mut(&1).unwrap();
        e.started = true;
        e.claimed_at = Some(Instant::now());
        e.claim_generation = "amo-1-1".into();
        e.cancellation_id = Some("amo-1-1".into());
    }

    // 旧 begin 的授权 future 最终失败 → 旧 claim 再次 rollback（模拟
    // 旧 future 的 Drop 或显式 rollback 迟到）：不得清掉新 claim。
    let mut stale = StartedClaim::new(live.clone(), 1, "amo-1-0".into());
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
            e.claim_generation.as_str(),
            "amo-1-1",
            "stale rollback must not clear the fresh claim's claim_generation"
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
    let mgr = TransactionManager::with_limits(10, 10);
    // 无会话总线（headless CI/容器，无 DBUS_SESSION_BUS_ADDRESS）时跳过，
    // 而不是 panic 拖垮整个测试套件——判定谓词本身不依赖真实总线，
    // 只有 name_has_owner 的 live-sender 分支需要。
    let Ok(conn) = Connection::session().await else {
        eprintln!("no session bus, skipping");
        return;
    };
    let Ok(dbus) = zbus::fdo::DBusProxy::new(&conn).await else {
        eprintln!("no session bus, skipping");
        return;
    };

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
        TransactionRole::Simulate,
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

/// 清扫器 phase 3 的移除判定：claim 被回滚后重新声明（claimed_at 变）
/// 的新声明不删；claimed_at 未变且未入队才删；已入队即使 claimed_at
/// 未变也不删（begin 在 live 锁内入队，与清扫器互斥）。
#[tokio::test]
async fn expired_claim_not_removed_after_fresh_retry() {
    let mgr = TransactionManager::with_limits(10, 10);
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
        TransactionRole::Simulate,
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

/// claim 代际：每次调用（无论是否需授权）都生成唯一且非空的值——
/// Simulate/UpdatesList 无 polkit cancellation_id，代际必须独立分配，
/// 否则 Cancel 回滚后立即重试时新旧调用无法区分。
#[test]
fn claim_generations_are_unique_and_nonempty() {
    let a = next_claim_generation(1);
    let b = next_claim_generation(1);
    assert!(!a.is_empty() && !b.is_empty());
    assert!(a.starts_with("amo-1-"), "unexpected generation {a}");
    assert_ne!(a, b, "counter must yield unique generations");
}

/// 对未知 cancellation_id 调用取消是无操作（验证
/// CancelCheckAuthorization 的 D-Bus 线路可用且不会报错）。
#[tokio::test]
async fn cancel_unknown_polkit_check_is_noop() {
    let Ok(conn) = Connection::system().await else {
        eprintln!("no system bus, skipping");
        return;
    };
    cancel_authorization(&conn, "amo-test-does-not-exist").await;
}