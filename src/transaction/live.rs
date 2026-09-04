//! 活动事务对象（Transaction 对象）的注册表与生命周期管理。
//!
//! 这个模块维护"当前有哪些事务对象存在"以及它们的生命周期状态。三个核心
//! 部件：
//! - `LiveTransaction`：注册表里的一条记录，对应一个事务 D-Bus 对象。
//! - `StartedClaim`：事务开始（`begin` 声明）的守卫，确保声明失败后回滚。
//! - `reclaim_dormant`：周期清扫器，回收超时 / 被放弃的对象。
//!
//! 所有对注册表的修改——无论是声明、销毁还是清扫——都共用同一把 live
//! 锁，同一时刻最多只有一方在改动它，从而避免竞态。

use crate::auth::cancel_authorization;
use crate::transaction::TransactionManager;
use crate::transaction::object::TransactionObject;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use tracing::error;
use zbus::{Connection, object_server::ObjectServer};

/// 同时存在的活动事务对象上限（含休眠尚未启动的）。
///
/// 防止 `CreateTransaction` 被无限调用、耗尽对象服务器内存。超限创建会
/// 返回 `LimitsExceeded`。
pub(crate) const MAX_LIVE_TRANSACTIONS: usize = 64;

/// 单个 uid 同时拥有的活动事务对象上限。
///
/// 休眠对象不占队列名额，可被无限创建；这个配额防止单个用户把全局
/// 64 个槽位全部占满，让其他人无法创建事务。
pub(crate) const MAX_LIVE_PER_UID: usize = 16;

/// 休眠（尚未启动）事务对象的回收超时。
///
/// 对象创建后超过 `DORMANT_TIMEOUT` 的时间后仍未被 `begin` 启动，就被清扫器移除。
pub(crate) const DORMANT_TIMEOUT: Duration = Duration::from_secs(60);

/// 已声明启动（`started`）但迟迟未入队（授权等待中）对象的回收超时。
///
/// 比休眠超时长得多，给真实授权弹窗留足时间；但必须有上限，否则未授权
/// 调用者可以对全部 16 个对象并发调用 `ApplyChanges` 并保持连接，让这些
/// 声明绕过休眠超时、长期占住槽位——同 uid 的其他调用全被 `LimitsExceeded`，
/// 多个 uid 可耗尽全局 64 个槽位。
pub(crate) const CLAIM_TIMEOUT: Duration = Duration::from_secs(300);

/// PolicyKit `CheckAuthorization` 的 cancellation_id 计数器。
///
/// 每个授权检查要有唯一的非空 ID，才能通过 `CancelCheckAuthorization`
/// 显式取消这个远程检查（空 ID 的远程检查不可取消）。
static NEXT_CANCEL_ID: AtomicU64 = AtomicU64::new(0);

/// 生成一个唯一且非空的 PolicyKit cancellation_id。
///
/// 计数器单调递增，保证进程内唯一；前缀带事务 id，便于排查是哪个事务
/// 触发的检查。
pub(crate) fn next_cancellation_id(tx_id: u64) -> String {
    format!(
        "amo-{tx_id}-{}",
        NEXT_CANCEL_ID.fetch_add(1, Ordering::Relaxed)
    )
}

/// claim 编号（generation）计数器。
///
/// 每次 `begin` 调用（无论是否要授权）都分到一个唯一编号，用来区分同一
/// 事务对象上"Cancel 回滚后立刻重试"的新旧两拨调用。
///
/// 为什么不用 polkit cancellation_id 当编号？因为 `Simulate` 和
/// `UpdatesList` 没有 polkit 授权，`cancellation_id` 是 `None`。若拿它
/// 当编号，`None` 会让新旧调用无法区分：旧调用可能把已被取消的操作
/// 入队，或回滚时清掉新 claim。
static NEXT_CLAIM_GENERATION: AtomicU64 = AtomicU64::new(0);

/// 生成一个唯一的 claim 编号。计数器单调递增保证进程内唯一，前缀带
/// 事务 id 便于排查。
pub(crate) fn next_claim_generation(tx_id: u64) -> String {
    format!(
        "amo-{tx_id}-{}",
        NEXT_CLAIM_GENERATION.fetch_add(1, Ordering::Relaxed)
    )
}

/// 活动事务对象注册表的一条记录，对应一个事务 D-Bus 对象。
pub(crate) struct LiveTransaction {
    /// 该事务对象的 D-Bus 路径，如 `/io/aosc/Amo/Transaction/123`。
    pub(crate) path: String,
    /// 创建者（发起事务的用户）的 uid，用于每用户配额检查。
    pub(crate) uid: u32,
    /// 创建者连接的 unique name。
    ///
    /// 对象被创建者锁定，只有这条连接能操作它。清扫器用这个字段判断
    /// "已声明但从未入队"的对象是否已被放弃：连接已死 ⇒ 操作永远无法
    /// 继续 ⇒ 可回收。
    pub(crate) sender: String,
    /// 对象创建的时刻，作为休眠超时的兜底基准。
    pub(crate) created_at: Instant,
    /// 对象处于休眠（未启动）状态的起始时刻。
    ///
    /// `Some(t)` = 自 `t` 起一直休眠；`None` = 已经声明启动（`started`
    /// 为 true，休眠计时不再运行）。
    ///
    /// 创建时和每次回滚（授权失败 / 入队失败 / Cancel）都会重置为当前
    /// 时刻。否则：一个早创建的对象（比如已经存在超过 `DORMANT_TIMEOUT`）
    /// 一旦回滚，下一个清扫周期就会立刻把它回收掉，导致"回滚后重试"
    /// 的窗口只有一个清扫间隔那么大。
    pub(crate) dormant_since: Option<Instant>,
    /// 声明启动（`begin` 把 `started` 置 true）的时刻。
    ///
    /// `None` = 休眠（还没被 `begin` 声明）。清扫器对"已声明但从未入队"
    /// 的对象会额外施加 `CLAIM_TIMEOUT` 上限，防止授权等待中的声明绕过
    /// 休眠超时、长期占住槽位。
    pub(crate) claimed_at: Option<Instant>,
    /// 本次声明（claim）的编号标记。
    pub(crate) claim_generation: String,
    /// 本次声明对应的 polkit `CheckAuthorization` cancellation_id。
    ///
    /// 仅需要授权的操作才有；与 `claim_generation` 分开存。当这个声明被
    /// 清扫器判定为"已放弃"（创建者断连或声明超时）时，用它显式取消
    /// 远程 polkit 检查。
    ///
    /// 为什么必须单独走这条路径？因为 `begin` 的本地超时只覆盖
    /// `TimedOut`；而创建者断连时 `begin` 的 future 不会被 zbus 取消，
    /// 只有清扫器能回收对象并取消远程检查。
    pub(crate) cancellation_id: Option<String>,
    /// 是否已通过 `begin` 声明启动。
    pub(crate) started: bool,
    /// 是否已交给 manager 排队执行。
    ///
    /// `begin` 授权成功后、`manager.enqueue` 成功时，在 live 锁内置为
    /// true（与入队原子可见），条目被 on_done 移除前一直保持。它用来
    /// 区分两类事务：
    /// - `enqueued = true`：已入队 / 正在运行 / 已结束但尚未清理；
    /// - `enqueued = false`：仅声明了 claim（还在等授权）。
    ///
    /// 为什么不能只看 `started` + `manager.contains`？因为 runner 清空
    /// running 槽到 on_done 移除条目之间，`contains` 会短暂为 false。
    /// 此时若把这个已执行的事务误判为"未入队的声明"，`Cancel` / `Destroy`
    /// 就会报成功——但 `ApplyChanges` 可能已经提交了；或者销毁掉一个
    /// 还没发终态信号的对象。
    pub(crate) enqueued: bool,
}

/// `begin` 的启动声明（lease）守卫。
///
/// `begin` 会在 live 锁内把对象标记为 `started`，然后持有这个守卫直到
/// 声明确认完成。它保证任何"还没入队就退出"的路径都会回滚 `started`——
/// 无论是因为授权失败、入队失败，还是客户端在 polkit 弹窗期间断开导致
/// `begin` 的 future 被取消。没有它，这些对象会停留在"已启动但从未入队"
/// 的状态，永久占住槽位（清扫器只回收休眠对象）。
pub(crate) struct StartedClaim {
    live: Arc<Mutex<HashMap<u64, LiveTransaction>>>,
    id: u64,
    /// 本代 claim 的编号标记。
    ///
    /// 回滚时只清"自己这一批"的 claim：Cancel 回滚后用户可能立刻重新
    /// 触发（得到新 claim、新编号）。如果旧 begin 的授权 future 最终失败
    /// 时无条件回滚，会把新 claim 一起清掉（新事务被旧 future 误杀）。
    /// 比对编号保证旧 future 的回滚只作用于自己声明的那一批。
    ///
    /// 编号必须独立于 polkit cancellation_id 分配：`Simulate` /
    /// `UpdatesList` 没有 cancellation_id。
    claim_generation: String,
    /// 是否仍处于"声明进行中"状态。true 时 Drop 要回滚；commit 后为 false。
    armed: bool,
}

impl StartedClaim {
    pub(crate) fn new(
        live: Arc<Mutex<HashMap<u64, LiveTransaction>>>,
        id: u64,
        claim_generation: String,
    ) -> Self {
        Self {
            live,
            id,
            claim_generation,
            armed: true,
        }
    }

    /// 在已知失败路径上立即回滚声明。
    pub(crate) async fn rollback(&mut self) {
        if self.armed {
            self.armed = false;
            if let Some(t) = self.live.lock().await.get_mut(&self.id)
                && t.claim_generation == self.claim_generation
            {
                t.started = false;
                t.claimed_at = None;
                t.claim_generation.clear();
                t.cancellation_id = None;
                // 回到休眠：休眠计时从回滚时刻重新起算。否则一个早
                // 创建的对象回滚后，下一个清扫周期就会立刻回收它，
                // 重试窗口只剩一个清扫间隔。
                t.dormant_since = Some(Instant::now());
            }
        }
    }

    /// 入队成功后调用：声明已完成，Drop 不再回滚。
    pub(crate) fn commit(&mut self) {
        self.armed = false;
    }
}

impl Drop for StartedClaim {
    fn drop(&mut self) {
        if self.armed {
            // Drop 里不能 await，所以这里只做"fire-and-forget"回滚。
            // 通常在 begin 的 future 被取消（如客户端断开）时走到这里；
            // 若运行时不可用（如正在关停）则跳过——进程退出时槽位会
            // 自然释放。
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let live = self.live.clone();
                let id = self.id;
                let claim_generation = self.claim_generation.clone();
                handle.spawn(async move {
                    if let Some(t) = live.lock().await.get_mut(&id)
                        && t.claim_generation == claim_generation
                    {
                        t.started = false;
                        t.claimed_at = None;
                        t.claim_generation.clear();
                        t.cancellation_id = None;
                        t.dormant_since = Some(Instant::now());
                    }
                });
            }
        }
    }
}

/// `begin` 授权成功后、入队前的复查（在 live 锁内调用）。
///
/// 进入队列前必须确认三个条件全部满足：条目仍存在、仍处于 `started`、
/// 且这还是本调用的那一批（`claim_generation` 一致）。任何一项不满足，
/// 都中止入队，不去执行这个已经被取消的操作。可能的情况：
/// - 授权等待期间被并发 `Cancel` 回滚（`started` 变 false）；
/// - 被并发 `Destroy` 移除（条目消失）；
/// - Cancel 后立刻重试，新 claim 替换了本调用的编号。
///
/// 为什么要单独存一个编号：`claim_generation` 是每次 claim 的唯一标记，
/// 独立于 polkit cancellation_id（`Simulate` / `UpdatesList` 没有
/// cancellation_id）。重试的新 claim 写入新编号，旧调用复查时发现不一致
/// 就拒绝入队——否则旧调用会把 Cancel 已报告取消的操作入队，与重试的
/// 操作用同一个 ID 排队，第一个完成后移除对象，重复项却仍然活跃。
pub(crate) fn check_claim_still_active(
    live: &HashMap<u64, LiveTransaction>,
    id: u64,
    expected_generation: &str,
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
    if t.claim_generation != expected_generation {
        return Err(zbus::fdo::Error::Failed(format!(
            "Transaction {id} claim was replaced by a newer invocation"
        )));
    }
    Ok(())
}

/// `Cancel` 对"已声明但未入队"对象的处理结果。
///
/// 把"是否回滚成功"和"带出的 cancellation_id"分开返回，是因为
/// `Simulate` / `UpdatesList` 没有 polkit 授权、没有 cancellation_id
/// （None），但回滚本身是成功的。如果这里只返回 `Option<String>`，
/// 调用方就无法区分"回滚成功但无 cid"和"没回滚"，于是会落到
/// `manager.cancel` 报 `UnknownObject`（尽管任务已经被阻止）。
pub(crate) struct ClaimRollback {
    /// claim 是否已被回滚（`started` 已清）。
    ///
    /// true 时调用方应返回成功；false 时调用方应走 `manager.cancel`
    /// （对象已入队 / 正在运行）。
    pub(crate) rolled_back: bool,
    /// 回滚时带出的 cancellation_id（可能是 None），供调用方在锁外取消
    /// 远程 polkit 检查。
    pub(crate) cancellation_id: Option<String>,
}

/// 在 live 锁内判断"已声明但未入队"（仍授权等待中）并回滚该声明。
///
/// 回滚会清掉 `started` / `claimed_at` / `claim_generation` /
/// `cancellation_id`，然后返回回滚结果与带出的 cancellation_id（由调用方
/// 在锁外调用 `cancel_authorization`）。
///
/// 用 `enqueued` 而不是 `manager.contains` 来区分"未入队的声明"和
/// "已入队 / 运行中 / 收尾中"：runner 清空 running 槽到 on_done 移除条目
/// 之间 `contains` 会短暂为 false。此时若把一个已执行的事务误判为未入队
/// 的声明，`Cancel` 会报成功——但 `ApplyChanges` 可能已经提交了。
/// `enqueued` 在 live 锁内与入队原子置位，直到条目移除才消失，没有这个
/// 窗口。条目不存在返回 None（调用方报 `UnknownObject`）。
///
/// 锁序：live → queue（与清扫器 phase 3 的 `claim_still_abandoned` 一致）。
pub(crate) async fn rollback_claim_if_not_enqueued(
    live: &mut HashMap<u64, LiveTransaction>,
    manager: &TransactionManager,
    id: u64,
) -> Option<ClaimRollback> {
    let t = live.get_mut(&id)?;
    // `enqueued` 是主判定；`contains` 只作防御。因为 `enqueued=false`
    // 时 begin 仍在 live 锁内入队，二者理论上不可能一致，若这里命中
    // `contains` 那就是编程错误。
    if t.started && !t.enqueued && !manager.contains(id).await {
        let cid = t.cancellation_id.take();
        t.started = false;
        t.claimed_at = None;
        t.claim_generation.clear();
        // 回到休眠：休眠计时从回滚时刻重新起算（同 `StartedClaim::rollback`）。
        t.dormant_since = Some(Instant::now());
        Some(ClaimRollback {
            rolled_back: true,
            cancellation_id: cid,
        })
    } else {
        Some(ClaimRollback {
            rolled_back: false,
            cancellation_id: None,
        })
    }
}

/// 在 live 锁内判断是否可以销毁一个事务对象并把它从注册表移除。
///
/// 可移除：休眠（未启动）的对象，以及授权等待中（已声明但未入队）的
/// 对象——后者会带出 cancellation_id，供调用方在锁外取消远程检查。
///
/// 不可移除：已入队 / 运行中 / 收尾中的对象（`enqueued` 为 true），返回
/// `Failed`；条目不存在返回 `UnknownObject`。
///
/// 用 `enqueued` 而不是 `contains` 判定：runner 清空 running 槽到 on_done
/// 移除条目之间 `contains` 会短暂为 false。此时若放行销毁，会在 Finished
/// 信号发出前就移除对象——客户端永远收不到终态。`enqueued` 在 live 锁内
/// 与入队原子置位，直到条目移除才消失，没有这个窗口。
///
/// 锁序：live → queue。
pub(crate) async fn remove_for_destroy(
    live: &mut HashMap<u64, LiveTransaction>,
    id: u64,
) -> Result<Option<String>, zbus::fdo::Error> {
    let Some(t) = live.get_mut(&id) else {
        return Err(zbus::fdo::Error::UnknownObject(format!(
            "Transaction {id} no longer exists"
        )));
    };
    if t.enqueued {
        return Err(zbus::fdo::Error::Failed(format!(
            "Transaction {id} already started"
        )));
    }
    let cid = t.cancellation_id.take();
    live.remove(&id);
    Ok(cid)
}

/// 判断一个休眠对象是否已经超过 `DORMANT_TIMEOUT`。
///
/// 以 `dormant_since`（创建或最近一次回滚的时刻）为基准；如果没有记录，
/// 退回 `created_at`。回滚会重置 `dormant_since`，保证"授权失败 /
/// 入队失败后回到休眠"的对象能获得完整的重试窗口，而不是在下一个清扫
/// 周期就被立刻回收。
pub(crate) fn dormant_expired(
    dormant_since: Option<Instant>,
    created_at: Instant,
    now: Instant,
) -> bool {
    now.saturating_duration_since(dormant_since.unwrap_or(created_at)) >= DORMANT_TIMEOUT
}

/// 周期清扫超时的事务对象，释放配额槽位。
///
/// 覆盖创建者断开连接或放弃对象、却没有显式调用 `Destroy` 的情况。回收
/// 两类对象：
/// 1. 休眠（未启动）超过 `DORMANT_TIMEOUT` 的对象（原始行为）；
/// 2. 已声明（`started`）但从未入队、且满足回收条件的对象：
///    - 创建者连接已死（对象被创建者锁定，操作永远无法继续）；
///    - 或声明超过 `CLAIM_TIMEOUT` 仍未入队（授权被放弃——即使创建者
///      还连着，也不能让声明绕过休眠超时长期占槽：否则未授权调用者可
///      对全部 16 个对象并发 `ApplyChanges` 并保持连接，同 uid 的其他
///      应用全被 `LimitsExceeded`，多个 uid 可耗尽全局 64 槽位）。
///
/// 已入队 / 运行中的事务（在 manager 里）即使创建者断开也会执行完，
/// 不回收。
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

        // Phase 1：在锁内快照候选对象，避免在锁内做异步判定。
        //
        // 元组：(id, path, sender, started, enqueued, created_at, claimed_at, dormant_since)。
        type Candidate = (
            u64,
            String,
            String,
            bool,
            bool,
            Instant,
            Option<Instant>,
            Option<Instant>,
        );
        let candidates: Vec<Candidate> = {
            let map = live.lock().await;
            map.iter()
                .map(|(id, t)| {
                    (
                        *id,
                        t.path.clone(),
                        t.sender.clone(),
                        t.started,
                        t.enqueued,
                        t.created_at,
                        t.claimed_at,
                        t.dormant_since,
                    )
                })
                .collect()
        };

        // Phase 2：在锁外做异步判定（此时不持锁，可以做 await 的网络 /
        // 队列查询）。
        let mut dormant_stale: Vec<String> = Vec::new();
        // (path, 快照时的 claimed_at)：phase 3 用快照值做生成校验，防止
        // 误删"回滚后重新声明"的新声明。
        let mut abandoned: Vec<(String, Option<Instant>)> = Vec::new();
        for (id, path, sender, started, enqueued, created_at, claimed_at, dormant_since) in
            candidates
        {
            if !started {
                if dormant_expired(dormant_since, created_at, now) {
                    dormant_stale.push(path);
                }
            } else if claim_expired(
                &manager,
                &dbus,
                enqueued,
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

        // Phase 3：在锁内移除。锁序是 live → queue（没有任何路径反向
        // queue → live），所以不会死锁。
        //
        // 对 dormant 候选要重新确认：防止夹在 begin 声明之间被误回收
        // （P12 竞态）。
        //
        // 对 abandoned 候选要在锁内复查：
        // ① 生成校验——条目当前 `claimed_at` 必须仍是快照时的值；期间被
        //    回滚后重新声明的（新声明）不删。
        // ② 与 begin 入队互斥——begin 在 live 锁内完成入队，所以这里持锁
        //    复查 manager 时，begin 不可能"刚通过复查就入队"：要么已入队
        //    （contains 命中，跳过），要么还没入队且 begin 被本锁挡住（我们
        //    移除后 begin 的入队前提——对象仍存在——不成立，begin 中止）。
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
                if let Some((id, t)) = map.iter().find(|(_, t)| &t.path == path)
                    && claim_still_abandoned(
                        &manager,
                        t.claimed_at,
                        *snap_claimed_at,
                        t.enqueued,
                        *id,
                    )
                    .await
                {
                    to_remove.push(*id);
                }
            }
            for id in to_remove {
                if let Some(t) = map.remove(&id) {
                    removed.push(t.path.clone());
                    // 被回收的声明若还有未完成的远程 polkit 检查，带出
                    // cancellation_id，在锁外显式取消。
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
        // 创建者断连 / 声明超时被回收的授权声明：远程 polkit 检查与认证
        // 弹窗不会随本地 future 消失而自动关闭，要显式调用
        // `CancelCheckAuthorization`。否则调用方可每隔一个清扫周期重连，
        // 远程检查持续累积（begin 的本地超时只覆盖 `TimedOut`，这里覆盖
        // dead-sender 与 reaper 回收这两条路径）。
        for cid in &cancel_ids {
            cancel_authorization(&conn, cid).await;
        }
    }
}

/// 判断一个已声明（`started`）但未入队的事务对象是否应被回收。
///
/// 已入队（`enqueued`）的事务——无论正在运行还是已结束尚未清理——一律
/// 不回收：runner 清空 running 槽到 on_done 移除条目之间，`contains` 会
/// 短暂为 false。此时若按 `contains` 判定，长任务超过 `CLAIM_TIMEOUT`
/// 后会在收尾窗口被误回收（对象 / 终态信号丢失）。
///
/// 以下两种情况之一视为放弃，可以回收：
/// - 创建者连接已死（对象被创建者锁定，操作永远无法继续）；
/// - 声明超过 `CLAIM_TIMEOUT` 仍未入队——即使创建者还连着，也视为超时，
///   防止声明绕过休眠 / abandoned 回收被用来长期占槽。
///
/// 已入队 / 运行中的事务即使创建者断开也会执行完，不回收。
pub(crate) async fn claim_expired(
    manager: &TransactionManager,
    dbus: &zbus::fdo::DBusProxy<'_>,
    enqueued: bool,
    id: u64,
    sender: &str,
    claimed_at: Instant,
    now: Instant,
) -> bool {
    if enqueued {
        // 已入队 / 运行中 / 收尾中：即使 contains 暂时 false 也不回收。
        return false;
    }
    if manager.contains(id).await {
        // 已入队 / 运行中：不回收。
        return false;
    }
    // 显式用 saturating 而不是 duration_since：reaper 捕获 now 之后、在
    // 锁外快照之前，可能并发 begin 声明写入晚于 now 的 claimed_at。
    // 标准库文档承诺 duration_since 在参数晚于 self 时会 panic（当前
    // 工具链实测返回 0，但这是未文档化的实现细节，不应依赖）。
    // saturating 语义明确：晚于 now → Duration::ZERO → 刚声明的不算超时、
    // 不回收，且跨 Rust 版本稳定。
    if now.saturating_duration_since(claimed_at) >= CLAIM_TIMEOUT {
        // 声明超时：无论创建者是否还在线，都视为放弃。
        return true;
    }
    // 创建者连接已死；BusName 解析失败时保守地不回收。
    match zbus::names::BusName::try_from(sender) {
        Ok(name) => !dbus.name_has_owner(name).await.unwrap_or(false),
        Err(_) => false,
    }
}

/// 清扫器 phase 3 对单个 abandoned 候选的移除判定（在 live 锁内调用）。
///
/// 三个条件都要满足才移除：
/// ① `enqueued` 的事务（已入队 / 运行中 / 收尾中）不删——runner 清空
///    running 槽到 on_done 移除条目之间 `contains` 短暂为 false，若按
///    `contains` 判定会误删尚未发终态信号的对象。
/// ② 生成校验——条目当前的 `claimed_at` 必须仍是快照时的值；期间被回滚
///    后重新声明的（重试）不删。
/// ③ 事务仍未入队才移除——begin 在 live 锁内完成入队，与这里互斥，所以
///    持锁复查时结果稳定。
pub(crate) async fn claim_still_abandoned(
    manager: &TransactionManager,
    entry_claimed_at: Option<Instant>,
    snap_claimed_at: Option<Instant>,
    enqueued: bool,
    id: u64,
) -> bool {
    if enqueued {
        // 已入队 / 运行中 / 收尾中：不删。
        return false;
    }
    if entry_claimed_at != snap_claimed_at {
        // 回滚后重新声明（claimed_at 变了）：是新声明，不删。
        return false;
    }
    !manager.contains(id).await
}
