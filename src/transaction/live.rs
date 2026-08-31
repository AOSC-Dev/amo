//! 活动事务对象注册表与生命周期管理。
//!
//! `LiveTransaction` 是每个事务 D-Bus 对象在注册表中的条目（休眠/已声明/
//! 已入队三种状态），`StartedClaim` 是 `begin` 的启动声明守卫，`reclaim_dormant`
//! 是周期清扫器。所有对注册表的修改（声明、销毁、清扫）共用同一把 live 锁，
//! 保证"启动声明 / 销毁 / 清扫"互斥（单一同步边界）。

use crate::auth::cancel_authorization;
use crate::transaction::object::TransactionObject;
use crate::transaction::TransactionManager;
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

/// 同时存在的活动事务对象上限（含休眠中未启动的），防止 CreateTransaction
/// 被无限调用耗尽对象服务器内存。
pub(crate) const MAX_LIVE_TRANSACTIONS: usize = 64;
/// 单个 uid 同时拥有的活动事务对象上限（休眠对象不占队列名额，可被
/// 无限创建；每用户配额防止一个用户占满全局槽位）。
pub(crate) const MAX_LIVE_PER_UID: usize = 16;
/// 休眠（未启动）事务对象的回收超时：超过该时间未启动即被清扫器移除。
pub(crate) const DORMANT_TIMEOUT: Duration = Duration::from_secs(60);
/// 已 claim（started）但未入队（授权等待中）对象的回收超时：超过该时间
/// 仍未入队即被清扫器移除。比休眠超时长得多，给真实授权弹窗留足时间；
/// 但必须有界——否则未授权调用者可对全部 16 个对象并发 ApplyChanges 并
/// 保持连接，让 claim 绕过休眠与 abandoned 回收、长期占住槽位（同 uid
/// 其他应用全被 LimitsExceeded，多个 uid 可耗尽全局 64 槽）。
pub(crate) const CLAIM_TIMEOUT: Duration = Duration::from_secs(300);
/// PolicyKit `CheckAuthorization` 的 cancellation_id 计数器：每个授权检查
/// 用唯一 ID（空 ID 的远程检查不可取消），超时后才能通过
/// `CancelCheckAuthorization` 显式取消远程检查。
static NEXT_CANCEL_ID: AtomicU64 = AtomicU64::new(0);

/// 生成唯一且非空的 PolicyKit cancellation_id。计数器单调递增保证进程内
/// 唯一，带事务 id 便于排查。
pub(crate) fn next_cancellation_id(tx_id: u64) -> String {
    format!(
        "amo-{tx_id}-{}",
        NEXT_CANCEL_ID.fetch_add(1, Ordering::Relaxed)
    )
}

/// claim 代际计数器：每次 begin 调用（无论是否需授权）都生成唯一代际，
/// 用于区分"Cancel 回滚后立即重试"的新旧调用。Simulate/UpdatesList 无
/// polkit cancellation_id，若用 cancellation_id 当代际，None 会让新旧
/// 调用无法区分（旧调用可把已取消的操作入队，或回滚清掉新 claim）。
static NEXT_CLAIM_GENERATION: AtomicU64 = AtomicU64::new(0);

/// 生成唯一 claim 代际。计数器单调递增保证进程内唯一，带事务 id 便于
/// 排查。
pub(crate) fn next_claim_generation(tx_id: u64) -> String {
    format!(
        "amo-{tx_id}-{}",
        NEXT_CLAIM_GENERATION.fetch_add(1, Ordering::Relaxed)
    )
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
    /// 本次 claim 的代际标记：每次 begin 调用（无论是否需授权）都生成
    /// 唯一值，用于区分"Cancel 回滚后立即重试"的新旧调用。Simulate/
    /// UpdatesList 无 polkit cancellation_id，若用 cancellation_id 当代际，
    /// None 会让新旧调用无法区分（旧调用可把已取消的操作入队，或回滚
    /// 清掉新 claim）。回滚时清空（无 claim 即无代际）。
    pub(crate) claim_generation: String,
    /// 本次 claim 对应的 PolicyKit `CheckAuthorization` cancellation_id
    /// （仅需要授权的操作）：与 claim_generation 分开，声明被清扫器判定
    /// abandoned（创建者断连或 claim 超时）时用它显式取消远程检查——
    /// begin 本地超时只覆盖 TimedOut，创建者断连时 begin 的 future 不会
    /// 被 zbus 取消，只有清扫器能回收并取消远程检查。
    pub(crate) cancellation_id: Option<String>,
    pub(crate) started: bool,
}

/// `begin` 的启动声明（lease）守卫：在 live 锁内标记 `started` 后持有它，
/// 保证任何未入队的退出路径都会回滚 `started`——授权失败、入队失败，乃至
/// 客户端在 polkit 弹窗期间断开导致 begin 的 future 被取消，都不会留下
/// "已启动但从未入队"的对象永久占用槽位（清扫器只回收 dormant 对象）。
pub(crate) struct StartedClaim {
    live: Arc<Mutex<HashMap<u64, LiveTransaction>>>,
    id: u64,
    /// 本代 claim 的代际标记。回滚时只清"自己这一代"的 claim：Cancel
    /// 回滚后用户可立即 re-trigger（新 claim、新代际），旧 begin 的授权
    /// future 最终失败时若无条件回滚会把新 claim 一起清掉（新事务被旧
    /// future 误杀）。比对代际保证旧 future 的回滚只作用于自己声明的
    /// 那一代——Simulate/UpdatesList 无 polkit cancellation_id，代际必须
    /// 独立于 cancellation_id 分配。
    claim_generation: String,
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

    /// 已知失败路径上立即回滚（同步等待，不依赖 Drop 的异步时机）。
    pub(crate) async fn rollback(&mut self) {
        if self.armed {
            self.armed = false;
            if let Some(t) = self.live.lock().await.get_mut(&self.id) {
                if t.claim_generation == self.claim_generation {
                    t.started = false;
                    t.claimed_at = None;
                    t.claim_generation.clear();
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
    pub(crate) fn commit(&mut self) {
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
                let claim_generation = self.claim_generation.clone();
                handle.spawn(async move {
                    if let Some(t) = live.lock().await.get_mut(&id) {
                        if t.claim_generation == claim_generation {
                            t.started = false;
                            t.claimed_at = None;
                            t.claim_generation.clear();
                            t.cancellation_id = None;
                            t.dormant_since = Some(Instant::now());
                        }
                    }
                });
            }
        }
    }
}

/// begin 授权成功后、入队前的复查（live 锁内调用）：条目必须仍存在、
/// started，且 claim 仍是本调用的那一代（claim_generation 一致）——授权
/// 等待期间被并发 Cancel 回滚（started=false）、Destroy 移除（条目缺失）、
/// 或 Cancel 后立即重试（新 claim 替换了本调用的代际）都中止入队，不执行
/// 已取消的操作。claim_generation 是 claim 的代际标记（每次 claim 生成
/// 唯一值，独立于 polkit cancellation_id——Simulate/UpdatesList 无
/// cancellation_id，若用其当代际，None 会让新旧调用无法区分）：重试的新
/// claim 写入新代际，旧调用复查时发现不一致即拒绝——否则旧调用会把
/// Cancel 已报告取消的操作入队，与重试的操作同 ID 排队，第一个完成移除
/// 对象后重复项仍活跃。
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

/// Cancel 对"已 claim 但未入队"对象的处理结果：回滚是否成功与带出的
/// cancellation_id 分开返回——Simulate/UpdatesList 无 polkit 授权，没有
/// cancellation_id（None），但回滚本身成功了；若只返回 Option<String>，
/// None 无法区分"回滚成功但无 cid"与"未回滚"，调用方会落入
/// manager.cancel 报 UnknownObject（尽管任务已被阻止）。
pub(crate) struct ClaimRollback {
    /// claim 是否被回滚（started 已清）：true 时调用方应返回成功；
    /// false 时调用方应走 manager.cancel（已入队/运行中）。
    pub(crate) rolled_back: bool,
    /// 回滚时带出的 cancellation_id（可能为 None），供调用方锁外取消
    /// 远程 PolicyKit 检查。
    pub(crate) cancellation_id: Option<String>,
}

/// 在 live 锁内判定"已 claim 但未入队"（授权等待中）并回滚 claim：清
/// started/claimed_at/claim_generation/cancellation_id，返回回滚结果与
/// 带出的 cancellation_id（调用方锁外 `cancel_authorization`）。已入队/
/// 运行中（manager 里有）返回 rolled_back=false（走 manager.cancel）；
/// 条目不存在返回 None（报 UnknownObject）。锁序 live→queue（与清扫器
/// phase 3 的 claim_still_abandoned 一致）。
pub(crate) async fn rollback_claim_if_not_enqueued(
    live: &mut HashMap<u64, LiveTransaction>,
    manager: &TransactionManager,
    id: u64,
) -> Option<ClaimRollback> {
    let t = live.get_mut(&id)?;
    if t.started && !manager.contains(id).await {
        let cid = t.cancellation_id.take();
        t.started = false;
        t.claimed_at = None;
        t.claim_generation.clear();
        // 回到休眠：休眠计时从回滚时刻重新起算（同 StartedClaim::rollback）。
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

/// 在 live 锁内判定 destroy 是否可行并移除条目：dormant（未启动）与
/// 授权等待中（claimed-but-not-enqueued）可移除，后者带出 cancellation_id
/// 供调用方锁外取消远程检查；已入队/运行中拒绝（Failed）；条目不存在报
/// UnknownObject。锁序 live→queue。
pub(crate) async fn remove_for_destroy(
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
pub(crate) fn dormant_expired(dormant_since: Option<Instant>, created_at: Instant, now: Instant) -> bool {
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
            cancel_authorization(&conn, cid).await;
        }
    }
}

/// 判定一个已 claim（started）但未入队的事务对象是否应被回收：创建者
/// 连接已死（sender 锁定，操作永远无法继续），或 claim 超过 CLAIM_TIMEOUT
/// 仍未入队（授权被放弃——即使创建者还连着，也视为超时，防止 claim 绕过
/// 休眠/abandoned 回收被用来长期占槽）。已入队/运行中的事务即使创建者
/// 断开也执行完，不回收。
pub(crate) async fn claim_expired(
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
pub(crate) async fn claim_still_abandoned(
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