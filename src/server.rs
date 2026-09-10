use crate::oma::{OmaClient, refresh_impl};
use crate::self_update::{IDLE_RECHECK_INTERVAL, SelfUpdate};
use crate::tum::updates_list_response;
use anyhow::anyhow;
use apt_auth_config::{AuthConfig, reqwuest::AuthMiddleware};
use chrono::Datelike;
use oma_apt_pkg::{
    AptConfig, AptDb, DpkgState, IndiciumSearch, OmaSearch, SearchType, apt_sources::SourceLookup,
};
use oma_fetch::reqwest::ClientBuilder;
use reqwest_middleware::ClientWithMiddleware;
use serde::{Deserialize, Serialize};
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use tokio::sync::{Mutex, Notify};
use tracing::{error, info, warn};
use zbus::{Connection, fdo, interface, names::BusName, object_server::SignalEmitter};
use zbus_polkit::policykit1::{AuthorityProxy, CheckAuthorizationFlags, Subject};

/// 退出协议，分两步，因为两件事该发生的时机不同：
///
/// 1. **发现二进制被替换** → 不再接受新工作（[`Exit::pending`]）。要尽早，不能
///    等空闲：监视器为了不打断在跑的操作而等锁，这期间如果还继续接纳新工作，
///    持续不断的请求就可能让它永远等不到空闲。
/// 2. **已接纳的工作收尾** → 通知 `main` 退出（[`Exit::wait`]）。此刻两把活动锁
///    都空着，关掉不会打断正在上报结果的任务。
///
/// 通知用 `notify_one` 而不是 `notify_waiters`：它会留下一个 permit，所以通知
/// 早于 `main` 开始等也不会丢。
#[derive(Default)]
pub struct Exit {
    pending: AtomicBool,
    notified: Notify,
}

impl Exit {
    /// 拒绝新工作的理由。写在一处：同一种状态经 D-Bus 或经 `anyhow` 上报，
    /// 文案一致。
    pub const REASON: &'static str = "The service is restarting to pick up an update";

    /// 是否已发现二进制被替换。请求侧在**取到活动锁之后**问这个。
    pub fn pending(&self) -> bool {
        self.pending.load(Ordering::Acquire)
    }

    /// 记下「二进制已被替换」。监视器一发现就调，不等空闲。
    pub fn mark_replaced(&self) {
        self.pending.store(true, Ordering::Release);
    }

    /// 等到「可以退了」。`main` 用它作为退出信号的一路。
    pub async fn wait(&self) {
        self.notified.notified().await;
    }

    /// 通知 `main` 可以退了。调用方必须持活动锁，见 [`decide_exit`]。
    fn notify_exit(&self) {
        self.notified.notify_one();
    }
}

/// 若此刻确实空闲，就通知退出并返回 `true`。
///
/// 空闲 = 两把活动锁都能拿到手 ⇒ 此刻既没有包操作、也没有索引刷新在进行。
/// 通知发生在**持锁状态下**：请求侧的 `try_lock` 若已经失败，它就已经在跑，
/// 而我们等到它跑了；请求若之后才开始，`Exit::pending` 早已置位，会在取到锁后
/// 立刻拒绝。不会再有任何工作以「刚开始」的身份通过。
///
/// 不负责置位 `pending`（那是监视器一发现就做的 [`Exit::mark_replaced`]）：
/// 这里的等待可能持续到很久以后，而停止接纳新工作不能等那么久。
fn decide_exit(run_lock: &Arc<Mutex<()>>, refresh_lock: &Arc<Mutex<()>>, exit: &Exit) -> bool {
    // 必须顺序获取，不要写成一个元组：那样第一个成功后第二个失败，第一个
    // guard 会随表达式结束而丢弃、锁又放开了。
    let Ok(run_guard) = run_lock.clone().try_lock_owned() else {
        return false;
    };
    let Ok(refresh_guard) = refresh_lock.clone().try_lock_owned() else {
        return false;
    };

    exit.notify_exit();
    drop((run_guard, refresh_guard));

    true
}

pub struct Amo {
    /// 同一时刻只允许一个任务改动包状态（安装/卸载/升级/摘要计算）。
    run_lock: Arc<Mutex<()>>,
    searcher: Arc<RwLock<IndiciumSearch>>,
    client: ClientWithMiddleware,
    /// 请求编号计数器：高位是日期，低位是当天的递增序列号。
    request_id_state: AtomicU64,
    apt_config: Arc<AptConfig>,
    refresh_lock: Arc<Mutex<()>>,
    /// 自我更新已确认、准备退出。置位后不再开始任何新的包操作或索引
    /// 刷新。
    exit: Arc<Exit>,
    /// 当前索引所基于的输入快照（lists + dpkg status），用于判断索引是否
    /// 已过期。
    index_inputs: Arc<Mutex<Option<IndexInputs>>>,
    /// APT lists 目录（TUM 清单读取用）。
    lists_dir: String,
}

impl Amo {
    pub fn new() -> anyhow::Result<Self> {
        let run_lock = Arc::new(Mutex::new(()));
        let refresh_lock = Arc::new(Mutex::new(()));
        let exit = Arc::new(Exit::default());

        let mut apt_config = AptConfig::new();
        apt_config.init_defaults()?;
        apt_config.set("Dir", "/");
        apt_config.set("RootDir", "/");
        let lists_dir = apt_config.get_dir("Dir::State::lists", "var/lib/apt/lists");

        // 输入快照在构建前捕获，与 `update_cache` 保持一致：若构建期间
        // 输入又变，快照仍指向本次实际使用的输入，首次查询会重建。
        let lists = lists_files_state(&apt_config);
        let apt_db = AptDb::load_or_build(&apt_config)
            .map_err(|e| anyhow::anyhow!("Failed to build oma packages database: {e}"))?;

        let dpkg_path_str = apt_config.get_file("Dir::State::status", "var/lib/dpkg/status");
        // 记录解析快照对应的 mtime（在读取 status 之前）。
        let status_mtime = std::fs::metadata(&dpkg_path_str)
            .ok()
            .and_then(|m| m.modified().ok());
        let dpkg = DpkgState::from_file(&dpkg_path_str)
            .map_err(|e| anyhow::anyhow!("Failed to parse dpkg status: {e}"))?;

        let searcher = Arc::new(RwLock::new(
            IndiciumSearch::new_with_cache(&apt_db, &dpkg, &apt_config, SearchType::Live, |_| {})
                .map_err(|e| anyhow::anyhow!("Failed to build search index: {e}"))?,
        ));

        let client = ClientBuilder::new().user_agent("oma/1.14.514").build()?;
        let client = reqwest_middleware::ClientBuilder::new(client)
            .with_init(AuthMiddleware::new(AuthConfig::system("/")?))
            .build();

        Ok(Self {
            run_lock,
            searcher,
            client: client.clone(),
            request_id_state: AtomicU64::new(current_date_val()),
            apt_config: Arc::new(apt_config),
            refresh_lock,
            exit,
            index_inputs: Arc::new(Mutex::new(Some(IndexInputs {
                lists,
                status_mtime,
            }))),
            lists_dir,
        })
    }

    /// 开始监视自我更新：本进程的二进制被替换且服务空闲时，决定退出并通知
    /// `main`，让 systemd 在下次 D-Bus 调用时拉起新版本。
    ///
    /// 挂监视失败就报错，由 `main` 当成启动失败。不做降级的理由：amo 升级后
    /// 旧进程会一直占着 D-Bus 名字、拿着旧代码继续服务，而且从外表完全看不
    /// 出来——「升级了但没生效」可能很久之后才有人发现；反过来，起不来只是当
    /// 下这一次调用失败，原因（比如 root 的 inotify 实例配额被占满）也在错误
    /// 信息里。
    ///
    /// 启动期间落地的替换会被漏掉：inotify 只投递注册之后的事件。这是有意
    /// 接受的——启动窗口只有初始化那几百毫秒，而且下一次升级会补上。
    pub fn watch_for_self_update(&self) -> anyhow::Result<()> {
        let mut self_update = SelfUpdate::watch()?;
        let run_lock = self.run_lock.clone();
        let refresh_lock = self.refresh_lock.clone();
        let exit = self.exit.clone();

        tokio::spawn(async move {
            if let Err(e) = self_update.wait_for_replacement().await {
                error!("Self-update watch stopped: {e}");
                return;
            }

            // 一发现被替换就停止接纳新工作，不等空闲。否则下面这个等锁的循环
            // 会被持续的请求一直延后——旧进程总有活干，就永远退不了。
            exit.mark_replaced();

            // 可能正忙着，隔一会儿再试；已经接纳的工作让它跑完。
            while !decide_exit(&run_lock, &refresh_lock, &exit) {
                tokio::time::sleep(IDLE_RECHECK_INTERVAL).await;
            }

            info!(
                "{} was replaced and amo is idle, notifying main",
                self_update.path().display()
            );
        });

        Ok(())
    }

    /// `main` 等的退出通知端。
    pub fn exit_handle(&self) -> Arc<Exit> {
        self.exit.clone()
    }

    fn generate_next_request_id(&self) -> u64 {
        let current_date_val = current_date_val();
        let mut old_state = self.request_id_state.load(Ordering::Relaxed);

        loop {
            // 右移 32 位拿日期，与掩码做按位与拿低 32 位序列号
            let old_date = old_state >> 32;
            let old_seq = old_state & 0xFFFFFFFF;

            let (new_date, new_seq) = if old_date != current_date_val {
                // 跨天了：重置序列号为 1
                (current_date_val, 1)
            } else {
                // 同一天：序列号直接自增（64 位下上限 4,294,967,295）
                (old_date, old_seq + 1)
            };

            // 重新拼装成一个
            let target_state = (new_date << 32) | new_seq;

            match self.request_id_state.compare_exchange_weak(
                old_state,
                target_state,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => return target_state,
                Err(actual) => old_state = actual, // 说明被其他线程做了这件事
            }
        }
    }

    /// 确保搜索索引反映最新的输入（lists + dpkg status），供 `search` /
    /// `get_description` 在读取前调用。委托 `refresh_if_stale`：等待任何
    /// 进行中的刷新完成后，比较输入快照并重建直到状态稳定。
    ///
    /// 等待不设超时；刷新失败时把错误通过 D-Bus 返回给调用方，而不是
    /// 静默返回旧索引。
    async fn ensure_fresh_index(&self, ctxt: &SignalEmitter<'_>) -> zbus::fdo::Result<()> {
        refresh_if_stale(ctxt.to_owned(), self.refresh_context())
            .await
            .map_err(|e| {
                error!("Failed to refresh package cache: {e}");
                zbus::fdo::Error::Failed(format!("Failed to refresh package cache: {e}"))
            })
    }

    /// 克隆一份共享的索引刷新状态，供后台任务使用。
    fn refresh_context(&self) -> RefreshContext {
        RefreshContext {
            searcher: self.searcher.clone(),
            apt_config: self.apt_config.clone(),
            refresh_lock: self.refresh_lock.clone(),
            exit: self.exit.clone(),
            index_inputs: self.index_inputs.clone(),
        }
    }

    /// 取得改动包状态的活动锁。
    ///
    /// 已有任务在跑、或服务已决定退出时返回错误。
    ///
    /// **先占锁再授权**（调用方顺序）：授权可能弹窗等待很久，期间必须让监视器
    /// 看到「有任务在进行」，否则它会在弹窗还开着时判定空闲并开始关闭，而
    /// `graceful_shutdown` 等的是在途方法调用，会一直等这个卡在弹窗上的方法，
    /// 旧进程就永远不释放 D-Bus 名字。同时也让「已在退出」的情况在弹窗之前就
    /// 拒绝——代价是弹窗期间并存的调用直接被拒，这与真有操作在跑时一致：正在
    /// 授权的那次就是本次要执行的操作。
    ///
    /// 退出检查放在取得锁**之后**：监视器是持锁置位的，若本调用先拿到锁，
    /// 监视器的 try_lock 必然失败；反之本调用拿到锁时一定能看到标志。两边不会
    /// 同时通过，所以退出一旦决定，就不会再有新工作开始。
    fn begin_activity(&self) -> Result<tokio::sync::OwnedMutexGuard<()>, fdo::Error> {
        let guard = self
            .run_lock
            .clone()
            .try_lock_owned()
            .map_err(|_| fdo::Error::Failed("Another task is already running!".to_string()))?;

        if self.exit.pending() {
            return Err(fdo::Error::Failed(Exit::REASON.to_string()));
        }

        Ok(guard)
    }
}

/// 通知客户端本服务即将退出，需要重连。
pub async fn announce_restart(conn: &Connection) {
    match conn
        .object_server()
        .interface::<_, Amo>("/io/aosc/Amo")
        .await
    {
        Ok(iface) => {
            if let Err(e) = AmoSignals::restart_schedule(iface.signal_emitter()).await {
                warn!("Failed to emit RestartSchedule: {e}");
            }
        }
        Err(e) => warn!("Failed to look up interface for RestartSchedule: {e}"),
    }
}

/// `refresh` / `apply_changes` / `invalidate_cache` / 查询路径共享的索引
/// 刷新状态。
#[derive(Clone)]
struct RefreshContext {
    searcher: Arc<RwLock<IndiciumSearch>>,
    apt_config: Arc<AptConfig>,
    refresh_lock: Arc<Mutex<()>>,
    exit: Arc<Exit>,
    index_inputs: Arc<Mutex<Option<IndexInputs>>>,
}

impl RefreshContext {
    /// 索引是否已基于当前输入（lists + dpkg status）构建。
    async fn is_fresh(&self) -> bool {
        self.index_inputs
            .lock()
            .await
            .as_ref()
            .is_some_and(|i| *i == current_inputs(&self.apt_config))
    }
}

fn current_date_val() -> u64 {
    let now = chrono::Local::now();
    let yy = now.year() as u64;
    let mm = now.month() as u64;
    let dd = now.day() as u64;

    yy * 10000 + mm * 100 + dd
}

/// 搜索索引所基于的输入快照：lists 目录中各索引文件的 (文件名, 大小, 整秒
/// mtime) 与 dpkg status 的 mtime。这些输入与当前一致时，索引才算是最新的。
#[derive(Clone, Debug, PartialEq, Eq)]
struct IndexInputs {
    lists: Vec<(String, u64, i64)>,
    status_mtime: Option<std::time::SystemTime>,
}

/// 当前 lists 目录状态：由当前源产生且存在的索引文件的 (文件名, 大小,
/// 整秒 mtime)，粒度与 oma-apt-pkg 的缓存有效性检查一致。
fn lists_files_state(apt_config: &AptConfig) -> Vec<(String, u64, i64)> {
    let lists_dir = apt_config.get_dir("Dir::State::lists", "var/lib/apt/lists");
    let lookup = SourceLookup::build(apt_config);
    let archs = apt_config.architectures();
    let mut state: Vec<(String, u64, i64)> = lookup
        .index_files(&archs)
        .into_iter()
        .filter_map(|(filename, _)| {
            let meta = std::fs::metadata(std::path::Path::new(&lists_dir).join(&filename)).ok()?;
            let mtime = meta
                .modified()
                .ok()?
                .duration_since(std::time::UNIX_EPOCH)
                .ok()?
                .as_secs() as i64;
            Some((filename, meta.len(), mtime))
        })
        .collect();
    state.sort();
    state
}

/// 当前输入快照。
fn current_inputs(apt_config: &AptConfig) -> IndexInputs {
    IndexInputs {
        lists: lists_files_state(apt_config),
        status_mtime: status_file_mtime(apt_config),
    }
}

/// 读取 `/var/lib/dpkg/status` 的修改时间。
fn status_file_mtime(apt_config: &AptConfig) -> Option<std::time::SystemTime> {
    let path = apt_config.get_file("Dir::State::status", "var/lib/dpkg/status");
    std::fs::metadata(path).ok().and_then(|m| m.modified().ok())
}

fn update_cache(
    searcher: &Arc<RwLock<IndiciumSearch>>,
    apt_config: &AptConfig,
) -> anyhow::Result<IndexInputs> {
    // 输入快照在构建前捕获：若构建期间 lists/status 又变，快照仍指向本次
    // 实际解析的输入，调用方循环重查会发现差异并再次重建。
    let lists = lists_files_state(apt_config);
    let apt_db = AptDb::load_or_build(apt_config)
        .map_err(|e| anyhow!("Failed to rebuild oma package database: {e}"))?;
    let status_path = apt_config.get_file("Dir::State::status", "var/lib/dpkg/status");
    // 记录解析快照对应的 mtime（在读取 status 之前）。
    let status_mtime = std::fs::metadata(&status_path)
        .ok()
        .and_then(|m| m.modified().ok());
    let dpkg = DpkgState::from_file(&status_path)
        .map_err(|e| anyhow!("Failed to read dpkg status: {e}"))?;

    // 若上次刷新在持锁时 panic，std RwLock 会中毒，之后每次 read/write
    // 都返回 Err。锁正常时用 refresh_from 增量更新；中毒时用 into_inner()
    // 取回锁并完整重建索引——refresh_from 只是增量更新，修不好 panic
    // 留下的半更新状态（新包可能只进了 pkg_map 而没进 index）。
    match searcher.write() {
        Ok(mut guard) => {
            guard.refresh_from(&apt_db, &dpkg);
        }
        Err(e) => {
            let fresh = IndiciumSearch::new_with_cache(
                &apt_db,
                &dpkg,
                apt_config,
                SearchType::Live,
                |_| {},
            )
            .map_err(|err| anyhow!("Failed to rebuild search index: {err}"))?;
            *e.into_inner() = fresh;
        }
    }

    info!("Search index status refreshed");
    Ok(IndexInputs {
        lists,
        status_mtime,
    })
}

/// 重建搜索索引（调用方须已持有 `refresh_lock`）。成功后记录新的输入快照
/// 并发 UpdatesChanged；失败时索引保持原样（记录不更新），由调用方决定
/// 如何处理。
async fn perform_refresh(ctx: &RefreshContext, emitter: &SignalEmitter<'_>) -> anyhow::Result<()> {
    let searcher = ctx.searcher.clone();
    let apt_config = ctx.apt_config.clone();
    match tokio::task::spawn_blocking(move || update_cache(&searcher, &apt_config)).await {
        Ok(Ok(snapshot)) => {
            *ctx.index_inputs.lock().await = Some(snapshot);
            if let Err(e) = AmoSignals::updates_changed(emitter).await {
                error!("Failed to emit UpdatesChanged signal: {e}");
            }
            Ok(())
        }
        Ok(Err(e)) => Err(e),
        Err(e) => Err(anyhow!("Cache refresh task failed to join: {e}")),
    }
}

/// 进入一次索引刷新：**等**当前刷新结束，但一旦决定退出就不再入场。
///
/// 两件事的顺序和方式都是契约的一部分：
///
/// - **等，不是试**。查询路径（`InvalidateCache` / `Search` /
///   `GetDescription`）以及操作收尾都会走到这里。撞上正在重建的索引时应当
///   排队，而不是被拒——否则每次重建期间的并发查询都会失败，`apply_changes`
///   更会把已经成功的包操作报成
///   「Package operation succeeded but cache refresh failed」。所以用
///   `lock().await`，不要改成 `try_lock`。
/// - **取到锁之后才看是否已发现被替换**（[`Exit::pending`]）。已置位时立刻返回
///   而不是继续等锁，这样关闭流程不会被卡住；此时也没必要再重建一次索引。
/// - **排队之前也看一眼**。排队是「占一个队列位、等一次调度器交接」，注定要被拒
///   的工作没必要去占：tokio 的公平锁把锁**直接交接**给下一个等待者，队列非空时
///   监视器的 `try_lock` 必然失败（实测），所以让队列尽快清空是有意义的。
///   （实测清空 64 个「取锁即拒」的等待者只要 ~11µs，远小于监视器 5 秒的重试间隔，
///   所以这不是「永远等不到空闲」那么严重——但仍然没有理由排这趟队。）
async fn begin_refresh(
    refresh_lock: &Arc<Mutex<()>>,
    exit: &Exit,
) -> anyhow::Result<tokio::sync::OwnedMutexGuard<()>> {
    if exit.pending() {
        return Err(anyhow!("{}", Exit::REASON));
    }

    let guard = refresh_lock.clone().lock_owned().await;

    if exit.pending() {
        return Err(anyhow!("{}", Exit::REASON));
    }

    Ok(guard)
}

/// 使搜索索引对应当前输入：已是最新则直接返回，否则持续重建直到最新
/// 或刷新失败。
async fn refresh_if_stale(
    emitter: SignalEmitter<'static>,
    ctx: RefreshContext,
) -> anyhow::Result<()> {
    let _guard = begin_refresh(&ctx.refresh_lock, &ctx.exit).await?;

    loop {
        if ctx.is_fresh().await {
            return Ok(());
        }
        // 刷新失败则直接返回错误，避免对持久性故障无限重试。
        perform_refresh(&ctx, &emitter).await?;
    }
}

/// 折算收尾的索引刷新结果：正在退出时一律当作成功。
///
/// 两个理由：
///
/// - 索引是**本进程**的，新进程会自己建，此刻刷新的成败与本次操作无关。
/// - `refresh_if_stale` 会因「正在重启」而拒绝，让那个拒绝冒出去就会把**已经
///   成功**的包操作报成「Package operation succeeded but cache refresh failed」。
///   升级 amo 自身时必然发生：监视器正是在这次事务里发现二进制被换掉的，那时
///   操作还握着 `run_lock` 在跑收尾。
///
/// 关键的是**在刷新尝试之后**才看 `pending`，不是在之前：`refresh_if_stale` 先
/// `await` 刷新锁（可能等上一次重建），这段时间足够让替换被发现。
fn refresh_result_for_report(exit: &Exit, refresh: anyhow::Result<()>) -> anyhow::Result<()> {
    if exit.pending() { Ok(()) } else { refresh }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ResultReport {
    pub request_id: u64,
    pub status: TaskStatus,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub enum TaskStatus {
    Success,
    Failed(String),
}

#[interface(name = "io.aosc.Amo1")]
impl Amo {
    #[tracing::instrument(ret, skip(self, conn))]
    async fn invalidate_cache(
        &self,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
        #[zbus(signal_context)] ctxt: SignalEmitter<'_>,
    ) -> zbus::fdo::Result<()> {
        let sender = header
            .sender()
            .ok_or_else(|| fdo::Error::AccessDenied("Unknown sender!".to_string()))?
            .to_owned();
        let dbus_proxy = zbus::fdo::DBusProxy::new(conn).await?;
        let real_uid = dbus_proxy
            .get_connection_unix_user(BusName::from(sender))
            .await?;

        if real_uid != 0 {
            return Err(fdo::Error::AccessDenied(
                "Only root may invalidate the package cache".to_string(),
            ));
        }

        // post-invoke 入口：使搜索索引对应当前输入后返回；索引已是最新时
        // 直接返回，失败时返回错误。
        refresh_if_stale(ctxt.to_owned(), self.refresh_context())
            .await
            .map_err(|e| fdo::Error::Failed(format!("Cache refresh failed: {e}")))?;

        Ok(())
    }

    #[tracing::instrument(ret, skip(self, ctxt, conn))]
    async fn refresh(
        &self,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(signal_context)] ctxt: SignalEmitter<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
    ) -> zbus::fdo::Result<u64> {
        // 先占锁再授权，理由见 `begin_activity`。
        let guard = self.begin_activity()?;
        auth(header, conn, "io.aosc.amo.refresh").await?;

        let request_id = self.generate_next_request_id();

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let ctxt_owned = ctxt.to_owned();

        tokio::spawn(async move {
            while let Some(status) = rx.recv().await {
                if let Err(e) = ctxt_owned.status(status.clone()).await {
                    error!(
                        msg = status,
                        error = e.to_string(),
                        "Failed to send refresh_status request!"
                    );
                }
            }
        });

        let client = self.client.clone();
        let ctxt_result = ctxt.to_owned();
        let ctx = self.refresh_context();
        let exit = self.exit.clone();

        tokio::spawn(async move {
            // 一直持有 run_lock 到本任务结束（含结果上报）：只包住阻塞部分
            // 不够——spawn_blocking 返回后 guard 就被释放，此时续作（刷新
            // 索引、发结果）还没跑，自我更新监视器可能趁这个空隙把两个锁
            // 都拿走并开始关闭，而异步任务会被 runtime 关闭中止，客户端就
            // 永远等不到事务结果。
            let _run_guard = guard;

            let outcome =
                tokio::task::spawn_blocking(move || refresh_impl(tx, client.clone())).await;

            let outcome = match outcome {
                Ok(r) => r,
                Err(e) => Err(anyhow!("Refresh task failed to join: {e}")),
            };

            // 等缓存刷新完成后再发 result_report，避免客户端收到完成信号
            // 时搜索索引还是旧的：refresh_impl 内部的 post-invoke 已触发
            // 刷新时（输入快照已更新）这里会跳过，否则由本方法重建。
            // 刷新失败也会反映在结果里，除非正在退出：见
            // `refresh_result_for_report`。
            let refresh_outcome = refresh_if_stale(ctxt_result.clone(), ctx).await;
            let refresh_outcome = refresh_result_for_report(&exit, refresh_outcome);

            let status = match (outcome, refresh_outcome) {
                (Ok(_), Ok(())) => TaskStatus::Success,
                (Err(e), _) => TaskStatus::Failed(e.to_string()),
                (Ok(_), Err(e)) => TaskStatus::Failed(format!(
                    "Package operation succeeded but cache refresh failed: {e}"
                )),
            };

            let report = ResultReport { request_id, status };
            if let Ok(json) = serde_json::to_string(&report)
                && let Err(e) = ctxt_result.result_report(json).await
            {
                error!("Failed to emit refresh result signal: {e}");
            }
        });

        Ok(request_id)
    }

    #[tracing::instrument(ret, skip(self))]
    async fn updates_list(&self) -> zbus::fdo::Result<String> {
        let guard = self.begin_activity()?;

        let client = self.client.clone();
        let lists_dir = self.lists_dir.clone();

        let result = tokio::task::spawn_blocking(move || {
            let _guard = guard;
            let mut apt = OmaClient::new(client, vec![])?;
            let operation = apt
                .summary(vec![], vec![], true)
                .map_err(|e| anyhow!("{e}"))?;
            Ok::<_, anyhow::Error>(updates_list_response(&lists_dir, operation))
        })
        .await
        .map_err(|e| zbus::fdo::Error::Failed(format!("Task failed: {e}")))?
        .map_err(|e| zbus::fdo::Error::Failed(e.to_string()))?;

        serde_json::to_string(&result).map_err(|e| zbus::fdo::Error::Failed(e.to_string()))
    }

    #[tracing::instrument(ret, skip(self, conn, ctxt), fields(install = ?install, remove = ?remove, upgrade = upgrade))]
    async fn apply_changes(
        &self,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
        #[zbus(signal_context)] ctxt: SignalEmitter<'_>,
        install: Vec<String>,
        remove: Vec<String>,
        upgrade: bool,
    ) -> zbus::fdo::Result<u64> {
        // 先占锁再授权，理由见 `begin_activity`。
        let guard = self.begin_activity()?;
        auth(header, conn, "io.aosc.Amo.apply.run").await?;

        let request_id = self.generate_next_request_id();

        let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let ctxt_progress = ctxt.to_owned();

        tokio::spawn(async move {
            while let Some(event_str) = progress_rx.recv().await {
                if let Err(e) = ctxt_progress.status(event_str).await {
                    error!("Failed to broadcast oma event signal: {}", e);
                }
            }
        });

        let client = self.client.clone();
        let ctxt_result = ctxt.to_owned();
        let ctx = self.refresh_context();
        let exit = self.exit.clone();

        tokio::spawn(async move {
            // 一直持有 run_lock 到本任务结束（含结果上报）：只包住阻塞部分
            // 不够——spawn_blocking 返回后 guard 就被释放，此时续作（刷新
            // 索引、发结果）还没跑，自我更新监视器可能趁这个空隙把两个锁
            // 都拿走并开始关闭，而异步任务会被 runtime 关闭中止，客户端就
            // 永远等不到事务结果。
            let _run_guard = guard;

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
                current_apt.commit(progress_tx, request_id)?;
                info!("apply_changes: commit done");

                Ok(())
            })
            .await;

            let result = match result {
                Ok(r) => r,
                Err(e) => Err(anyhow!("Apply task failed to join: {e}")),
            };

            // 等缓存刷新完成后再发 result_report：commit 内部 dpkg 触发的
            // DPkg::Post-Invoke 已刷新时（输入快照已更新）这里会跳过，
            // 否则重建。刷新失败也会反映在结果里，除非正在退出：见
            // `refresh_result_for_report`。
            let refresh_outcome = refresh_if_stale(ctxt_result.clone(), ctx).await;
            let refresh_outcome = refresh_result_for_report(&exit, refresh_outcome);
            info!("apply_changes: cache refresh done");

            let status = match (result, refresh_outcome) {
                (Ok(_), Ok(())) => TaskStatus::Success,
                (Err(e), _) => TaskStatus::Failed(e.to_string()),
                (Ok(_), Err(e)) => TaskStatus::Failed(format!(
                    "Package operation succeeded but cache refresh failed: {e}"
                )),
            };

            let report = ResultReport { request_id, status };
            if let Ok(json) = serde_json::to_string(&report)
                && let Err(e) = ctxt_result.result_report(json).await
            {
                error!("Failed to emit apply result signal: {e}");
            }
        });

        Ok(request_id)
    }

    #[tracing::instrument(ret, skip(self), fields(install = ?install, remove = ?remove, upgrade = upgrade))]
    async fn get_transaction(
        &self,
        install: Vec<String>,
        remove: Vec<String>,
        upgrade: bool,
    ) -> zbus::fdo::Result<String> {
        let guard = self.begin_activity()?;

        let client = self.client.clone();
        let lists_dir = self.lists_dir.clone();

        let result = tokio::task::spawn_blocking(move || {
            let _guard = guard;
            let mut apt = OmaClient::new(client, vec![])?;
            let operation = apt
                .summary(install, remove, upgrade)
                .map_err(|e| anyhow!("{e}"))?;
            Ok::<_, anyhow::Error>(updates_list_response(&lists_dir, operation))
        })
        .await
        .map_err(|e| zbus::fdo::Error::Failed(format!("Task failed: {e}")))?
        .map_err(|e| zbus::fdo::Error::Failed(e.to_string()))?;

        serde_json::to_string(&result).map_err(|e| zbus::fdo::Error::Failed(e.to_string()))
    }

    #[tracing::instrument(ret, skip(self))]
    async fn search(
        &self,
        #[zbus(signal_context)] ctxt: SignalEmitter<'_>,
        query: String,
    ) -> zbus::fdo::Result<String> {
        self.ensure_fresh_index(&ctxt).await?;

        // 锁中毒时返回旧索引而不是 panic：写侧下次刷新会恢复并重建。
        let engine = self.searcher.read().unwrap_or_else(|e| e.into_inner());

        match engine.search(&query) {
            Ok(results) => serde_json::to_string(&results)
                .map_err(|e| zbus::fdo::Error::Failed(format!("Search serialization error: {e}"))),
            Err(e) => Err(zbus::fdo::Error::Failed(e.to_string())),
        }
    }

    #[tracing::instrument(ret, skip(self))]
    async fn get_description(
        &self,
        #[zbus(signal_context)] ctxt: SignalEmitter<'_>,
        pkg_name: String,
    ) -> zbus::fdo::Result<String> {
        // 同 search：确保索引反映最新的 installed 状态。
        self.ensure_fresh_index(&ctxt).await?;

        // 锁中毒时返回旧索引而不是 panic：写侧下次刷新会恢复并重建。
        let engine = self.searcher.read().unwrap_or_else(|e| e.into_inner());

        match engine.pkg_map.get(&pkg_name) {
            Some(entry) => Ok(entry.description.clone()),
            None => Ok("No description available.".to_string()),
        }
    }

    #[zbus(signal)]
    async fn status(ctxt: &SignalEmitter<'_>, status: String) -> zbus::Result<()>;

    #[zbus(signal)]
    async fn result_report(ctxt: &SignalEmitter<'_>, report: String) -> zbus::Result<()>;

    #[zbus(signal)]
    async fn updates_changed(ctxt: &SignalEmitter<'_>) -> zbus::Result<()>;

    /// 二进制被更新、本进程即将退出时发出。客户端收到后应重建与本服务
    /// 的连接：旧进程的接口对象随后就会消失。
    #[zbus(signal)]
    async fn restart_schedule(ctxt: &SignalEmitter<'_>) -> zbus::Result<()>;
}

pub async fn auth(
    header: zbus::message::Header<'_>,
    conn: &Connection,
    action: &str,
) -> Result<(), fdo::Error> {
    let sender = header
        .sender()
        .ok_or_else(|| fdo::Error::AccessDenied("Unknown sender!".to_string()))?
        .to_owned();

    let dbus_proxy = zbus::fdo::DBusProxy::new(conn).await?;

    let bus_name = BusName::from(sender);
    let real_pid = dbus_proxy
        .get_connection_unix_process_id(bus_name.clone())
        .await?;
    let real_uid = dbus_proxy.get_connection_unix_user(bus_name).await?;

    let proxy = AuthorityProxy::new(conn).await?;
    let subject = Subject::new_for_owner(real_pid, None, Some(real_uid))
        .map_err(|e| fdo::Error::AccessDenied(e.to_string()))?;

    let result = proxy
        .check_authorization(
            &subject,
            action,
            &std::collections::HashMap::new(),
            CheckAuthorizationFlags::AllowUserInteraction.into(),
            "",
        )
        .await?;

    if !result.is_authorized {
        return Err(fdo::Error::AccessDenied("Authorized failed!".to_string()));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Exit, begin_refresh, decide_exit, refresh_result_for_report};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn entering_a_refresh_waits_for_the_one_in_progress() {
        // 查询路径的契约是排队而不是被拒：撞上正在重建的索引时必须等，
        // 否则每次重建期间的并发查询都会失败，已经成功的包操作还会被
        // 报成「refresh failed」。
        let refresh_lock = Arc::new(Mutex::new(()));

        let in_progress = refresh_lock.clone().try_lock_owned().unwrap();
        let waiting = tokio::spawn({
            let refresh_lock = refresh_lock.clone();
            async move { begin_refresh(&refresh_lock, &Exit::default()).await.is_ok() }
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !waiting.is_finished(),
            "must queue behind the refresh in progress, not fail immediately"
        );

        drop(in_progress);
        assert!(
            waiting.await.unwrap(),
            "must be admitted once the refresh in progress finishes"
        );
    }

    #[tokio::test]
    async fn new_work_is_refused_as_soon_as_the_replacement_is_seen() {
        // 监视器发现被替换后还得等已有工作收尾（下面那条用例），等待可能很久。
        // 这期间新来的工作必须从一开始就被拒——否则持续不断的请求会让旧进程
        // 一直有活干，监视器永远等不到空闲，退出被无限期推迟。
        let run_lock = Arc::new(Mutex::new(()));
        let refresh_lock = Arc::new(Mutex::new(()));
        let exit = Exit::default();

        exit.mark_replaced();

        // run 侧：请求取到锁后就能看到 pending，不会再开始新的包操作。
        let guard = run_lock.clone().try_lock_owned().unwrap();
        assert!(exit.pending());
        drop(guard);

        // refresh 侧：空闲时同样立即被拒（不会先等锁）。
        assert!(begin_refresh(&refresh_lock, &exit).await.is_err());
    }

    #[tokio::test]
    async fn exit_waits_for_the_work_that_was_already_admitted() {
        // 发现被替换不等于可以退：已经接纳的工作要让它跑完。这里用 refresh 锁
        // 代表「正在重建索引」——注意此时**不能**再调 begin_refresh，那条路会
        // 排队等锁（查询路径的契约），而排队不返回。
        let run_lock = Arc::new(Mutex::new(()));
        let refresh_lock = Arc::new(Mutex::new(()));
        let exit = Exit::default();

        let in_progress = refresh_lock.clone().try_lock_owned().unwrap();
        exit.mark_replaced();

        assert!(
            !decide_exit(&run_lock, &refresh_lock, &exit),
            "an operation is in progress, so exit has to wait for it"
        );

        // 在跑的那次结束了，这才谈得上退出。
        drop(in_progress);
        assert!(decide_exit(&run_lock, &refresh_lock, &exit));
    }

    #[tokio::test]
    async fn seeing_a_replacement_does_not_yet_tell_main_to_exit() {
        // 两阶段分开，正是为了不让持续的请求把退出无限延后，同时又不打断已经
        // 在跑的任务：发现被替换只是停止接纳新工作，退出要等 decide_exit 拿到
        // 两把锁。
        let exit = Exit::default();
        exit.mark_replaced();

        assert!(exit.pending());
        assert!(
            tokio::time::timeout(Duration::from_millis(100), exit.wait())
                .await
                .is_err(),
            "main must keep waiting while the admitted work drains"
        );
    }

    #[tokio::test]
    async fn a_decision_made_before_waiting_is_not_lost() {
        // `notify_one` 会留下一个 permit，所以「可以退了」发生在 main 开始
        // 等之前也不会丢。这正是它取代 channel 的原因。
        let run_lock = Arc::new(Mutex::new(()));
        let refresh_lock = Arc::new(Mutex::new(()));
        let exit = Exit::default();

        assert!(decide_exit(&run_lock, &refresh_lock, &exit));

        tokio::time::timeout(Duration::from_millis(100), exit.wait())
            .await
            .expect("the permit must survive a decision made before the wait");
    }

    #[tokio::test]
    async fn a_refresh_is_refused_without_queueing_once_the_replacement_is_seen() {
        // 锁被占着的时候，如果先排队就永远返回不了（测试里表现为超时）。已经
        // 发现替换就必须**不排队**地立刻拒绝：那趟队注定白排，而且会占住队列位。
        let refresh_lock = Arc::new(Mutex::new(()));
        let exit = Exit::default();

        let held = refresh_lock.clone().try_lock_owned().unwrap();
        exit.mark_replaced();

        let outcome = tokio::time::timeout(
            Duration::from_millis(100),
            begin_refresh(&refresh_lock, &exit),
        )
        .await;

        assert!(
            matches!(outcome, Ok(Err(_))),
            "must be refused before entering the lock queue, got {outcome:?}"
        );

        drop(held);
    }

    #[test]
    fn seeing_a_replacement_is_observable_by_requests() {
        let exit = Exit::default();
        assert!(!exit.pending());

        exit.mark_replaced();
        assert!(exit.pending());
    }

    #[test]
    fn a_restart_does_not_turn_a_finished_operation_into_a_failure() {
        // 升级 amo 自身时必然走到这里：监视器在事务进行中发现了二进制被换掉，
        // 于是收尾的 `refresh_if_stale` 被拒。那个拒绝要挡的是新工作，不该把
        // 已经成功的包操作报成失败。
        let exit = Exit::default();
        let rejected = || Err(anyhow::anyhow!("{}", Exit::REASON));

        // 没在退出：刷新失败照实上报。
        assert!(refresh_result_for_report(&exit, rejected()).is_err());

        // 已发现被替换：刷新结果不再影响本次操作的结果。
        exit.mark_replaced();
        assert!(refresh_result_for_report(&exit, rejected()).is_ok());
    }
}
