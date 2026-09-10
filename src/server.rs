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
use tokio::sync::Mutex;
use tracing::{error, info, warn};
use zbus::{Connection, fdo, interface, names::BusName, object_server::SignalEmitter};
use zbus_polkit::policykit1::{AuthorityProxy, CheckAuthorizationFlags, Subject};

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
    shutting_down: Arc<AtomicBool>,
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
        let shutting_down = Arc::new(AtomicBool::new(false));

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
            shutting_down,
            index_inputs: Arc::new(Mutex::new(Some(IndexInputs {
                lists,
                status_mtime,
            }))),
            lists_dir,
        })
    }

    /// 开始监视自我更新：本进程的二进制被替换且服务空闲时，由返回的通知端
    /// 告知 `main` 退出，让 systemd 在下次 D-Bus 调用时拉起新版本。
    ///
    /// 单独一步，不放进 `new()`：`Amo` 要交给 D-Bus 对象服务器，通知端则由
    /// `main` 的 `select!` 等待，两者归属不同。
    pub fn watch_for_self_update(&self) -> anyhow::Result<RestartWatcher> {
        spawn_restart_watcher(
            self.run_lock.clone(),
            self.refresh_lock.clone(),
            self.shutting_down.clone(),
        )
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
            shutting_down: self.shutting_down.clone(),
            index_inputs: self.index_inputs.clone(),
        }
    }

    /// 取得改动包状态的活动锁。
    ///
    /// 已有任务在跑、或服务已决定退出时返回错误。检查放在取得锁**之后**：
    /// 监视器是持锁置位的，若本调用先拿到锁，监视器的 try_lock 必然失败；
    /// 反之本调用拿到锁时一定能看到标志。两边不会同时通过。
    fn begin_activity(&self) -> Result<tokio::sync::OwnedMutexGuard<()>, fdo::Error> {
        let guard = self
            .run_lock
            .clone()
            .try_lock_owned()
            .map_err(|_| fdo::Error::Failed("Another task is already running!".to_string()))?;

        if self.shutting_down.load(Ordering::Acquire) {
            return Err(fdo::Error::Failed(
                "The service is restarting to pick up an update".to_string(),
            ));
        }

        Ok(guard)
    }
}

/// 自我更新的通知端。
pub struct RestartWatcher {
    rx: tokio::sync::mpsc::UnboundedReceiver<()>,
}

impl RestartWatcher {
    /// 等待「二进制已被替换且服务空闲」。监视任务只投递一次。
    pub async fn wait(&mut self) {
        match self.rx.recv().await {
            Some(()) => {}
            // 通道关闭说明监视任务已不在；保持挂起，不要把它当成重启通知。
            None => std::future::pending().await,
        }
    }
}

/// 启动自我更新监视：二进制被替换且服务空闲时，通过通道通知 main。
///
/// 不必赶在 [`Amo::new`] 的初始化之前挂上，谁先谁后都不漏：注册之后马上会
/// 做一次「运行中的二进制和安装路径是否已经不一致」的检查，启动期间落地的
/// 替换由它兜底（详见 `SelfUpdate::replaced_at_start`），之后才靠事件。
///
/// 挂监视失败就直接报错退出，不做降级。降级的后果比起不来严重：amo 升级后
/// 旧进程会一直占着 D-Bus 名字、拿着旧代码继续服务，而且从外表完全看不出来
/// ——「升级了但没生效」这种情况可能很久之后才有人发现。反过来，起不来只是
/// 当下这一次调用失败，原因（比如 root 的 inotify 实例配额被占满）也在错误
/// 信息里，修好就恢复正常。
fn spawn_restart_watcher(
    run_lock: Arc<Mutex<()>>,
    refresh_lock: Arc<Mutex<()>>,
    shutting_down: Arc<AtomicBool>,
) -> anyhow::Result<RestartWatcher> {
    let mut self_update = SelfUpdate::watch()?;
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

    tokio::spawn(async move {
        // 监视注册之前就已落地的替换不会有事件，先查一次；之后靠事件叫醒，
        // 再由同一套判据确认。
        let replaced_at_start = self_update.replaced_at_start();
        if !replaced_at_start && let Err(e) = self_update.wait_for_replacement().await {
            error!("Self-update watch stopped: {e}");
            return;
        }

        // 可能正忙着，隔一会儿再试着把两个锁都拿到手。
        loop {
            // 两个锁都能拿到 ⇒ 此刻没有包操作或索引刷新在进行。必须顺序
            // 获取：若放进同一个元组，第一个成功后第二个失败，第一个
            // guard 会被立即丢弃、锁又放开了。
            let Ok(run_guard) = run_lock.clone().try_lock_owned() else {
                tokio::time::sleep(IDLE_RECHECK_INTERVAL).await;
                continue;
            };
            let Ok(refresh_guard) = refresh_lock.clone().try_lock_owned() else {
                drop(run_guard);
                tokio::time::sleep(IDLE_RECHECK_INTERVAL).await;
                continue;
            };

            // 在持锁状态下置位，与请求侧的「取锁后检查」配对：请求若先
            // 取到锁，这里的 try_lock 就会失败；反之请求取到锁时一定会
            // 看到标志，从而拒绝本次调用。这样不会再有新工作开始。
            shutting_down.store(true, Ordering::Release);
            drop((run_guard, refresh_guard));

            info!(
                "{} was replaced and amo is idle, notifying main",
                self_update.path().display()
            );
            // main 可能已因信号退出，发送失败无需处理。
            let _ = tx.send(());
            return;
        }
    });

    Ok(RestartWatcher { rx })
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
    shutting_down: Arc<AtomicBool>,
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

/// 使搜索索引对应当前输入：已是最新则直接返回，否则持续重建直到最新
/// 或刷新失败。
async fn refresh_if_stale(
    emitter: SignalEmitter<'static>,
    ctx: RefreshContext,
) -> anyhow::Result<()> {
    let _guard = ctx.refresh_lock.lock().await;

    // 取到锁之后才检查：监视器是持锁置位的，两边不能同时通过。置位后
    // 立刻返回而不是继续等锁，这样关闭流程不会被卡住。
    if ctx.shutting_down.load(Ordering::Acquire) {
        return Err(anyhow!("the service is restarting to pick up an update"));
    }

    loop {
        if ctx.is_fresh().await {
            return Ok(());
        }
        // 刷新失败则直接返回错误，避免对持久性故障无限重试。
        perform_refresh(&ctx, &emitter).await?;
    }
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
        // 先占住活动锁再授权：授权可能弹窗等待很久，期间必须让监视器看到
        // 「有任务在进行」，否则它会在弹窗还开着时判定空闲并开始关闭，而
        // graceful_shutdown 等的是在途方法调用，会一直等这个卡在弹窗上的
        // 方法，旧进程就永远不释放 D-Bus 名字。同时也让「已在关闭」的情况
        // 在弹窗之前就拒绝。
        //
        // 代价是弹窗期间并存的调用会直接被拒，而不是像不持锁那样先弹一次
        // 授权再失败——这与真有操作在跑时一致：正在授权的那次就是本次要执行
        // 的操作。
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
            // 刷新失败也会反映在结果里。
            let refresh_outcome = refresh_if_stale(ctxt_result.clone(), ctx).await;

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
        // 先占住活动锁再授权：授权可能弹窗等待很久，期间必须让监视器看到
        // 「有任务在进行」，否则它会在弹窗还开着时判定空闲并开始关闭，而
        // graceful_shutdown 等的是在途方法调用，会一直等这个卡在弹窗上的
        // 方法，旧进程就永远不释放 D-Bus 名字。同时也让「已在关闭」的情况
        // 在弹窗之前就拒绝。
        //
        // 代价是弹窗期间并存的调用会直接被拒，而不是像不持锁那样先弹一次
        // 授权再失败——这与真有操作在跑时一致：正在授权的那次就是本次要执行
        // 的操作。
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
            // 否则重建。刷新失败也会反映在结果里。
            let refresh_outcome = refresh_if_stale(ctxt_result.clone(), ctx).await;
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
