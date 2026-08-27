use crate::oma::{OmaClient, refresh_impl};
use crate::transaction::{CancelError, EnqueueError, Task, TransactionManager, TransactionRole};
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
use std::{
    collections::HashMap,
    sync::{
        Arc, OnceLock, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use tracing::{error, info};
use zbus::{
    Connection, fdo, interface,
    names::BusName,
    object_server::{ObjectServer, SignalEmitter},
    zvariant::OwnedObjectPath,
};
use zbus_polkit::policykit1::{AuthorityProxy, CheckAuthorizationFlags, Subject};

/// 同时存在的活动事务对象上限（含休眠中未启动的），防止 CreateTransaction
/// 被无限调用耗尽对象服务器内存。
const MAX_LIVE_TRANSACTIONS: usize = 64;
/// 单个 uid 同时拥有的活动事务对象上限（休眠对象不占队列名额，可被
/// 无限创建；每用户配额防止一个用户占满全局槽位）。
const MAX_LIVE_PER_UID: usize = 16;
/// 休眠（未启动）事务对象的回收超时：超过该时间未启动即被清扫器移除。
const DORMANT_TIMEOUT: Duration = Duration::from_secs(60);

/// 活动事务对象注册表条目。
struct LiveTransaction {
    path: String,
    uid: u32,
    created_at: Instant,
    started: bool,
}

pub struct Amo {
    manager: Arc<TransactionManager>,
    searcher: Arc<RwLock<IndiciumSearch>>,
    client: ClientWithMiddleware,
    request_id_state: AtomicU64,
    apt_config: Arc<AptConfig>,
    refresh_lock: Arc<Mutex<()>>,
    /// 当前索引所基于的输入快照（lists + dpkg status），用于判断索引是否
    /// 已过期。
    index_inputs: Arc<Mutex<Option<IndexInputs>>>,
    /// APT lists 目录（TUM 清单读取用）。
    lists_dir: String,
    /// 活动事务对象注册表（含休眠未启动的）：全局上限与每用户配额检查、
    /// 清扫器回收、事务结束移除。
    live: Arc<Mutex<HashMap<u64, LiveTransaction>>>,
    /// 休眠对象清扫器只启动一次。
    reaper_started: OnceLock<()>,
}

impl Amo {
    pub fn new() -> anyhow::Result<Self> {
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

        let manager = TransactionManager::new();

        Ok(Self {
            manager,
            searcher,
            client: client.clone(),
            request_id_state: AtomicU64::new(current_date_val()),
            apt_config: Arc::new(apt_config),
            refresh_lock: Arc::new(Mutex::new(())),
            index_inputs: Arc::new(Mutex::new(Some(IndexInputs {
                lists,
                status_mtime,
            }))),
            lists_dir,
            live: Arc::new(Mutex::new(HashMap::new())),
            reaper_started: OnceLock::new(),
        })
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
            index_inputs: self.index_inputs.clone(),
        }
    }
}

/// `refresh` / `apply_changes` / `invalidate_cache` / 查询路径共享的索引
/// 刷新状态。
#[derive(Clone)]
struct RefreshContext {
    searcher: Arc<RwLock<IndiciumSearch>>,
    apt_config: Arc<AptConfig>,
    refresh_lock: Arc<Mutex<()>>,
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
        let uid = dbus_proxy
            .get_connection_unix_user(BusName::from(sender))
            .await?;
        if uid != 0 {
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

    /// PackageKit 风格：创建休眠的事务对象并返回其路径
    /// `/io/aosc/Amo/Transaction/<id>`。对象注册时不执行任何工作；
    /// 客户端先订阅该路径上的信号，再调用事务对象上的操作方法开工，
    /// 因此不存在"先调用后订阅"的竞态（信号不重放）。事务结束
    /// （完成或取消）后对象自动移除。
    #[tracing::instrument(ret, skip(self, conn, server, ctxt))]
    async fn create_transaction(
        &self,
        #[zbus(object_server)] server: &ObjectServer,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
        #[zbus(signal_context)] ctxt: SignalEmitter<'_>,
    ) -> zbus::fdo::Result<OwnedObjectPath> {
        // 全局上限 + 每用户配额：休眠对象不占队列名额，可被无限创建，
        // 若不加配额，一个用户可创建 64 个永不启动的对象永久耗尽槽位。
        let (sender, uid) = peer_identity(&header, conn).await?;
        {
            let live = self.live.lock().await;
            if live.len() >= MAX_LIVE_TRANSACTIONS {
                return Err(fdo::Error::LimitsExceeded(
                    "Too many live transactions".to_string(),
                ));
            }
            if live.values().filter(|t| t.uid == uid).count() >= MAX_LIVE_PER_UID {
                return Err(fdo::Error::LimitsExceeded(
                    "Too many live transactions for this user".to_string(),
                ));
            }
        }

        let id = self.generate_next_request_id();
        let path = format!("/io/aosc/Amo/Transaction/{id}");
        let obj = TransactionObject {
            manager: self.manager.clone(),
            id,
            sender,
            uid,
            client: self.client.clone(),
            ctx: self.refresh_context(),
            lists_dir: self.lists_dir.clone(),
            main_emitter: ctxt.to_owned(),
            server: server.clone(),
            live: self.live.clone(),
            started: AtomicBool::new(false),
        };
        server
            .at(&*path, obj)
            .await
            .map_err(|e| fdo::Error::Failed(format!("Failed to create transaction: {e}")))?;
        self.live.lock().await.insert(
            id,
            LiveTransaction {
                path: path.clone(),
                uid,
                created_at: Instant::now(),
                started: false,
            },
        );
        // 启动一次性的休眠对象清扫器（覆盖创建者断开/放弃对象的情况）。
        let _ = self.reaper_started.get_or_init(|| {
            let live = self.live.clone();
            let server = server.clone();
            tokio::spawn(reclaim_dormant(live, server));
        });
        OwnedObjectPath::try_from(path.as_str())
            .map_err(|e| fdo::Error::Failed(format!("Invalid transaction path: {e}")))
    }

    #[tracing::instrument(ret, skip(self))]
    async fn get_transaction_list(&self) -> zbus::fdo::Result<String> {
        let list = self.manager.list().await;
        serde_json::to_string(&list)
            .map_err(|e| zbus::fdo::Error::Failed(format!("Serialize transaction list: {e}")))
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
    async fn updates_changed(ctxt: &SignalEmitter<'_>) -> zbus::Result<()>;
}

/// 单个事务的 D-Bus 对象（PackageKit 风格）：路径
/// `/io/aosc/Amo/Transaction/<id>`。
///
/// 操作方法（Refresh/ApplyChanges/Simulate/UpdatesList/Cancel）与
/// Status/ResultReport/TransactionState 信号都挂在该对象自己的路径上，
/// 信号天然按事务隔离——客户端无需按 transaction_id 过滤，也不存在
/// "先订阅后调用"的竞态（客户端先 CreateTransaction 拿路径、订阅信号，
/// 再调用操作方法开工）。
pub struct TransactionObject {
    manager: Arc<TransactionManager>,
    id: u64,
    /// 创建事务对象的连接（sender）唯一名：只有它能操作该对象
    /// （PackageKit 风格，所有方法先校验 sender）。
    sender: String,
    /// 创建者的 uid，记录到事务（GetTransactionList / 队列配额）。
    uid: u32,
    client: ClientWithMiddleware,
    ctx: RefreshContext,
    /// APT lists 目录（TUM 清单读取用）。
    lists_dir: String,
    /// 主接口（/io/aosc/Amo）的信号发射目标，供 UpdatesChanged 等主接口信号用。
    main_emitter: SignalEmitter<'static>,
    /// 动态对象服务器：事务结束时移除自身。
    server: ObjectServer,
    /// 活动事务对象注册表：启动时标记、结束（完成/取消）时移除。
    live: Arc<Mutex<HashMap<u64, LiveTransaction>>>,
    /// 一个事务对象只能启动一次操作。
    started: AtomicBool,
}

impl TransactionObject {
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
        if self.started.swap(true, Ordering::SeqCst) {
            return Err(zbus::fdo::Error::Failed(format!(
                "Transaction {} already started",
                self.id
            )));
        }
        // 注册表标记已启动：清扫器不再回收它。
        self.live.lock().await.get_mut(&self.id).map(|t| t.started = true);

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

        if let Err(e) = self
            .manager
            .enqueue(ctxt_owned, self.id, role, caller, uid, task, on_done)
            .await
        {
            // 入队失败（队列满/配额）：回滚 started，对象回到 dormant，
            // 可被 Destroy 或清扫器回收。
            self.started.store(false, Ordering::SeqCst);
            self.live.lock().await.get_mut(&self.id).map(|t| t.started = false);
            return Err(enqueue_error(e));
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
        auth(&header, conn, "io.aosc.amo.refresh").await?;
        let id = self.id;
        let client = self.client.clone();
        let ctx = self.ctx.clone();
        let main_emitter = self.main_emitter.clone();
        self.begin(&header, conn, TransactionRole::Refresh, ctxt, move |ctxt| {
            Box::pin(async move {
                let (progress_tx, progress_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
                let ctxt_status = ctxt.clone();
                // 保留转发任务的句柄：生产端关闭后先 await 它，把缓冲的
                // 进度全部发完再发 ResultReport，否则客户端收到报告即
                // 返回，会丢尾部进度。
                let forwarder = tokio::spawn(async move {
                    let mut progress_rx = progress_rx;
                    while let Some(status) = progress_rx.recv().await {
                        if let Err(e) = ctxt_status.status(status).await {
                            error!(error = e.to_string(), "Failed to send refresh_status request!");
                        }
                    }
                });

                let outcome =
                    tokio::task::spawn_blocking(move || refresh_impl(progress_tx, client.clone()))
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
                if let Ok(json) = serde_json::to_string(&report)
                    && let Err(e) = ctxt.result_report(json).await
                {
                    error!("Failed to emit refresh result signal: {e}");
                }
            })
        })
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
        auth(&header, conn, "io.aosc.Amo.apply.run").await?;
        let id = self.id;
        let client = self.client.clone();
        let ctx = self.ctx.clone();
        let main_emitter = self.main_emitter.clone();
        self.begin(&header, conn, TransactionRole::ApplyChanges, ctxt, move |ctxt| {
            Box::pin(async move {
                let (progress_tx, progress_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
                let ctxt_status = ctxt.clone();
                // 保留转发任务的句柄：生产端关闭后先 await 它，把缓冲的
                // 进度全部发完再发 ResultReport，否则客户端收到报告即
                // 返回，会丢尾部进度。
                let forwarder = tokio::spawn(async move {
                    let mut progress_rx = progress_rx;
                    while let Some(event_str) = progress_rx.recv().await {
                        if let Err(e) = ctxt_status.status(event_str).await {
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
                if let Ok(json) = serde_json::to_string(&report)
                    && let Err(e) = ctxt.result_report(json).await
                {
                    error!("Failed to emit apply result signal: {e}");
                }
            })
        })
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
        let id = self.id;
        let client = self.client.clone();
        let lists_dir = self.lists_dir.clone();
        self.begin(&header, conn, TransactionRole::Simulate, ctxt, move |ctxt| {
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
                if let Ok(json) = serde_json::to_string(&report)
                    && let Err(e) = ctxt.result_report(json).await
                {
                    error!("Failed to emit simulate result signal: {e}");
                }
            })
        })
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
        self.begin(&header, conn, TransactionRole::UpdatesList, ctxt, move |ctxt| {
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
                if let Ok(json) = serde_json::to_string(&report)
                    && let Err(e) = ctxt.result_report(json).await
                {
                    error!("Failed to emit updates_list result signal: {e}");
                }
            })
        })
        .await
    }

    /// 取消排队中的本事务（PackageKit 风格，从事务对象上调）。
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
        self.manager.cancel(self.id, self.uid).await.map_err(|e| match e {
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
    /// 已启动的对象请用 Cancel 或等其自然结束（结束后自动移除）。
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
        if self.started.load(Ordering::SeqCst) {
            return Err(zbus::fdo::Error::Failed(format!(
                "Transaction {} already started",
                self.id
            )));
        }
        let path = format!("/io/aosc/Amo/Transaction/{}", self.id);
        self.server
            .remove::<TransactionObject, _>(path.as_str())
            .await
            .map_err(|e| zbus::fdo::Error::Failed(format!("Failed to destroy transaction: {e}")))?;
        self.live.lock().await.remove(&self.id);
        Ok(())
    }

    #[zbus(signal)]
    async fn status(ctxt: &SignalEmitter<'_>, status: String) -> zbus::Result<()>;

    #[zbus(signal)]
    async fn result_report(ctxt: &SignalEmitter<'_>, report: String) -> zbus::Result<()>;

    #[zbus(signal)]
    async fn transaction_state(ctxt: &SignalEmitter<'_>, state: String) -> zbus::Result<()>;
}

/// 周期清扫休眠（未启动）超时的事务对象，释放配额槽位。覆盖创建者
/// 断开或放弃对象而不显式销毁的情况。
async fn reclaim_dormant(
    live: Arc<Mutex<HashMap<u64, LiveTransaction>>>,
    server: ObjectServer,
) {
    loop {
        tokio::time::sleep(DORMANT_TIMEOUT / 2).await;
        let now = Instant::now();
        let stale: Vec<String> = {
            let mut map = live.lock().await;
            let mut removed = Vec::new();
            map.retain(|_, t| {
                if !t.started && now.duration_since(t.created_at) >= DORMANT_TIMEOUT {
                    removed.push(t.path.clone());
                    false
                } else {
                    true
                }
            });
            removed
        };
        for path in stale {
            if let Err(e) = server.remove::<TransactionObject, _>(path.as_str()).await {
                error!("Failed to reclaim dormant transaction object {path}: {e}");
            }
        }
    }
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

/// 取调用方的 D-Bus 唯一名与 Unix uid，供事务记录（GetTransactionList）使用。
async fn peer_identity(
    header: &zbus::message::Header<'_>,
    conn: &Connection,
) -> Result<(String, u32), fdo::Error> {
    let sender = header
        .sender()
        .ok_or_else(|| fdo::Error::AccessDenied("Unknown sender!".to_string()))?
        .to_owned();

    let dbus_proxy = zbus::fdo::DBusProxy::new(conn).await?;
    let uid = dbus_proxy
        .get_connection_unix_user(BusName::from(sender.clone()))
        .await?;

    Ok((sender.to_string(), uid))
}

pub async fn auth(
    header: &zbus::message::Header<'_>,
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
