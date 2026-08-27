//! 主接口 `io.aosc.Amo1`：搜索 / 描述 / 事务列表 / 缓存失效 / 创建事务对象。
//!
//! 事务本身是独立对象（见 `transaction_object`），刷新逻辑见 `refresh`，
//! 调用方身份见 `auth`。

use crate::auth::peer_identity;
use crate::refresh::{lists_files_state, refresh_if_stale, IndexInputs, RefreshContext};
use crate::transaction::TransactionManager;
use crate::transaction_object::{
    LiveTransaction, TransactionObject, MAX_LIVE_PER_UID, MAX_LIVE_TRANSACTIONS,
    reclaim_dormant,
};
use apt_auth_config::{AuthConfig, reqwuest::AuthMiddleware};
use chrono::Datelike;
use oma_apt_pkg::{AptConfig, AptDb, DpkgState, IndiciumSearch, OmaSearch, SearchType};
use oma_fetch::reqwest::ClientBuilder;
use reqwest_middleware::ClientWithMiddleware;
use std::{
    collections::HashMap,
    sync::{
        Arc, OnceLock, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};
use tokio::sync::Mutex;
use tracing::error;
use zbus::{
    fdo, interface,
    names::BusName,
    object_server::{ObjectServer, SignalEmitter},
    zvariant::OwnedObjectPath,
};

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

fn current_date_val() -> u64 {
    let now = chrono::Local::now();
    let yy = now.year() as u64;
    let mm = now.month() as u64;
    let dd = now.day() as u64;

    yy * 10000 + mm * 100 + dd
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

        // 配额检查与槽位预占在同一把锁内完成：并发 CreateTransaction
        // 串行化，不会出现多个调用都观察到低于上限的 map 而超限创建
        // （全局 64 / 每用户 16）。对象注册失败时回滚预占的槽位。
        {
            let mut live = self.live.lock().await;
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
            live.insert(
                id,
                LiveTransaction {
                    path: path.clone(),
                    uid,
                    created_at: Instant::now(),
                    started: false,
                },
            );
        }

        if let Err(e) = server.at(&*path, obj).await {
            // 注册失败：回滚预占的槽位。
            self.live.lock().await.remove(&id);
            return Err(fdo::Error::Failed(format!("Failed to create transaction: {e}")));
        }

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

