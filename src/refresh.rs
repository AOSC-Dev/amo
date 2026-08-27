//! 搜索索引刷新：输入快照（lists + dpkg status）与重建逻辑。
//!
//! `RefreshContext` 供 `refresh` / `apply_changes` / `invalidate_cache` /
//! 查询路径共享。

use crate::server::AmoSignals;
use anyhow::anyhow;
use oma_apt_pkg::{
    AptConfig, AptDb, DpkgState, IndiciumSearch, SearchType, apt_sources::SourceLookup,
};
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;
use tracing::{error, info};
use zbus::object_server::SignalEmitter;

/// `refresh` / `apply_changes` / `invalidate_cache` / 查询路径共享的索引
/// 刷新状态。
#[derive(Clone)]
pub(crate) struct RefreshContext {
    pub(crate) searcher: Arc<RwLock<IndiciumSearch>>,
    pub(crate) apt_config: Arc<AptConfig>,
    pub(crate) refresh_lock: Arc<Mutex<()>>,
    pub(crate) index_inputs: Arc<Mutex<Option<IndexInputs>>>,
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


/// 搜索索引所基于的输入快照：lists 目录中各索引文件的 (文件名, 大小, 整秒
/// mtime) 与 dpkg status 的 mtime。这些输入与当前一致时，索引才算是最新的。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IndexInputs {
    pub(crate) lists: Vec<(String, u64, i64)>,
    pub(crate) status_mtime: Option<std::time::SystemTime>,
}

/// 当前 lists 目录状态：由当前源产生且存在的索引文件的 (文件名, 大小,
/// 整秒 mtime)，粒度与 oma-apt-pkg 的缓存有效性检查一致。
pub(crate) fn lists_files_state(apt_config: &AptConfig) -> Vec<(String, u64, i64)> {
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
pub(crate) async fn refresh_if_stale(
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

