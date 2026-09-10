//! 自我更新检测

use anyhow::{Context, anyhow, bail};
use futures::StreamExt;
use inotify::{EventMask, EventStream, Inotify, WatchMask};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// 二进制被替换后，重新查询服务是否空闲的间隔。
pub const IDLE_RECHECK_INTERVAL: Duration = Duration::from_secs(5);

/// 内核在 `/proc/self/exe` 失去路径引用后附加的后缀。
const DELETED_SUFFIX: &str = " (deleted)";

/// 监视当前进程自己的二进制。
pub struct SelfUpdate {
    exe: PathBuf,
    events: EventStream<[u8; 4096]>,
}

impl SelfUpdate {
    /// 开始监视当前进程自己的二进制。
    pub fn watch() -> anyhow::Result<Self> {
        Self::watch_path(&installed_path()?)
    }

    /// 监视指定路径的二进制。
    fn watch_path(exe: &Path) -> anyhow::Result<Self> {
        let dir = exe
            .parent()
            .ok_or_else(|| anyhow!("{} has no parent directory", exe.display()))?;

        let inotify = Inotify::init().context("cannot create an inotify instance")?;

        // 监视父目录而不是文件本身。两个原因：包管理器用「写临时文件再 rename」
        // 替换，对文件本身的 watch 只会收到 DELETE_SELF 然后失效，拿不到新文件；
        // 而 GFileMonitor（PackageKit 用的那套）监视单个文件时也会在文件被替换后
        // 失效，挂到父目录上则没有这个失效问题。
        inotify
            .watches()
            .add(dir, WatchMask::MOVED_TO | WatchMask::CLOSE_WRITE)
            .with_context(|| format!("cannot watch {}", dir.display()))?;

        let events = inotify
            .into_event_stream([0u8; 4096])
            .context("cannot start the inotify event stream")?;

        Ok(Self {
            exe: exe.to_owned(),
            events,
        })
    }

    /// 等到安装路径被改动。
    pub async fn wait_for_replacement(&mut self) -> anyhow::Result<()> {
        let file_name = self
            .exe
            .file_name()
            .ok_or_else(|| anyhow!("{} has no file name", self.exe.display()))?;

        // 只有两类事件会触发我们关心的逻辑：
        //
        // - 队列溢出：内核丢掉了事件，其中可能就有我们等的替换，只留下这条
        //   没有名字的 IN_Q_OVERFLOW（wd 为 -1）。按名字过滤会把它当成其他
        //   事件丢掉，那就再也等不到通知了。宁可多退一次，也别漏。
        // - 安装路径上的文件被写入或改名到位，即 MOVED_TO / CLOSE_WRITE。
        //
        // 其余（同目录其它文件）跳过——它们的名字对不上。
        while let Some(event) = self.events.next().await {
            let event = event?;

            let overflow = event.mask.contains(EventMask::Q_OVERFLOW);
            let landed = event.name.as_deref().is_some_and(|name| name == file_name)
                && event
                    .mask
                    .intersects(EventMask::MOVED_TO | EventMask::CLOSE_WRITE);

            if overflow || landed {
                return Ok(());
            }
        }

        bail!("inotify event stream ended")
    }

    /// 二进制路径，用于日志。
    pub fn path(&self) -> &Path {
        &self.exe
    }
}

/// 本进程的安装路径
///
/// 就是 `current_exe()`（Linux 上等于读 `/proc/self/exe`），去掉内核附加的
/// ` (deleted)` 后缀。这个后缀必须去掉：监视靠**文件名**匹配 inotify 事件，
/// 名字带着后缀就永远对不上。
///
/// dpkg 装新版本时做了什么（源码 `src/main/archives.c` 的 `tarobject`，与实测
/// 一致）：先把旧文件 `link` 一份硬链接备份成 `.dpkg-tmp`，再把 `.dpkg-new`
/// 改名盖到原路径上，最后删掉那份硬链接。硬链接无非是给同一个文件再起一个名字，
/// 运行中的文件并没有离开原路径——所以 `/proc/self/exe` 报出来的就是安装路径
/// 本身，只是多了个 ` (deleted)`。dpkg 会把旧文件 `rename` 挪走的情况只有目录。
fn installed_path() -> anyhow::Result<PathBuf> {
    let target = std::env::current_exe()
        .map_err(|e| anyhow!("cannot determine the running executable: {e}"))?;

    let name = target
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("{} has no file name", target.display()))?;

    Ok(match name.strip_suffix(DELETED_SUFFIX) {
        Some(real_name) => target.with_file_name(real_name),
        None => target,
    })
}

#[cfg(test)]
mod tests {
    use super::{DELETED_SUFFIX, SelfUpdate, installed_path};
    use futures::StreamExt;
    use inotify::EventMask;
    use std::ffi::OsStr;
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    /// 临时目录里的唯一路径。
    fn temp_path(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("amo-self-update-{tag}-{}", std::process::id()))
    }

    /// 建一个只属于本用例的目录，避免 inotify 看到其它用例的文件事件。
    fn temp_dir(tag: &str) -> PathBuf {
        let dir = temp_path(tag);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// 用 rename 覆盖替换文件，模拟包管理器的写法。
    fn replace_via_rename(path: &Path, contents: &[u8]) {
        let staged = path.with_extension("staged");
        std::fs::write(&staged, contents).unwrap();
        std::fs::rename(&staged, path).unwrap();
    }

    #[test]
    fn installed_path_is_absolute() {
        // 监视目标必须是绝对路径，否则父目录和文件名都可能不对。
        let path = installed_path().unwrap();
        assert!(path.is_absolute(), "expected absolute path, got {path:?}");
    }

    #[test]
    fn installed_path_strips_the_deleted_suffix() {
        // 剥后缀依赖的字符串必须与内核实际追加的一致；带后缀的名字永远对不上
        // inotify 事件里的名字。
        assert!("amo (deleted)".ends_with(DELETED_SUFFIX));
        assert!(!"amo".ends_with(DELETED_SUFFIX));
    }

    #[tokio::test]
    async fn rename_replacement_is_detected() {
        let dir = temp_dir("inotify-rename");
        let exe = dir.join("amo");
        std::fs::write(&exe, b"old").unwrap();
        let mut watcher = SelfUpdate::watch_path(&exe).unwrap();

        replace_via_rename(&exe, b"new");

        tokio::time::timeout(Duration::from_secs(5), watcher.wait_for_replacement())
            .await
            .expect("timed out waiting for replacement")
            .unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn identical_content_still_counts() {
        // 事件就是答案，不做验证：内容与原文件逐字节相同也算被换过，于是会多
        // 退一次。这是有意接受的代价——退出很便宜，而漏掉真替换的代价是旧进程
        // 一直服务下去。
        let dir = temp_dir("inotify-identical");
        let exe = dir.join("amo");
        std::fs::write(&exe, b"same").unwrap();
        let mut watcher = SelfUpdate::watch_path(&exe).unwrap();

        replace_via_rename(&exe, b"same");

        tokio::time::timeout(Duration::from_secs(5), watcher.wait_for_replacement())
            .await
            .expect("an event on the installed path is enough on its own")
            .unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn in_place_rewrite_is_detected() {
        let dir = temp_dir("inotify-in-place");
        let exe = dir.join("amo");
        std::fs::write(&exe, b"old").unwrap();
        let mut watcher = SelfUpdate::watch_path(&exe).unwrap();

        // 原地重写会触发 CLOSE_WRITE。
        std::fs::write(&exe, b"new").unwrap();

        tokio::time::timeout(Duration::from_secs(5), watcher.wait_for_replacement())
            .await
            .expect("timed out waiting for replacement")
            .unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn replacement_after_a_gap_is_detected() {
        // dpkg 有时先把旧文件移走、过一会儿才放入新文件。移走的那一刻安装
        // 路径不存在，但监视必须继续，才能收到随后的 MOVED_TO。
        let dir = temp_dir("inotify-gap");
        let exe = dir.join("amo");
        std::fs::write(&exe, b"old").unwrap();
        let mut watcher = SelfUpdate::watch_path(&exe).unwrap();

        std::fs::rename(&exe, dir.join("amo.dpkg-tmp")).unwrap();
        assert!(!exe.exists(), "installation path should be absent now");

        // 新文件稍后到位。
        std::thread::sleep(Duration::from_millis(100));
        std::fs::write(&exe, b"new").unwrap();

        tokio::time::timeout(Duration::from_secs(5), watcher.wait_for_replacement())
            .await
            .expect("timed out waiting for replacement")
            .unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn unrelated_files_in_directory_are_ignored() {
        let dir = temp_dir("inotify-unrelated");
        let exe = dir.join("amo");
        std::fs::write(&exe, b"old").unwrap();
        let mut watcher = SelfUpdate::watch_path(&exe).unwrap();

        // 同目录下其它文件变动不该被当成自身被替换。
        std::fs::write(dir.join("other"), b"noise").unwrap();
        replace_via_rename(&dir.join("other"), b"more noise");

        let result =
            tokio::time::timeout(Duration::from_millis(500), watcher.wait_for_replacement()).await;
        assert!(
            result.is_err(),
            "unrelated files must not trigger a restart"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn replacement_is_detected_after_a_queue_overflow() {
        let dir = temp_dir("inotify-overflow");
        let exe = dir.join("amo");
        std::fs::write(&exe, b"old").unwrap();
        let mut watcher = SelfUpdate::watch_path(&exe).unwrap();
        // 再开一个实例，只用来验证前提。它和被测实例同时被灌爆、队列内容
        // 相同，但读它不会动到被测实例的事件流——那条流必须原封不动地留给
        // `wait_for_replacement`：溢出事件只投递一次，被读掉就没了。
        let mut witness = SelfUpdate::watch_path(&exe).unwrap();

        // 灌入远超内核队列容量（`fs.inotify.max_queued_events`）的事件，期间
        // 不读事件流，把队列挤爆；替换发生在此之后，事件因而被丢掉。
        let cap: usize = std::fs::read_to_string("/proc/sys/fs/inotify/max_queued_events")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(16384);
        for i in 0..cap + cap / 2 {
            std::fs::write(dir.join(format!("noise-{i}")), b"x").unwrap();
        }
        replace_via_rename(&exe, b"new");

        let mut overflowed = false;
        let mut survived = false;
        while !overflowed {
            let Some(Ok(event)) = witness.events.next().await else {
                panic!("the witness stream ended before the overflow event");
            };
            if event.mask.contains(EventMask::Q_OVERFLOW) {
                // 溢出事件没有文件名，也没有归属的 watch（内核给的是 -1）。
                assert_eq!(event.name, None, "an overflow event carries no name");
                overflowed = true;
            } else if event.name.as_deref() == Some(OsStr::new("amo")) {
                survived = true;
            }
        }
        assert!(
            !survived,
            "the replacement event made it into the queue, so nothing was lost \
             and this test would pass without the overflow handling"
        );

        // 替换事件已被丢掉，只剩那条没有名字的溢出事件；必须据此退出。
        tokio::time::timeout(Duration::from_secs(5), watcher.wait_for_replacement())
            .await
            .expect("the overflow must be treated as a possible replacement")
            .unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
