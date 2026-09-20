//! 自我更新检测

use anyhow::{Context, anyhow, bail};
use digest_io::IoWrapper;
use futures::StreamExt;
use inotify::{EventMask, EventStream, Inotify, WatchMask};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// 二进制被替换后，重新查询服务是否空闲的间隔。
pub const IDLE_RECHECK_INTERVAL: Duration = Duration::from_secs(5);

/// 内核在 `/proc/self/exe` 失去路径引用后附加的后缀。
const DELETED_SUFFIX: &str = " (deleted)";

/// 运行中的二进制。读这个而不读 `current_exe()`：安装路径被覆盖后，后者带着
/// ` (deleted)` 后缀，那个字面路径打不开；这里即使安装路径已被 unlink 也仍可读
/// （实测确认）。
const RUNNING_EXE: &str = "/proc/self/exe";

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

        // 监视父目录而不是文件本身。原因：包管理器用「写临时文件再 rename」
        // 替换，对文件本身的 watch 只会收到 DELETE_SELF 然后失效，拿不到新文件
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

    /// 等到 amo 二进制被替换。
    pub async fn wait_for_replacement(&mut self) -> anyhow::Result<()> {
        let file_name = self
            .exe
            .file_name()
            .ok_or_else(|| anyhow!("{} has no file name", self.exe.display()))?;

        // 安装路径上的文件被写入或改名到位（MOVED_TO / CLOSE_WRITE）就是答案，
        // 内容相同也算换过（重装同版本多退一次，有意接受）。
        //
        // 队列溢出是唯一要补判的一条：内核丢掉了事件，其中可能正有这次替换，
        // 只剩这条没有名字的溢出事件。这时用内容摘要代替事件——跑着的和装着的
        // 摘要一致就当没换，继续等。
        while let Some(event) = self.events.next().await {
            let event = event?;

            let landed = event.name.as_deref().is_some_and(|name| name == file_name)
                && event
                    .mask
                    .intersects(EventMask::MOVED_TO | EventMask::CLOSE_WRITE);

            let overflowed = event.mask.contains(EventMask::Q_OVERFLOW)
                && replaced(Path::new(RUNNING_EXE), &self.exe);

            if landed || overflowed {
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

/// 跑着的这份（`running`）和装着的这份（`installed`）是两份不同的文件吗。
///
/// 比的是内容摘要。读不到 `installed` 当作「还没落地」，返回 `false` 继续等——
/// 包管理器会先把旧文件移走、过一会儿才放入新的，这段时间不值得退出；读不到
/// `running` 则当作已经被换。
///
/// 判据是内容而不是 inode：同一份内容（重装同样的字节）不算被换，原地重写改了
/// 内容则算。
///
/// 只有队列溢出、没有事件可依时才问（见 [`SelfUpdate::wait_for_replacement`]）。
fn replaced(running: &Path, installed: &Path) -> bool {
    let Ok(installed) = digest(installed) else {
        return false;
    };

    digest(running).ok() != Some(installed)
}

/// 文件内容的 SHA-256 摘要。
///
/// 哈希器经 `digest-io` 包成写入端，文件内容直接 `io::copy` 进去：流式读取，
/// 不整份进内存。约 27 MB 实测不到 20 ms，而且只在队列溢出后各算一次，不在
/// 常规路径上。
fn digest(path: &Path) -> anyhow::Result<[u8; 32]> {
    let mut file = File::open(path).map_err(|e| anyhow!("cannot open {}: {e}", path.display()))?;
    let mut hasher = IoWrapper(Sha256::new());

    io::copy(&mut file, &mut hasher).map_err(|e| anyhow!("cannot read {}: {e}", path.display()))?;

    Ok(hasher.0.finalize().into())
}

#[cfg(test)]
mod tests {
    use super::{DELETED_SUFFIX, SelfUpdate, digest, installed_path, replaced};
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

    #[test]
    fn identical_contents_have_the_same_digest() {
        // 内容跨过读缓冲边界，确认分块读取不会把摘要算歪。
        let dir = temp_dir("digest-equal");
        let left = dir.join("left");
        let right = dir.join("right");
        let bytes = vec![0x5a; 20 * 1024];
        std::fs::write(&left, &bytes).unwrap();
        std::fs::write(&right, &bytes).unwrap();
        assert_eq!(digest(&left).unwrap(), digest(&right).unwrap());

        // 空文件也有摘要，而且相同。
        std::fs::write(&left, b"").unwrap();
        std::fs::write(&right, b"").unwrap();
        assert_eq!(digest(&left).unwrap(), digest(&right).unwrap());

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn one_differing_byte_changes_the_digest() {
        let dir = temp_dir("digest-one-byte");
        let left = dir.join("left");
        let right = dir.join("right");
        let mut bytes = vec![0x5a; 20 * 1024];
        std::fs::write(&left, &bytes).unwrap();
        bytes[10 * 1024] ^= 0xff;
        std::fs::write(&right, &bytes).unwrap();

        assert_ne!(digest(&left).unwrap(), digest(&right).unwrap());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_length_difference_changes_the_digest() {
        let dir = temp_dir("digest-length");
        let left = dir.join("left");
        let right = dir.join("right");
        std::fs::write(&left, vec![0x5a; 1024]).unwrap();
        std::fs::write(&right, vec![0x5a; 1025]).unwrap();

        assert_ne!(digest(&left).unwrap(), digest(&right).unwrap());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn the_same_contents_are_not_a_replacement() {
        // 溢出的那波写入与二进制无关：摘要一致，继续等。
        let dir = temp_dir("replaced-same");
        let running = dir.join("running");
        let installed = dir.join("installed");
        std::fs::write(&running, b"same bytes").unwrap();
        std::fs::write(&installed, b"same bytes").unwrap();

        assert!(!replaced(&running, &installed));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn different_contents_are_a_replacement() {
        let dir = temp_dir("replaced-different");
        let running = dir.join("running");
        let installed = dir.join("installed");
        std::fs::write(&running, b"old bytes").unwrap();
        std::fs::write(&installed, b"new bytes").unwrap();

        assert!(replaced(&running, &installed));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_missing_installed_file_is_not_a_replacement() {
        // 安装路径上暂时没有文件 = 替换还没落地：继续等，随后到的落地事件会
        // 叫醒监视。
        let dir = temp_dir("replaced-missing");
        let running = dir.join("running");
        let missing = dir.join("installed");
        std::fs::write(&running, b"old bytes").unwrap();

        assert!(!replaced(&running, &missing));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_missing_running_file_counts_as_replaced() {
        // 跑着的那份读不到就没法比，宁可多退一次。
        let dir = temp_dir("replaced-no-running");
        let running = dir.join("running");
        let installed = dir.join("installed");
        std::fs::write(&installed, b"bytes").unwrap();

        assert!(replaced(&running, &installed));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn the_running_binary_can_be_opened() {
        // 判据要从这里读；打不开的话每次溢出都会被当成「已替换」，白白退出。
        assert!(std::fs::File::open(super::RUNNING_EXE).is_ok());
    }
}
