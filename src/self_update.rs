//! 自我更新检测。
//!
//! 包管理器替换掉 amo 的二进制后，当前进程跑的还是旧代码。这里负责发现
//! 这件事，由 `main.rs` 在服务空闲时退出，让 systemd 在下次 D-Bus 调用时
//! 拉起新版本。

use anyhow::anyhow;
use futures::StreamExt;
use inotify::{EventMask, EventStream, Inotify, WatchMask};
use sha2::{Digest, Sha256};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// 二进制被替换后，重查服务是否空闲的间隔。
pub const IDLE_RECHECK_INTERVAL: Duration = Duration::from_secs(5);

/// 内核在 `/proc/self/exe` 失去路径引用后附加的后缀。
const DELETED_SUFFIX: &str = " (deleted)";

/// 二进制内容的 SHA-256 摘要。
type Checksum = [u8; 32];

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

        let inotify = Inotify::init()?;
        // 监视父目录而不是文件本身：包管理器用「写临时文件再 rename」替换
        // 二进制，此时对文件本身的 watch 只会收到 DELETE_SELF 事件然后失效，
        // 拿不到新文件；父目录则会报告带文件名的 MOVED_TO 事件
        inotify
            .watches()
            .add(dir, WatchMask::MOVED_TO | WatchMask::CLOSE_WRITE)?;

        let events = inotify.into_event_stream([0u8; 4096])?;

        Ok(Self {
            exe: exe.to_owned(),
            events,
        })
    }

    /// 监视注册之前二进制就已经被替换了吗。
    ///
    /// inotify 只投递注册之后发生的事件，启动期间完成的替换不会产生事件，
    /// 调用方需要据此立即退出，而不是傻等一个永不到来的通知。
    pub fn replaced_at_start(&self) -> anyhow::Result<bool> {
        running_exe_replaced(&self.exe)
    }

    /// 等待二进制被替换
    pub async fn wait_for_replacement(&mut self) -> anyhow::Result<()> {
        let Some(file_name) = self.exe.file_name().map(|name| name.to_owned()) else {
            return Err(anyhow!("{} has no file name", self.exe.display()));
        };

        loop {
            let event = self
                .events
                .next()
                .await
                .ok_or_else(|| anyhow!("inotify stream ended"))??;

            // 父目录里其它文件的事件与我们无关。
            if event.name.as_deref() != Some(file_name.as_os_str()) {
                continue;
            }

            if event
                .mask
                .intersects(EventMask::MOVED_TO | EventMask::CLOSE_WRITE)
            {
                return Ok(());
            }
        }
    }

    /// 二进制路径，用于日志。
    pub fn path(&self) -> &Path {
        &self.exe
    }
}

/// 本进程的安装路径。
///
/// 优先用 `argv[0]`：它是启动时由 systemd 传入的绝对路径（`ExecStart=`），
/// 不随后续 rename 变化。`/proc/self/exe` 则**会**跟着 rename 走——包管理
/// 器若先把运行中的二进制改名备份（如 `amo.dpkg-tmp`）再把新文件放到原
/// 路径，`current_exe()` 就指向那个备份，据此监视会认错目标，新文件的
/// MOVED_TO 事件被忽略，旧进程一直跑下去。
///
/// `argv[0]` 不可用时（被改写、相对路径、缺失）退回 `current_exe()`，此时
/// 再剥掉内核附加的 ` (deleted)` 后缀。
fn installed_path() -> anyhow::Result<PathBuf> {
    if let Some(arg0) = std::env::args_os().next()
        && Path::new(&arg0).is_absolute()
    {
        return Ok(PathBuf::from(arg0));
    }

    let (exe, _) = running_exe()?;

    Ok(exe)
}

/// 运行中的二进制路径，以及它是否已与安装路径脱钩。
///
/// 内核在运行中的 inode 失去所有路径引用后，会把 `/proc/self/exe` 报成
/// `<原路径> (deleted)`。这既说明二进制已被替换，也意味着拿到的文件名多
/// 了一截、与 inotify 事件里的名字对不上，所以要一并剥掉后缀。
///
/// Linux 上 `current_exe()` 就是 `read_link("/proc/self/exe")`。
fn running_exe() -> anyhow::Result<(PathBuf, bool)> {
    let target = std::env::current_exe()
        .map_err(|e| anyhow!("cannot determine the running executable: {e}"))?;
    let name = target
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("{} has no file name", target.display()))?;

    match name.strip_suffix(DELETED_SUFFIX) {
        Some(real_name) => Ok((target.with_file_name(real_name), true)),
        None => Ok((target, false)),
    }
}

/// 运行中的二进制是否已不同于安装路径上的文件。
///
/// 判据按可靠性排序：
///
/// 1. ` (deleted)` 后缀：运行中的 inode 已无路径引用，必然是旧版本。
/// 2. 内容摘要比对：覆盖其余所有写法——包括包管理器把旧文件改名备份
///    （`amo` → `amo.dpkg-tmp`）后再放入新文件，此时 `/proc/self/exe` 跟着
///    改名走且不带后缀，只有内容能揭穿。
///
/// 摘要读 `/proc/self/exe` 而不是 `current_exe()` 的返回值：后者带后缀时
/// 那个字面路径并不存在，打不开；且 `/proc/self/exe` 即使安装路径已被
/// unlink 或覆盖也仍可读（实测确认）。不用 inode 判定，因为号码会回收
/// 再分配，且比较结果受文件系统影响。
///
/// 安装路径此刻读不到（包管理器先移走旧文件、稍后才放入新文件）不算替换
/// 完成，返回 `false` 让调用方继续等事件；只有连自己的可执行文件都读不到
/// 才报错。
fn running_exe_replaced(installed: &Path) -> anyhow::Result<bool> {
    let (_, detached) = running_exe()?;
    if detached {
        return Ok(true);
    }

    let running = file_checksum(Path::new("/proc/self/exe"))?;
    let Ok(installed) = file_checksum(installed) else {
        // 安装路径此刻不存在，替换还没落地，等 MOVED_TO 事件即可。
        return Ok(false);
    };

    Ok(running != installed)
}

/// 文件内容的 SHA-256 摘要。
///
/// 二进制约 27 MB，实测摘要耗时不到 20 ms，且只在启动时和收到事件后各算
/// 一次，不在轮询路径上。
fn file_checksum(path: &Path) -> anyhow::Result<Checksum> {
    // 流式读取，避免把整个二进制读进内存。
    let mut file =
        std::fs::File::open(path).map_err(|e| anyhow!("cannot open {}: {e}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file
            .read(&mut buf)
            .map_err(|e| anyhow!("cannot read {}: {e}", path.display()))?;

        if n == 0 {
            break;
        }

        hasher.update(&buf[..n]);
    }

    Ok(hasher.finalize().into())
}

#[cfg(test)]
mod tests {
    use super::{DELETED_SUFFIX, SelfUpdate, file_checksum, installed_path, running_exe_replaced};
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
    fn proc_self_exe_is_readable_and_matches_the_installed_file() {
        // checksum 方案的前提：/proc/self/exe 可读，且内容与安装路径一致。
        let running = file_checksum(Path::new("/proc/self/exe")).unwrap();
        let installed = file_checksum(&std::env::current_exe().unwrap()).unwrap();
        assert_eq!(running, installed);
    }

    #[test]
    fn installed_path_is_absolute() {
        // 监视目标必须是绝对路径，否则父目录和文件名都可能不对。
        let path = installed_path().unwrap();
        assert!(path.is_absolute(), "expected absolute path, got {path:?}");
    }

    #[test]
    fn installed_path_prefers_absolute_argv0() {
        // 包管理器把运行中的二进制改名备份后，/proc/self/exe 会跟着改名走，
        // 而 argv[0] 不会——这正是要用它的原因。测试进程的 argv[0] 由
        // cargo 传入，可能不是绝对路径，此时才回退到 current_exe()。
        let arg0 = std::env::args_os().next();
        let expected = match arg0 {
            Some(arg0) if Path::new(&arg0).is_absolute() => PathBuf::from(arg0),
            _ => std::env::current_exe().unwrap(),
        };
        assert_eq!(installed_path().unwrap(), expected);
    }

    #[test]
    fn missing_file_has_no_checksum() {
        let path = temp_path("missing");
        let _ = std::fs::remove_file(&path);
        assert!(file_checksum(&path).is_err());
    }

    #[test]
    fn same_content_has_same_checksum() {
        let dir = temp_dir("same-content");
        let a = dir.join("a");
        let b = dir.join("b");
        std::fs::write(&a, b"identical").unwrap();
        std::fs::write(&b, b"identical").unwrap();
        assert_eq!(file_checksum(&a).unwrap(), file_checksum(&b).unwrap());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn different_content_changes_checksum() {
        let dir = temp_dir("different-content");
        let a = dir.join("a");
        let b = dir.join("b");
        std::fs::write(&a, b"old").unwrap();
        std::fs::write(&b, b"new").unwrap();
        assert_ne!(file_checksum(&a).unwrap(), file_checksum(&b).unwrap());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn replaced_is_false_before_any_replacement() {
        let exe = std::env::current_exe().unwrap();
        assert!(!running_exe_replaced(&exe).unwrap());
    }

    #[test]
    fn deleted_suffix_recognises_the_marker() {
        // 快速路径依赖的字符串必须与内核实际追加的一致。
        assert!("amo (deleted)".ends_with(DELETED_SUFFIX));
        assert!(!"amo".ends_with(DELETED_SUFFIX));
    }

    #[test]
    fn replaced_is_true_for_a_different_file() {
        let dir = temp_dir("replaced-different");
        let other = dir.join("other");
        std::fs::write(&other, b"not our binary").unwrap();
        assert!(running_exe_replaced(&other).unwrap());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn temporarily_missing_installed_path_is_not_replaced_yet() {
        // 包管理器先移走旧文件、稍后才放入新文件时，安装路径会短暂不存在。
        // 此时替换还没落地，应返回 false（让调用方继续等事件），而不是报错
        // ——报错会让调用方终止监视，丢掉马上就到的 MOVED_TO。
        let missing = Path::new("/nonexistent/amo-does-not-exist");
        assert!(!running_exe_replaced(missing).unwrap());
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
        assert!(
            !running_exe_replaced(&exe).unwrap(),
            "the replacement has not landed yet"
        );

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
}
