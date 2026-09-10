//! 自我更新检测。
//!
//! 包管理器替换掉 amo 的二进制后，当前进程跑的还是旧代码。这里只负责发现
//! 这件事；「何时可以退出」是服务的活动状态问题，由 `server.rs` 决定。

use anyhow::{Context, anyhow, bail};
use futures::StreamExt;
use inotify::{EventMask, EventStream, Inotify, WatchMask};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// 二进制被替换后，重查服务是否空闲的间隔。
pub const IDLE_RECHECK_INTERVAL: Duration = Duration::from_secs(5);

/// 内核在 `/proc/self/exe` 失去路径引用后附加的后缀。
const DELETED_SUFFIX: &str = " (deleted)";

/// 运行中的二进制。读这个而不读 `current_exe()`：后者带 ` (deleted)` 后缀时
/// 那个字面路径并不存在，打不开；而这里即使安装路径已被 unlink 或覆盖也仍可读
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
        // 监视父目录而不是文件本身：包管理器用「写临时文件再 rename」替换
        // 二进制，此时对文件本身的 watch 只会收到 DELETE_SELF 事件然后失效，
        // 拿不到新文件；父目录则会报告带文件名的 MOVED_TO 事件
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

    /// 监视注册之前二进制就已经被替换了吗。
    ///
    /// inotify 只投递注册之后发生的事件（实测：先替换后注册，等待窗口内收不
    /// 到任何事件；先注册后替换，事件正常到达），所以监视刚挂上时发生的替换
    /// 不会产生通知，得查一次才知道，否则会一直等下去。
    ///
    /// 与 [`Self::wait_for_replacement`] 问的不是同一件事：这里比较的是**本
    /// 进程**和安装路径，只有监视目标就是安装路径时才成立，所以由调用方在
    /// 装配监听器时问。
    pub fn replaced_at_start(&self) -> bool {
        replaced(&self.exe)
    }

    /// 等到安装路径上出现另一个文件。
    ///
    /// 事件只负责「叫醒」，是否真的被替换一律交给 [`replaced`] 判定，不按
    /// 事件类型分叉，也不听事件本身怎么说。
    pub async fn wait_for_replacement(&mut self) -> anyhow::Result<()> {
        let file_name = self
            .exe
            .file_name()
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow!("{} has no file name", self.exe.display()))?;

        // 只有两类事件值得醒过来看：
        //
        // - 队列溢出：内核丢掉了事件，其中可能就有我们等的替换，只留下这条
        //   没有名字的 IN_Q_OVERFLOW（wd 为 -1）。按名字过滤会把它当成别人家
        //   的事件丢掉，那就再也等不到通知了。
        // - 安装路径上的文件被写入或改名到位，即 MOVED_TO / CLOSE_WRITE。
        //
        // 其余（同目录其它文件）跳过。
        while let Some(event) = self.events.next().await {
            let event = event?;

            let overflow = event.mask.contains(EventMask::Q_OVERFLOW);
            let landed = event.name.as_deref() == Some(file_name.as_os_str())
                && event
                    .mask
                    .intersects(EventMask::MOVED_TO | EventMask::CLOSE_WRITE);

            if (overflow || landed) && replaced(&self.exe) {
                return Ok(());
            }
        }

        bail!("inotify stream ended")
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

    // 退回运行中的二进制。Linux 上 `current_exe()` 就是读 `/proc/self/exe`；
    // 运行中的 inode 失去路径引用后，内核把它报成 `<原路径> (deleted)`，那个
    // 文件名永远对不上 inotify 事件里的名字，所以剥掉后缀。
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

/// 安装路径上现在是另一个二进制吗。
///
/// 比较内容摘要：一个读运行中的二进制，一个读安装路径。安装路径上此刻没有
/// 文件、或打不开，都算「还没换」返回 `false`——包管理器会先移走旧文件、稍后
/// 才放入新的，这段时间应当继续等；把它当错误会让监视终止，反而丢掉随后就到的
/// 落地事件。
///
/// 新文件内容与旧文件逐字节相同时会返回 `false`，那不是遗漏：跑着的代码与新
/// 装上的完全一致，本来就没有需要重启的理由。
fn replaced(installed: &Path) -> bool {
    // 读不到安装路径可能是「不存在」（替换未落地）或权限问题，都按未完成处理。
    let Some(installed) = digest(installed).ok() else {
        return false;
    };

    digest(Path::new(RUNNING_EXE)).ok() != Some(installed)
}

/// 文件内容的 SHA-256 摘要。
///
/// 二进制约 27 MB，实测不到 20 ms，且只在启动时和个别事件之后各算一次，不在
/// 轮询路径上。流式读取，不把整个二进制读进内存。
fn digest(path: &Path) -> anyhow::Result<[u8; 32]> {
    let mut file = File::open(path).map_err(|e| anyhow!("cannot open {}: {e}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];

    loop {
        let n = file
            .read(&mut buf)
            .map_err(|e| anyhow!("cannot read {}: {e}", path.display()))?;

        if n == 0 {
            return Ok(hasher.finalize().into());
        }

        hasher.update(&buf[..n]);
    }
}

#[cfg(test)]
mod tests {
    use super::{DELETED_SUFFIX, SelfUpdate, digest, installed_path, replaced};
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
    fn proc_self_exe_is_readable_and_matches_the_installed_file() {
        // 判据的前提：/proc/self/exe 可读，且内容与安装路径一致。
        let running = digest(Path::new("/proc/self/exe")).unwrap();
        let installed = digest(&std::env::current_exe().unwrap()).unwrap();
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
    fn missing_file_has_no_digest() {
        let path = temp_path("missing");
        let _ = std::fs::remove_file(&path);
        assert!(digest(&path).is_err());
    }

    #[test]
    fn same_content_has_same_digest() {
        let dir = temp_dir("same-content");
        let a = dir.join("a");
        let b = dir.join("b");
        std::fs::write(&a, b"identical").unwrap();
        std::fs::write(&b, b"identical").unwrap();
        assert_eq!(digest(&a).unwrap(), digest(&b).unwrap());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn different_content_changes_digest() {
        let dir = temp_dir("different-content");
        let a = dir.join("a");
        let b = dir.join("b");
        std::fs::write(&a, b"old").unwrap();
        std::fs::write(&b, b"new").unwrap();
        assert_ne!(digest(&a).unwrap(), digest(&b).unwrap());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn replaced_is_false_before_any_replacement() {
        let exe = std::env::current_exe().unwrap();
        assert!(!replaced(&exe));
    }

    #[test]
    fn a_copy_of_the_running_binary_is_not_replaced() {
        // 内容一样就不算被换：跑着的代码与新装上的逐字节相同，没有要重启的
        // 东西。（新文件与旧文件内容一致但 inode 不同时，也是这处理。）
        let dir = temp_dir("identical-copy");
        let copy = dir.join("amo");
        std::fs::copy(std::env::current_exe().unwrap(), &copy).unwrap();
        assert!(!replaced(&copy));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn deleted_suffix_recognises_the_marker() {
        // 文件名剥后缀依赖的字符串必须与内核实际追加的一致。
        assert!("amo (deleted)".ends_with(DELETED_SUFFIX));
        assert!(!"amo".ends_with(DELETED_SUFFIX));
    }

    #[test]
    fn replaced_is_true_for_a_different_file() {
        let dir = temp_dir("replaced-different");
        let other = dir.join("other");
        std::fs::write(&other, b"not our binary").unwrap();
        assert!(replaced(&other));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn temporarily_missing_installed_path_is_not_replaced_yet() {
        // 包管理器先移走旧文件、稍后才放入新文件时，安装路径会短暂不存在。
        // 此时替换还没落地，应返回 false（让调用方继续等事件），而不是报错
        // ——报错会让调用方终止监视，丢掉马上就到的 MOVED_TO。
        let missing = Path::new("/nonexistent/amo-does-not-exist");
        assert!(!replaced(missing));
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
        assert!(!replaced(&exe), "the replacement has not landed yet");

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

        // 替换事件已被丢掉，只能靠溢出后的重查发现。
        tokio::time::timeout(Duration::from_secs(5), watcher.wait_for_replacement())
            .await
            .expect("the replacement was swallowed by the overflow")
            .unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
