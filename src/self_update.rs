//! 自我更新检测。
//!
//! 包管理器替换掉 amo 的二进制后，当前进程跑的还是旧代码。这里负责发现
//! 这件事，由 `main.rs` 在服务空闲时退出，让 systemd 在下次 D-Bus 调用时
//! 拉起新版本。

use anyhow::anyhow;
use futures::StreamExt;
use inotify::{EventMask, EventStream, Inotify, WatchMask};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// 二进制被替换后，重查服务是否空闲的间隔。
pub const IDLE_RECHECK_INTERVAL: Duration = Duration::from_secs(5);

/// 监视当前进程自己的二进制。
pub struct SelfUpdate {
    exe: PathBuf,
    events: EventStream<[u8; 4096]>,
}

impl SelfUpdate {
    /// 开始监视当前进程自己的二进制。
    pub fn watch() -> anyhow::Result<Self> {
        Self::watch_path(&std::env::current_exe()?)
    }

    /// 监视指定路径的二进制
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

#[cfg(test)]
mod tests {
    use super::SelfUpdate;
    use std::time::Duration;

    /// 临时目录里的唯一路径。
    fn temp_path(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("amo-self-update-{tag}-{}", std::process::id()))
    }

    /// 建一个只属于本用例的目录，避免 inotify 看到其它用例的文件事件。
    fn temp_dir(tag: &str) -> std::path::PathBuf {
        let dir = temp_path(tag);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// 用 rename 覆盖替换文件，模拟包管理器的写法。
    fn replace_via_rename(path: &std::path::Path, contents: &[u8]) {
        let staged = path.with_extension("staged");
        std::fs::write(&staged, contents).unwrap();
        std::fs::rename(&staged, path).unwrap();
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
