//! 自我更新检测。
//!
//! 包管理器替换掉 amo 的二进制后，当前进程跑的还是旧代码。这里负责发现
//! 这件事，由 `main.rs` 在服务空闲时退出，让 systemd 在下次 D-Bus 调用时
//! 拉起新版本。

use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// 检查间隔。目标只是尽快发现二进制被替换，不需要毫秒级响应。
pub const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// 二进制内容的 SHA-256 摘要。
type Checksum = [u8; 32];

/// 监视当前进程自己的二进制。
pub struct SelfUpdate {
    exe: PathBuf,
    checksum: Option<Checksum>,
}

impl SelfUpdate {
    /// 记下当前二进制的内容摘要。
    pub fn watch() -> anyhow::Result<Self> {
        let exe = std::env::current_exe()?;
        let checksum = file_checksum(&exe);
        Ok(Self { exe, checksum })
    }

    /// 二进制是否已被替换。
    pub fn replaced(&self) -> bool {
        checksum_changed(self.checksum, file_checksum(&self.exe))
    }

    /// 二进制路径，用于日志。
    pub fn path(&self) -> &Path {
        &self.exe
    }
}

/// 摘要是否变了。任一侧读不到文件都算没变：宁可多跑一会儿，也不要因为
/// 读不到文件就无故重启服务。
fn checksum_changed(before: Option<Checksum>, now: Option<Checksum>) -> bool {
    matches!((before, now), (Some(before), Some(now)) if before != now)
}

/// 文件内容的 SHA-256 摘要。
///
/// 用内容而不是 inode / mtime：重装同一个版本、或换了构建但源码未变时，
/// 文件会被整个换掉（inode 变）而内容不变，此时没有必要重启服务。反过来，
/// 只要内容真的变了就一定检测得到，不依赖包管理器怎么写文件。
///
/// 二进制约 27 MB，实测摘要耗时不到 20 ms，对 5 秒的轮询间隔可以忽略。
fn file_checksum(path: &Path) -> Option<Checksum> {
    use std::io::Read;

    // 流式读取，避免把整个二进制读进内存。
    let mut file = std::fs::File::open(path).ok()?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf).ok()?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Some(hasher.finalize().into())
}

#[cfg(test)]
mod tests {
    use super::{checksum_changed, file_checksum};

    /// 临时目录里的唯一路径。
    fn temp_path(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("amo-self-update-{tag}-{}", std::process::id()))
    }

    #[test]
    fn missing_file_has_no_checksum() {
        let path = temp_path("missing");
        let _ = std::fs::remove_file(&path);
        assert_eq!(file_checksum(&path), None);
    }

    #[test]
    fn rewriting_in_place_keeps_checksum() {
        let path = temp_path("in-place");
        std::fs::write(&path, b"same contents").unwrap();
        let before = file_checksum(&path);

        // 原地重写相同内容：内容没变，不该被当成「换了个新版本」。
        std::fs::write(&path, b"same contents").unwrap();

        assert!(!checksum_changed(before, file_checksum(&path)));
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn rename_over_same_content_keeps_checksum() {
        let path = temp_path("rename-same");
        let staged = temp_path("rename-same-staged");
        std::fs::write(&path, b"same contents").unwrap();
        let before = file_checksum(&path);

        // 包管理器的典型写法：写临时文件再 rename 覆盖。inode 变了，
        // 但内容一样，重装同一版本时不该重启服务。
        std::fs::write(&staged, b"same contents").unwrap();
        std::fs::rename(&staged, &path).unwrap();

        assert!(!checksum_changed(before, file_checksum(&path)));
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn different_content_changes_checksum() {
        let path = temp_path("different");
        let staged = temp_path("different-staged");
        std::fs::write(&path, b"old").unwrap();
        let before = file_checksum(&path);

        std::fs::write(&staged, b"new").unwrap();
        std::fs::rename(&staged, &path).unwrap();

        assert!(checksum_changed(before, file_checksum(&path)));
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn unknown_checksum_never_counts_as_change() {
        let sum = [7u8; 32];
        assert!(!checksum_changed(None, Some(sum)));
        assert!(!checksum_changed(Some(sum), None));
        assert!(!checksum_changed(None, None));
    }
}
