//! 退出协议：不再接纳新工作、等已经接纳的收尾、什么时候算可以退。
//!
//! 这里只描述协议，不认识 D-Bus 也不认识包操作：`server` 在请求路径上问
//! [`Exit::pending`]，`main` 等 [`Exit::wait`]。

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::{Mutex, Notify};

/// 退出协议，分两步，因为两件事该发生的时机不同：
///
/// 1. **发现二进制被替换** → 不再接受新工作（[`Exit::pending`]）。要尽早，不能
///    等空闲：监视器为了不打断在跑的操作而等锁，这期间如果还继续接纳新工作，
///    持续不断的请求就可能让它永远等不到空闲。
/// 2. **已接纳的工作收尾** → 通知 `main` 退出（[`Exit::wait`]）。此刻两把活动锁
///    都空着，关掉不会打断正在上报结果的任务。
///
/// 通知用 `notify_one` 而不是 `notify_waiters`：它会留下一个 permit，所以通知
/// 早于 `main` 开始等也不会丢。
#[derive(Default)]
pub struct Exit {
    pending: AtomicBool,
    notified: Notify,
}

impl Exit {
    /// 拒绝新工作的理由。写在一处：同一种状态经 D-Bus 或经 `anyhow` 上报，
    /// 文案一致。
    pub const REASON: &'static str = "The service is restarting to pick up an update";

    /// 是否已发现二进制被替换。请求侧在**取到活动锁之后**问这个。
    pub fn pending(&self) -> bool {
        self.pending.load(Ordering::Acquire)
    }

    /// 记下「二进制已被替换」。监视器一发现就调，不等空闲。
    pub fn mark_replaced(&self) {
        self.pending.store(true, Ordering::Release);
    }

    /// 等到「可以退了」。`main` 用它作为退出信号的一路。
    pub async fn wait(&self) {
        self.notified.notified().await;
    }

    /// 通知 `main` 可以退了。调用方必须持活动锁，见 [`decide_exit`]。
    fn notify_exit(&self) {
        self.notified.notify_one();
    }
}

/// 若此刻确实空闲，就通知退出并返回 `true`。
///
/// 空闲 = 两把活动锁都能拿到手 ⇒ 此刻既没有包操作、也没有索引刷新在进行。
/// 通知发生在**持锁状态下**：请求侧的 `try_lock` 若已经失败，它就已经在跑，
/// 而我们等到它跑了；请求若之后才开始，`Exit::pending` 早已置位，会在取到锁后
/// 立刻拒绝。不会再有任何工作以「刚开始」的身份通过。
///
/// 不负责置位 `pending`（那是监视器一发现就做的 [`Exit::mark_replaced`]）：
/// 这里的等待可能持续到很久以后，而停止接纳新工作不能等那么久。
pub(crate) fn decide_exit(
    run_lock: &Arc<Mutex<()>>,
    refresh_lock: &Arc<Mutex<()>>,
    exit: &Exit,
) -> bool {
    // 必须顺序获取，不要写成一个元组：那样第一个成功后第二个失败，第一个
    // guard 会随表达式结束而丢弃、锁又放开了。
    let Ok(run_guard) = run_lock.clone().try_lock_owned() else {
        return false;
    };
    let Ok(refresh_guard) = refresh_lock.clone().try_lock_owned() else {
        return false;
    };

    exit.notify_exit();
    drop((run_guard, refresh_guard));

    true
}

#[cfg(test)]
mod tests {
    use super::{Exit, decide_exit};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn exit_waits_for_the_work_that_was_already_admitted() {
        // 发现被替换不等于可以退：已经接纳的工作要让它跑完。这里用 refresh 锁
        // 代表「正在重建索引」——注意此时**不能**再调 begin_refresh，那条路会
        // 排队等锁（查询路径的契约），而排队不返回。
        let run_lock = Arc::new(Mutex::new(()));
        let refresh_lock = Arc::new(Mutex::new(()));
        let exit = Exit::default();

        let in_progress = refresh_lock.clone().try_lock_owned().unwrap();
        exit.mark_replaced();

        assert!(
            !decide_exit(&run_lock, &refresh_lock, &exit),
            "an operation is in progress, so exit has to wait for it"
        );

        // 在跑的那次结束了，这才谈得上退出。
        drop(in_progress);
        assert!(decide_exit(&run_lock, &refresh_lock, &exit));
    }

    #[tokio::test]
    async fn seeing_a_replacement_does_not_yet_tell_main_to_exit() {
        // 两阶段分开，正是为了不让持续的请求把退出无限延后，同时又不打断已经
        // 在跑的任务：发现被替换只是停止接纳新工作，退出要等 decide_exit 拿到
        // 两把锁。
        let exit = Exit::default();
        exit.mark_replaced();

        assert!(exit.pending());
        assert!(
            tokio::time::timeout(Duration::from_millis(100), exit.wait())
                .await
                .is_err(),
            "main must keep waiting while the admitted work drains"
        );
    }

    #[tokio::test]
    async fn a_decision_made_before_waiting_is_not_lost() {
        // `notify_one` 会留下一个 permit，所以「可以退了」发生在 main 开始
        // 等之前也不会丢。这正是它取代 channel 的原因。
        let run_lock = Arc::new(Mutex::new(()));
        let refresh_lock = Arc::new(Mutex::new(()));
        let exit = Exit::default();

        assert!(decide_exit(&run_lock, &refresh_lock, &exit));

        tokio::time::timeout(Duration::from_millis(100), exit.wait())
            .await
            .expect("the permit must survive a decision made before the wait");
    }

    #[test]
    fn seeing_a_replacement_is_observable_by_requests() {
        let exit = Exit::default();
        assert!(!exit.pending());

        exit.mark_replaced();
        assert!(exit.pending());
    }

}
