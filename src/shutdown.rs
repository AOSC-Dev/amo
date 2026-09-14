//! 退出协议：不再接纳新工作、退出时的清理工作
//!
//! 一次退出分两步（见 [`Exit`]）：先停止接纳新工作，再等已接纳的工作收尾

use std::sync::Arc;
use tokio::sync::{Mutex, watch};

pub struct Exit {
    /// 谁让它退的。请求侧每个请求都来读一次。
    trigger: watch::Sender<Trigger>,
    /// 「可以退了」这一路，只发给 `main`，置位后不再撤销。
    exit_ready: watch::Sender<bool>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Trigger {
    /// 还没决定退出
    Running,
    /// 发现二进制被替换（自我更新）
    Replaced,
    /// 收到停止信号
    Stopping,
}

impl Default for Exit {
    fn default() -> Self {
        // 两个通道都只留发送端：要等的一方自己 `subscribe()`，发送端可以后配接收者。
        let (trigger, _) = watch::channel(Trigger::Running);
        let (exit_ready, _) = watch::channel(false);
        Self {
            trigger,
            exit_ready,
        }
    }
}

impl Exit {
    /// 拒绝新工作的理由，自我更新那一种。写在一处：同一种状态经 D-Bus 或经
    /// `anyhow` 上报，文案一致。
    pub const REASON: &'static str = "The service is restarting to pick up an update";

    /// 拒绝新工作的理由，收到停止信号那一种。客户端看它跟自我更新的区别只在
    /// 措辞：服务要关了，不至于让人以为马上就要回来。
    pub const STOPPING_REASON: &'static str = "The service is shutting down";

    /// 是否已决定退出。请求侧在**取到活动锁之后**问这个。
    pub fn pending(&self) -> bool {
        !matches!(*self.trigger.borrow(), Trigger::Running)
    }

    /// 拒绝新工作时给客户端的理由。
    pub fn reason(&self) -> &'static str {
        match *self.trigger.borrow() {
            // 两种触发都发生过时用这一句，与先后无关：它比「服务要关了」多说明
            // 一件事，而「两种都发生过」由 `mark_replaced` 顶掉 `Stopping`
            // 记下来。
            Trigger::Replaced => Self::REASON,
            // `Running` 到不了：调用方都是先问过 [`Exit::pending`] 的。
            Trigger::Stopping | Trigger::Running => Self::STOPPING_REASON,
        }
    }

    /// 记下「二进制已被替换」
    pub fn mark_replaced(&self) {
        self.trigger.send_if_modified(|trigger| {
            let changed = *trigger != Trigger::Replaced;
            *trigger = Trigger::Replaced;
            changed
        });
    }

    /// 记下「收到停止信号」
    pub fn mark_stopping(&self) {
        self.trigger.send_if_modified(|trigger| {
            if *trigger == Trigger::Running {
                *trigger = Trigger::Stopping;
                true
            } else {
                false
            }
        });
    }

    /// 等到「可以退了」。`main` 用它作为可以退出的条件
    pub async fn wait(&self) {
        let mut ready = self.exit_ready.subscribe();
        // 订阅之后先拿当前值判断一次，所以早到的通知不会丢。发送端就在 `self`
        // 里、不会关闭，因此这里的错误到不了。
        let _ = ready.wait_for(|ready| *ready).await;
    }

    /// 通知 amo 可以退出。调用方必须持活动锁，见 [`decide_exit`]
    fn notify_exit(&self) {
        self.exit_ready.send_replace(true);
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
        // 「可以退了」是**状态**（`watch` 里的最新值），不是一次性的信号：
        // `wait` 订阅之后先看当前值，所以决定早于 main 开始等也不会丢。
        let run_lock = Arc::new(Mutex::new(()));
        let refresh_lock = Arc::new(Mutex::new(()));
        let exit = Exit::default();

        assert!(decide_exit(&run_lock, &refresh_lock, &exit));

        tokio::time::timeout(Duration::from_millis(100), exit.wait())
            .await
            .expect("a decision made before the wait must still be seen");
    }

    #[test]
    fn seeing_a_replacement_is_observable_by_requests() {
        let exit = Exit::default();
        assert!(!exit.pending());

        exit.mark_replaced();
        assert!(exit.pending());
    }

    #[test]
    fn the_self_update_reason_wins_whichever_order_the_triggers_arrive() {
        // 客户端看到的文案不同：自我更新是「马上回来」，停止信号是「关了」。
        // 两种都发生过就得说前一句，而且与谁先谁后无关。
        let replaced_first = Exit::default();
        replaced_first.mark_replaced();
        replaced_first.mark_stopping();
        assert_eq!(replaced_first.reason(), Exit::REASON);

        let stopping_first = Exit::default();
        stopping_first.mark_stopping();
        stopping_first.mark_replaced();
        assert_eq!(stopping_first.reason(), Exit::REASON);
    }
}
