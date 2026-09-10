use std::time::{Duration, Instant};
use tokio::signal::unix::{SignalKind, signal};
use tracing::{info, warn};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};
use tracing_tree::{HierarchicalLayer, time::LocalDateTime};

use crate::server::{Amo, announce_restart};

mod oma;
mod self_update;
mod server;
mod tum;

/// 退出时愿意等的总时长。
///
/// 退出要等两件事，它们各自都会无上限地拖住：
///
/// 1. **在途 D-Bus 方法调用**——`graceful_shutdown` 等它们返回。平时这是好事，
///    正在跑的调用能把结果报给客户端再走；但**授权弹窗**会让它变味：`Refresh`
///    / `ApplyChanges` 取到活动锁之后才去 `auth`（见 `Amo::begin_activity` 的
///    说明），没人回答 polkit 弹窗就什么都不会发生。实测：有这样一个方法时，
///    `graceful_shutdown` 三秒仍未返回。
///
/// 2. **包操作留在 detached 任务里的 `spawn_blocking`**——`Refresh` /
///    `ApplyChanges` 发出 request id 就返回了，真正干活的 `spawn_blocking`
///    没人 await（也取消不了）。`graceful_shutdown` 看不到它，但 runtime 析构
///    会等它，而那是没有上限的。实测：卡住的 `spawn_blocking` 会让
///    `drop(runtime)` 一直不返回。
///
/// 两边都要有上限，进程才能真的退出。总预算给 30 秒，选在 systemd 的
/// `TimeoutStopSec`（默认 90 秒，本 unit 未覆盖）之下，好让我们自己把日志写完
/// 再正常退出，而不是被 SIGKILL 掉、什么记录都不留。
///
/// 超时不等于取消工作——包操作仍在进行，只是不再由我们等。
///
/// 期限必须在**收到退出触发的那一刻**创建（见 `serve`）。锚在启动时刻的话，
/// 守护进程只要活过这么久，期限就已经过期，上面两处等待都拿不到宽限。
const SHUTDOWN_GRACE: Duration = Duration::from_secs(30);

/// 运行时和收尾都由 [`main`] 手工管：`#[tokio::main]` 把 runtime 藏在宏里，
/// 没地方给它设收尾上限（见 `SHUTDOWN_GRACE` 第 2 条）。
fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"))
        .add_directive("zbus=error".parse()?)
        .add_directive("zbus_fdo=error".parse()?)
        .add_directive("tokio=warn".parse()?);

    tracing_subscriber::registry()
        .with(filter)
        .with(
            HierarchicalLayer::new(2)
                .with_targets(true)
                .with_bracketed_fields(true)
                .with_span_modes(true)
                .with_timer(LocalDateTime {
                    higher_precision: true,
                }),
        )
        .init();

    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider!");

    let runtime = tokio::runtime::Runtime::new()?;

    // `serve` 在收到退出触发时才立期限，并把它交回来。
    let result = runtime.block_on(serve());

    // 只再等包操作留下的 spawn_blocking 到期限为止；超时就把它抛下，`main`
    // 返回、进程退出（线程随进程一起消失）。
    //
    // `serve` 若没走到收尾就失败（比如启动阶段出错），也就没有期限可言，
    // 此时不必给包操作留宽限。
    let grace = match result.as_ref() {
        Ok(deadline) => deadline.saturating_duration_since(Instant::now()),
        Err(_) => Duration::ZERO,
    };
    runtime.shutdown_timeout(grace);

    result.map(|_| ())
}

/// 服务本体。返回**收尾期限**：`main` 用它给 runtime 析构定上限。
async fn serve() -> anyhow::Result<Instant> {
    info!("amo is running");

    let amo = Amo::new()?;
    // 挂监视算启动的一部分，失败即退出：理由见 `Amo::watch_for_self_update`。
    let exit = amo.exit_handle();
    amo.watch_for_self_update()?;
    let conn = zbus::connection::Builder::system()?
        .name("io.aosc.Amo")?
        .allow_name_replacements(false)
        .serve_at("/io/aosc/Amo", amo)?
        .build()
        .await?;

    let mut sigterm = signal(SignalKind::terminate())?;

    tokio::select! {
        _ = tokio::signal::ctrl_c() => info!("Received SIGINT, shutting down"),
        _ = sigterm.recv() => info!("Received SIGTERM, shutting down"),
        // 二进制被替换且服务已空闲：通知客户端重连后退出，让 systemd 在
        // 下次 D-Bus 调用时拉起新版本。
        _ = exit.wait() => announce_restart(&conn).await,
    }

    // 期限从**收到退出触发**这一刻算起，两段等待共用：`graceful_shutdown`
    // 用它做上限，`main` 拿剩下的给 runtime 析构。锚在启动时刻的话，守护进程
    // 活过 SHUTDOWN_GRACE 后期限已过期，两处宽限都变成 0。
    let deadline = Instant::now() + SHUTDOWN_GRACE;

    // 等在途方法调用收尾，但不超过期限：理由见 `SHUTDOWN_GRACE`。
    if tokio::time::timeout_at(deadline.into(), conn.graceful_shutdown())
        .await
        .is_err()
    {
        warn!("Gave up waiting for in-flight calls after {SHUTDOWN_GRACE:?}");
    }

    info!("amo stopped");

    Ok(deadline)
}
