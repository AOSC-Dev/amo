//! 事务参数（install + remove）的大小校验：入队前拒绝超大请求，防止
//! 未授权调用者用接近系统总线消息上限的字符串耗尽守护进程内存。

/// 单个事务参数（install + remove 全部字符串）允许的最大总字节数。
/// 未授权调用者可提交接近系统总线消息上限（~128MB）的字符串，且这些
/// 向量被 boxed future 无限制捕获——队列上限只数条目（每 uid 8 + 运行中
/// 1），不数字节，可让守护进程保留近 1GB。入队前必须校验聚合大小。
pub(crate) const MAX_TRANSACTION_ARG_BYTES: usize = 16 * 1024 * 1024; // 16 MiB
/// 单个事务参数（install + remove）允许的最大元素数。字节上限只统计字符串
/// 内容——空串/极短串贡献 0 字节，但每个反序列化的 `String` 都占 24 字节
/// 头 + Vec 容量，数百万空串可绕过字节上限（系统总线消息 ~128MB 可装
/// 上千万空串，内存数百 MB/请求）。元素数必须单独有界。
pub(crate) const MAX_TRANSACTION_ARG_ITEMS: usize = 65_536;

/// 校验事务参数（install + remove）：聚合字节数与元素数任一超限都拒绝
/// （LimitsExceeded），在构造任务/入队之前调用。字节上限防大字符串，
/// 元素上限防空串/极短串的海量条目。
pub(crate) fn check_arg_size(
    install: &[String],
    remove: &[String],
) -> Result<(), zbus::fdo::Error> {
    let items = install.len().saturating_add(remove.len());
    if items > MAX_TRANSACTION_ARG_ITEMS {
        return Err(zbus::fdo::Error::LimitsExceeded(format!(
            "Too many transaction arguments ({items}, limit {MAX_TRANSACTION_ARG_ITEMS})"
        )));
    }
    let total: usize = install.iter().chain(remove.iter()).map(|s| s.len()).sum();
    if total > MAX_TRANSACTION_ARG_BYTES {
        return Err(zbus::fdo::Error::LimitsExceeded(format!(
            "Transaction arguments too large ({total} bytes, limit {MAX_TRANSACTION_ARG_BYTES})"
        )));
    }
    Ok(())
}