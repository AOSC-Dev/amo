//! 事务参数（install + remove）的大小校验：请求太大就直接拒绝，
//! 防止恶意请求者用超大字符串把守护进程的内存耗光。

/// install + remove 里所有字符串加起来，最多允许的总字节数。
/// 为什么要限：D-Bus 单条消息最多约 128MB，任何人都能提交这么大的一串
/// 参数；而队列上限只数"有几条事务"，不数"参数占多少内存"，排队的请求
/// 会一直留在守护进程里。不校验的话，十几个超大请求就能让它保留近 1GB
/// 内存。
pub(crate) const MAX_TRANSACTION_ARG_BYTES: usize = 16 * 1024 * 1024; // 16 MiB

/// install + remove 里最多允许多少条参数。
/// 为什么要限：字节上限只管字符串内容，管不住"数量"。包含空/短字符串的请求条目几乎
/// 不占字节，但每条字符串在内存里还有 24 字节头部 + 容量，几百万个空字符串
/// 照样能占掉几百 MB——所以条数也得单独限制。
pub(crate) const MAX_TRANSACTION_ARG_ITEMS: usize = 65_536;

/// 入队前检查事务参数：字节数或条数任一超出限制就返回 LimitsExceeded。
/// 字符串大小上限防"大字符串"，请求数目上限制"海量空字符串/短字符串"。
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
