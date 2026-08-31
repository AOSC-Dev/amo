//! 事务参数大小校验的单元测试。

use crate::transaction::limits::{
    MAX_TRANSACTION_ARG_BYTES, MAX_TRANSACTION_ARG_ITEMS, check_arg_size,
};

/// 事务参数校验：字节上限防大字符串；元素上限防空串/极短串的海量
/// 条目（每个 String 都占 24 字节头，空串绕过字节上限）。正常大小
/// 通过；单个超字节、合计超字节、超元素数都被拒绝。
#[test]
fn oversized_transaction_arguments_rejected() {
    // 正常大小通过。
    assert!(check_arg_size(&["fish".into()], &["vim".into()]).is_ok());
    // install 单个超字节拒绝。
    let big = "x".repeat(MAX_TRANSACTION_ARG_BYTES + 1);
    assert!(matches!(
        check_arg_size(&[big], &[]),
        Err(zbus::fdo::Error::LimitsExceeded(_))
    ));
    // install + remove 合计超字节也拒绝。
    let half = MAX_TRANSACTION_ARG_BYTES / 2 + 1;
    assert!(check_arg_size(&["a".repeat(half)], &["b".repeat(half)]).is_err());
    // 恰好等于字节上限允许。
    assert!(check_arg_size(&["y".repeat(MAX_TRANSACTION_ARG_BYTES)], &[]).is_ok());
    // 超元素数（全部空串，字节数=0 但内存可观）拒绝。
    let many = vec![String::new(); MAX_TRANSACTION_ARG_ITEMS + 1];
    assert!(matches!(
        check_arg_size(&many, &[]),
        Err(zbus::fdo::Error::LimitsExceeded(_))
    ));
    // 恰好等于元素上限允许（空串）。
    let exactly = vec![String::new(); MAX_TRANSACTION_ARG_ITEMS];
    assert!(check_arg_size(&exactly, &[]).is_ok());
    // install + remove 合计超元素数也拒绝。
    let half_items = MAX_TRANSACTION_ARG_ITEMS / 2 + 1;
    assert!(
        check_arg_size(&vec![String::new(); half_items], &vec![String::new(); half_items])
            .is_err()
    );
}