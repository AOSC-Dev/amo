//! 事务系统的单元测试（按实现模块拆分，`#[cfg(test)]` 下编译）。
//!
//! 测试通过 `crate::transaction::tests` 子模块访问实现细节：Rust 的
//! 可见性规则允许子模块访问祖先模块的私有项，因此调度器测试可直接
//! 构造 `Transaction`、调用 `failure_result_json` 等私有函数；跨模块
//! 的项（live/object/limits 的私有项）已提升为 `pub(crate)`。

mod limits;
mod live;
mod object;
mod scheduler;
