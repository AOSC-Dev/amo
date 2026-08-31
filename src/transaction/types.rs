//! 事务系统的共享类型：角色、状态、事件载荷与错误。
//!
//! 这些类型被调度器（`mod.rs`）、事务对象（`object.rs`）与注册表
//! （`live.rs`）共同使用，独立成模块避免循环依赖与重复定义。

use serde::{Deserialize, Serialize};
use std::{future::Future, pin::Pin};

/// 事务角色：对应 amo 的各个操作入口。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionRole {
    /// 刷新软件源索引
    Refresh,
    /// 安装 / 移除 / 升级
    ApplyChanges,
    /// 模拟事务（预览将发生的变更）
    Simulate,
    /// 获取可更新列表
    UpdatesList,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionState {
    /// 已入队，等待执行
    Queued,
    /// 正在执行
    Running,
    /// 已完成
    Finished,
    /// 排队期间被取消，未执行
    Cancelled,
}

/// 事务任务：boxed future，由 runner 串行执行。
pub type Task = Pin<Box<dyn Future<Output = ()> + Send>>;

/// `TransactionState` 信号的 JSON 载荷。
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct TransactionStateEvent {
    pub transaction_id: u64,
    pub role: TransactionRole,
    pub state: TransactionState,
}

/// 取消事务失败的原因。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CancelError {
    /// 事务不存在（或已结束，不再保留在列表里）。
    NotFound,
    /// 调用者不是事务所有者，且不是 root。
    NotOwner,
    /// 事务已开始运行，不能取消。
    Running,
    /// 事务已处于取消状态。
    AlreadyCancelled,
}

/// 入队被拒绝的原因。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnqueueError {
    /// 队列已满（全局上限）。
    QueueFull,
    /// 该调用者（uid）已占用过多排队中的事务（每用户配额）。
    QuotaExceeded,
}

/// 事务结束报告（单流 `TransactionEvent` 的 Result 变体载荷）。
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ResultReport {
    pub transaction_id: u64,
    pub role: TransactionRole,
    pub status: TaskStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub enum TaskStatus {
    Success,
    Failed(String),
}

/// 单流 `TransactionEvent` 信号的载荷：一个事务的全部事件（进度、状态、
/// 结果）都走这一个信号，服务端保证发射顺序（进度 → 结果 → 终态）。
/// 客户端只需订阅一个流，无需自行合并/排序。
#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TransactionEvent {
    /// 一条进度（原 Status 信号载荷）。必须是带 `payload` 字段的 struct
    /// 变体：`oma_refresh::db::Event` 的单元变体（ScanningTopic /
    /// RunInvokeScript / Done）序列化为 JSON 标量（如 `"Done"`），内部
    /// 标签（tag="type"）的 newtype 变体要求载荷是 map 才能注入 tag，
    /// 标量会序列化失败、事件被转发器丢弃。struct 变体把载荷放进
    /// 相邻的 `payload` 字段，任意 `serde_json::Value` 都能承载。
    Progress {
        /// 进度载荷（任意 JSON：oma 事件、下载进度、dpkg 进度等）。
        payload: serde_json::Value,
    },
    /// 事务状态变更（原 TransactionState 信号载荷）。
    State(TransactionStateEvent),
    /// 事务结束报告（原 ResultReport 信号载荷）。
    Result(ResultReport),
}