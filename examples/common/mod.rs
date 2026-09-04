//! amo 事务客户端共享封装（PackageKit 风格对象路径，单流事件协议）。
//!
//! 每个事务是独立的 D-Bus 对象（`/io/aosc/Amo/Transaction/<id>`）：
//! - `CreateTransaction()` 返回路径，此时事务是"休眠"的，不做任何工作
//! - 客户端先订阅该路径上的 `TransactionEvent` 信号
//! - 再调用操作方法（Refresh / ApplyChanges / Simulate / UpdatesList）开工
//!
//! 信号按对象路径隔离，客户端只会收到自己这个事务的信号——不需要按
//! transaction_id 过滤，也没有"先订阅再调用"的竞态（工作永远在订阅
//! 之后才开始）。
//!
//! 单流协议：进度、状态、结果都走同一个 `TransactionEvent` 信号，服务端
//! 保证发射顺序（进度 → 结果 → 终态）。客户端只需消费一个有序流，无需
//! 自行合并/排序/处理取消边界。

use anyhow::bail;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use zbus::{Connection, proxy, zvariant::OwnedObjectPath};

/// 事务最终状态。
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub enum TaskStatus {
    Success,
    Failed(String),
}

/// ResultReport 载荷（TransactionEvent 的 Result 变体）。
#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub struct TransactionResult {
    pub transaction_id: u64,
    pub role: String,
    pub status: TaskStatus,
    pub result: Option<serde_json::Value>,
}

/// TransactionState 信号的 state 字段。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TxState {
    Queued,
    Running,
    Finished,
    Cancelled,
}

/// TransactionState 载荷（TransactionEvent 的 State 变体）。
#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub struct TransactionStateEvent {
    pub transaction_id: u64,
    pub role: String,
    pub state: TxState,
}

/// 单流 `TransactionEvent` 信号载荷：一个事务的全部事件。
///
/// 注意：不能叫 `TransactionEvent`——zbus 的 proxy 宏会为
/// `transaction_event` 信号生成同名类型，会冲突。
#[derive(Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventEnvelope {
    /// 一条进度（原 Status 载荷）。服务端用带 `payload` 字段的 struct
    /// 变体承载任意 JSON（含标量，如 oma 事件的 `"Done"`）。
    Progress {
        /// 进度载荷。
        payload: serde_json::Value,
    },
    /// 事务状态变更。
    State(TransactionStateEvent),
    /// 事务结束报告。
    Result(TransactionResult),
}

/// 主接口：创建事务对象。
#[proxy(
    interface = "io.aosc.Amo1",
    default_service = "io.aosc.Amo",
    default_path = "/io/aosc/Amo"
)]
pub trait Amo {
    fn create_transaction(&self) -> zbus::Result<OwnedObjectPath>;
    fn get_transaction_list(&self) -> zbus::Result<String>;
}

/// 事务对象接口：路径在创建时返回，客户端用 builder 指定。
#[proxy(
    interface = "io.aosc.Amo.Transaction",
    default_service = "io.aosc.Amo",
    default_path = "/io/aosc/Amo"
)]
pub trait AmoTransaction {
    fn refresh(&self) -> zbus::Result<()>;
    fn apply_changes(
        &self,
        install: Vec<&str>,
        remove: Vec<&str>,
        upgrade: bool,
    ) -> zbus::Result<()>;
    fn simulate(&self, install: Vec<&str>, remove: Vec<&str>, upgrade: bool) -> zbus::Result<()>;
    fn updates_list(&self) -> zbus::Result<()>;
    fn cancel(&self) -> zbus::Result<()>;
    fn destroy(&self) -> zbus::Result<()>;

    #[zbus(signal)]
    fn transaction_event(&self, event: String) -> zbus::Result<()>;
}

/// 一个已创建、已订阅信号的事务句柄。必须先 `create()` 再调用操作
/// 方法，信号才不丢（D-Bus 信号不重放）。
pub struct Tx {
    pub proxy: AmoTransactionProxy<'static>,
    events: TransactionEventStream,
}

/// 事务客户端：负责连接并创建事务对象。
pub struct TransactionClient {
    connection: Connection,
    main: AmoProxy<'static>,
}

impl TransactionClient {
    pub async fn connect() -> zbus::Result<Self> {
        let connection = Connection::system().await?;
        let main = AmoProxy::new(&connection).await?;
        Ok(Self { connection, main })
    }

    /// 创建事务对象并订阅它的信号（在调用操作方法之前）。
    pub async fn create(&self) -> zbus::Result<Tx> {
        let path = self.main.create_transaction().await?;
        let proxy = AmoTransactionProxy::builder(&self.connection)
            .path(path)?
            .build()
            .await?;
        let events = proxy.receive_transaction_event().await?;
        Ok(Tx { proxy, events })
    }
}

/// 合并后的订阅事件，只属于本事务（路径隔离）。
#[allow(dead_code)]
pub enum TxEvent {
    /// 一条进度（TransactionEvent::Progress 载荷）。
    Status(serde_json::Value),
    /// 事务状态变更（TransactionEvent::State 载荷）。
    State(TxState),
    /// 事务结束报告（TransactionEvent::Result 载荷）。
    Result(TransactionResult),
}

/// 处理一条状态事件：Cancelled 与 Finished 都是终态，直接报错。
///
/// 单流协议保证发射顺序（进度 → 结果 → 终态）：Finished 到达意味着
/// Result 已不可能还在路上（同一有序流）——若还没收到 Result，说明
/// 服务端结果发射失败（emit_result 错误只记日志），客户端必须报错
/// 而不是永远等待。
pub fn check_terminal_state(state: TxState) -> anyhow::Result<()> {
    match state {
        TxState::Cancelled => bail!("transaction cancelled"),
        TxState::Finished => bail!("transaction finished without result"),
        TxState::Queued | TxState::Running => Ok(()),
    }
}

impl Tx {
    /// 下一条事件（进度、状态或结果）；流关闭（连接断开）时返回 `None`。
    ///
    /// 单流协议：服务端保证发射顺序（进度 → 结果 → 终态），这里只需
    /// 消费一个有序流，无需跨流合并/排序。
    pub async fn next_event(&mut self) -> anyhow::Result<Option<TxEvent>> {
        let Some(signal) = self.events.next().await else {
            return Ok(None);
        };
        let event: EventEnvelope = serde_json::from_str(&signal.args()?.event)?;
        Ok(Some(match event {
            EventEnvelope::Progress { payload } => TxEvent::Status(payload),
            EventEnvelope::State(state) => TxEvent::State(state.state),
            EventEnvelope::Result(report) => TxEvent::Result(report),
        }))
    }

    /// 等到事务最终结果（跳过进度事件）。
    #[allow(dead_code)]
    pub async fn wait_result(&mut self) -> anyhow::Result<TransactionResult> {
        loop {
            match self.next_event().await? {
                Some(TxEvent::Result(report)) => return Ok(report),
                Some(TxEvent::Status(_)) => continue,
                // 终态（Cancelled/Finished）到达即报错：单流协议保证
                // 顺序（进度 → 结果 → 终态），Finished 意味着 Result
                // 已不可能在路上——服务端 emit_result 失败时只记日志，
                // 继续等会挂死。
                Some(TxEvent::State(state)) => check_terminal_state(state)?,
                None => bail!("result stream closed"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finished_without_result_is_an_error() {
        let err = check_terminal_state(TxState::Finished).unwrap_err();
        assert!(
            err.to_string().contains("finished without result"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn cancelled_is_an_error() {
        let err = check_terminal_state(TxState::Cancelled).unwrap_err();
        assert!(
            err.to_string().contains("cancelled"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn intermediate_states_are_not_terminal() {
        assert!(check_terminal_state(TxState::Queued).is_ok());
        assert!(check_terminal_state(TxState::Running).is_ok());
    }
}
