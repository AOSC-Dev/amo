//! amo 事务客户端共享封装（PackageKit 风格对象路径）。
//!
//! 每个事务是独立的 D-Bus 对象（`/io/aosc/Amo/Transaction/<id>`）：
//! - `CreateTransaction()` 返回路径，此时事务是"休眠"的，不做任何工作
//! - 客户端先订阅该路径上的 Status / ResultReport / TransactionState 信号
//! - 再调用操作方法（Refresh / ApplyChanges / Simulate / UpdatesList）开工
//!
//! 信号按对象路径隔离，客户端只会收到自己这个事务的信号——不需要按
//! transaction_id 过滤，也没有"先订阅再调用"的竞态（工作永远在订阅
//! 之后才开始）。

use anyhow::bail;
use futures::{FutureExt, StreamExt};
use serde::{Deserialize, Serialize};
use zbus::{Connection, proxy, zvariant::OwnedObjectPath};

/// 事务最终状态。
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub enum TaskStatus {
    Success,
    Failed(String),
}

/// ResultReport 信号载荷。
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

/// TransactionState 信号载荷。
#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub struct TransactionStateEvent {
    pub transaction_id: u64,
    pub role: String,
    pub state: TxState,
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
    fn status(&self, status: String) -> zbus::Result<()>;
    #[zbus(signal)]
    fn result_report(&self, report: String) -> zbus::Result<()>;
    #[zbus(signal)]
    fn transaction_state(&self, state: String) -> zbus::Result<()>;
}

/// 一个已创建、已订阅信号的事务句柄。必须先 `create()` 再调用操作
/// 方法，信号才不丢（D-Bus 信号不重放）。
pub struct Tx {
    pub proxy: AmoTransactionProxy<'static>,
    status: StatusStream,
    result: ResultReportStream,
    state: TransactionStateStream,
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
        let status = proxy.receive_status().await?;
        let result = proxy.receive_result_report().await?;
        let state = proxy.receive_transaction_state().await?;
        Ok(Tx {
            proxy,
            status,
            result,
            state,
        })
    }
}

/// 合并后的订阅事件，只属于本事务（路径隔离）。
#[allow(dead_code)]
pub enum TxEvent {
    /// 一条进度（Status 信号原始载荷）。
    Status(serde_json::Value),
    /// 事务结束报告（ResultReport）。
    Result(TransactionResult),
    /// 事务状态变更（TransactionState 信号）。
    State(TxState),
}

impl Tx {
    /// 下一条事件（进度、结果或状态）；流关闭（连接断开）时返回 `None`。
    ///
    /// 三个信号流是独立的，`tokio::select!` 在多个流都有缓冲消息时是
    /// 无偏的：可能先选到 ResultReport，而更早发出的 Status 还留在
    /// 缓冲里。apply 类示例收到 Result 就立即返回，最终进度会因此
    /// 丢失。这里在每次选择前先排空 status 流里已缓冲的消息——保持
    /// 跨流顺序（先 Status 后 Result）。
    ///
    /// 取消语义：排队中被取消的事务只发 `TransactionState::Cancelled`，
    /// 没有 ResultReport；订阅 state 流保证取消后这里能返回
    /// `TxEvent::State(TxState::Cancelled)` 而不是永远等待。
    pub async fn next_event(&mut self) -> anyhow::Result<Option<TxEvent>> {
        loop {
            // Result 就绪时也先返回已缓冲的 Status：流独立缓冲，
            // 无偏 select 可能先取到 ResultReport，丢掉更早的进度。
            if let Some(signal) = self.status.next().now_or_never() {
                if let Some(signal) = signal {
                    let value: serde_json::Value =
                        serde_json::from_str(&signal.args()?.status)?;
                    return Ok(Some(TxEvent::Status(value)));
                }
                // status 流已关闭：交给下面的 select 处理其余流。
            }
            tokio::select! {
                Some(signal) = self.status.next() => {
                    let value: serde_json::Value =
                        serde_json::from_str(&signal.args()?.status)?;
                    return Ok(Some(TxEvent::Status(value)));
                }
                Some(signal) = self.result.next() => {
                    let report: TransactionResult =
                        serde_json::from_str(&signal.args()?.report)?;
                    return Ok(Some(TxEvent::Result(report)));
                }
                Some(signal) = self.state.next() => {
                    let event: TransactionStateEvent =
                        serde_json::from_str(&signal.args()?.state)?;
                    return Ok(Some(TxEvent::State(event.state)));
                }
                else => return Ok(None),
            }
        }
    }

    /// 等到事务最终结果（跳过进度事件）。
    #[allow(dead_code)]
    pub async fn wait_result(&mut self) -> anyhow::Result<TransactionResult> {
        loop {
            match self.next_event().await? {
                Some(TxEvent::Result(report)) => return Ok(report),
                Some(TxEvent::Status(_)) => continue,
                // 排队中被取消：服务端只发 Cancelled，不会再有
                // ResultReport，继续等会挂死。
                Some(TxEvent::State(TxState::Cancelled)) => bail!("transaction cancelled"),
                // Finished 总是跟在 ResultReport 之后；若先被 select
                // 取到，Result 仍在路上，继续等。
                Some(TxEvent::State(_)) => continue,
                None => bail!("result stream closed"),
            }
        }
    }
}
