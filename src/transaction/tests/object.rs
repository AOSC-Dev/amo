//! 事务对象（`TransactionObject`）的单元测试。

use crate::transaction::object::TransactionObject;
use crate::transaction::types::TransactionEvent;
use std::time::Duration;

/// 授权等待的超时语义：挂起的授权 future 超过时限被放弃（TimedOut，
/// drop 掉对 PolicyKit 的等待）；失败与成功的 future 原样透传。
#[tokio::test]
async fn auth_timeout_aborts_pending_auth() {
    // 永不 resolve 的授权 future → 超时返回 TimedOut。
    let err = TransactionObject::await_auth(1, Duration::from_millis(50), std::future::pending())
        .await
        .expect_err("pending auth must time out");
    assert!(
        matches!(err, zbus::fdo::Error::TimedOut(_)),
        "expected TimedOut, got {err:?}"
    );

    // 快速失败的授权 → 原样返回错误。
    let err = TransactionObject::await_auth(1, Duration::from_secs(5), async {
        Err(zbus::fdo::Error::AccessDenied("no".into()))
    })
    .await
    .expect_err("denied auth must return its error");
    assert!(
        matches!(err, zbus::fdo::Error::AccessDenied(_)),
        "expected AccessDenied, got {err:?}"
    );

    // 快速成功的授权 → Ok。
    TransactionObject::await_auth(1, Duration::from_secs(5), async { Ok(()) })
        .await
        .expect("approved auth must succeed");
}

/// Progress 事件必须能承载标量载荷（oma_refresh::db::Event 的单元变体
/// 如 Done/ScanningTopic 序列化为 `"Done"` 这类 JSON 标量；内部标签的
/// newtype 无法承载标量，payload 字段则任意值都行）。同时验证 map
/// 载荷与客户端可反序列化。
#[test]
fn progress_event_carries_scalar_and_map_payloads() {
    // 标量：oma 事件单元变体（Done）。
    let event = TransactionEvent::Progress {
        payload: serde_json::json!("Done"),
    };
    let json = serde_json::to_string(&event).expect("scalar progress must serialize");
    let v: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(v["type"], "progress");
    assert_eq!(v["payload"], "Done");
    // 反序列化回枚举（客户端同构结构）。
    assert!(matches!(
        serde_json::from_str::<TransactionEvent>(&json).unwrap(),
        TransactionEvent::Progress { payload } if payload == "Done"
    ));

    // map 载荷：oma 事件 struct 变体（如 DownloadEvent）。
    let event = TransactionEvent::Progress {
        payload: serde_json::json!({"DownloadEvent": {"AllDone": {}}}),
    };
    let json = serde_json::to_string(&event).expect("map progress must serialize");
    let v: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(v["type"], "progress");
    assert_eq!(
        v["payload"]["DownloadEvent"]["AllDone"],
        serde_json::json!({})
    );
}
