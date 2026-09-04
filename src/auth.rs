//! D-Bus 调用方身份解析与 polkit 授权。

use tracing::error;
use zbus::{Connection, fdo, names::BusName};
use zbus_polkit::policykit1::{AuthorityProxy, CheckAuthorizationFlags, Subject};

/// 取调用方的 D-Bus 唯一名与 Unix uid
pub(crate) async fn peer_identity(
    header: &zbus::message::Header<'_>,
    conn: &Connection,
) -> Result<(String, u32), fdo::Error> {
    let sender = header
        .sender()
        .ok_or_else(|| fdo::Error::AccessDenied("Unknown sender!".to_string()))?
        .to_owned();

    let dbus_proxy = zbus::fdo::DBusProxy::new(conn).await?;
    let uid = dbus_proxy
        .get_connection_unix_user(BusName::from(sender.clone()))
        .await?;

    Ok((sender.to_string(), uid))
}

/// 请求 polkit 授权
pub async fn auth(
    header: &zbus::message::Header<'_>,
    conn: &Connection,
    action: &str,
    cancellation_id: &str,
) -> Result<(), fdo::Error> {
    let sender = header
        .sender()
        .ok_or_else(|| fdo::Error::AccessDenied("Unknown sender!".to_string()))?
        .to_owned();

    let dbus_proxy = zbus::fdo::DBusProxy::new(conn).await?;

    let bus_name = BusName::from(sender);
    let real_pid = dbus_proxy
        .get_connection_unix_process_id(bus_name.clone())
        .await?;
    let real_uid = dbus_proxy.get_connection_unix_user(bus_name).await?;

    let proxy = AuthorityProxy::new(conn).await?;
    let subject = Subject::new_for_owner(real_pid, None, Some(real_uid))
        .map_err(|e| fdo::Error::AccessDenied(e.to_string()))?;

    let result = proxy
        .check_authorization(
            &subject,
            action,
            &std::collections::HashMap::new(),
            CheckAuthorizationFlags::AllowUserInteraction.into(),
            cancellation_id,
        )
        .await?;

    if !result.is_authorized {
        return Err(fdo::Error::AccessDenied("Authorized failed!".to_string()));
    }

    Ok(())
}

/// 取消一次尚未完成的远程 PolicyKit 授权检查：仅 drop 本地 zbus future
/// 只会放弃回复，不会向 polkit 发送取消——远程检查与认证弹窗会在 amo
/// 释放槽位后继续累积。超时/中止后对对应 `cancellation_id` 调用；对已
/// 完成或不存在的检查是安全的无操作。
pub(crate) async fn cancel_authorization(conn: &Connection, cancellation_id: &str) {
    if let Ok(proxy) = AuthorityProxy::new(conn).await
        && let Err(e) = proxy.cancel_check_authorization(cancellation_id).await
    {
        error!("Failed to cancel PolicyKit check {cancellation_id}: {e}");
    }
}
