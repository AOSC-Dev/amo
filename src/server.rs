use serde::{Deserialize, Serialize};
use std::fmt::Display;
use zbus::{Connection, fdo, interface, object_server::SignalEmitter, zvariant::Type};
use zbus_polkit::policykit1::{AuthorityProxy, CheckAuthorizationFlags, Subject};
use crate::refresh::refresh_impl;

#[derive(Debug, Clone, Copy)]
pub enum Work {
    Refresh,
}

impl Display for Work {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Work::Refresh => write!(f, "refresh"),
        }
    }
}

#[derive(Serialize, Deserialize, Type, Clone, Debug)]
pub enum RefreshState {
    Started,
    Progress,
    Finished,
    Failed,
}

/// 2. 组合成统一的 D-Bus 信号负载
#[derive(Serialize, Deserialize, Type, Clone, Debug)]
pub struct RefreshStatus {
    pub state: RefreshState,
    pub message: String,
}

pub struct Amo;

impl Amo {
    pub fn new() -> Self {
        Self
    }
}

#[interface(name = "io.aosc.Amo1")]
impl Amo {
    async fn refresh(
        &self,
        #[zbus(signal_context)] ctxt: SignalEmitter<'_>,
    ) -> zbus::fdo::Result<()> {
        auth().await?;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<RefreshStatus>();

        let ctxt_owned = ctxt.to_owned();

        tokio::spawn(async move {
            let _ = ctxt_owned
                .refresh_status(RefreshStatus {
                    state: RefreshState::Started,
                    message: "".into(),
                })
                .await;

            while let Some(status) = rx.recv().await {
                if matches!(status.state, RefreshState::Failed) {
                    let _ = ctxt_owned.refresh_status(status).await;
                    return;
                }
                let _ = ctxt_owned.refresh_status(status).await;
            }

            let _ = ctxt_owned.refresh_status(RefreshStatus {
                state: RefreshState::Finished,
                message: "".into(),
            });
        });

        tokio::task::spawn_blocking(move || {
            if let Err(e) = refresh_impl(tx.clone()) {
                let _ = tx.send(RefreshStatus {
                    state: RefreshState::Failed,
                    message: e,
                });
            }
        });

        Ok(())
    }

    #[zbus(signal)]
    async fn refresh_status(ctxt: &SignalEmitter<'_>, status: RefreshStatus) -> zbus::Result<()>;
}

pub async fn auth() -> Result<(), fdo::Error> {
    let connection = Connection::system().await?;
    let proxy = AuthorityProxy::new(&connection).await?;
    let subject = Subject::new_for_owner(std::process::id(), None, None)
        .map_err(|e| fdo::Error::AccessDenied(e.to_string()))?;

    let result = proxy
        .check_authorization(
            &subject,
            "io.aosc.Amo.apply.run",
            &std::collections::HashMap::new(),
            CheckAuthorizationFlags::AllowUserInteraction.into(),
            "",
        )
        .await?;

    if !result.is_authorized {
        return Err(fdo::Error::AccessDenied("not authorized".to_string()));
    }

    Ok(())
}
