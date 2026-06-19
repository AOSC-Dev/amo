use crate::refresh::refresh_impl;
use std::fmt::Display;
use zbus::{Connection, fdo, interface, object_server::SignalEmitter};
use zbus_polkit::policykit1::{AuthorityProxy, CheckAuthorizationFlags, Subject};

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

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();

        let ctxt_owned = ctxt.to_owned();

        tokio::spawn(async move {
            while let Some(status) = rx.recv().await {
                let _ = ctxt_owned.refresh_status(status).await;
            }
        });

        tokio::task::spawn_blocking(move || {
            if let Err(e) = refresh_impl(tx.clone()) {
                eprintln!("Failed to refresh package metadata: {e}");
            }
        });

        Ok(())
    }

    #[zbus(signal)]
    async fn refresh_status(ctxt: &SignalEmitter<'_>, status: String) -> zbus::Result<()>;
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
