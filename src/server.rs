use crate::refresh::refresh_impl;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, atomic::AtomicU64};
use tokio::sync::{Mutex, RwLock};
use tracing::error;
use zbus::{Connection, fdo, interface, object_server::SignalEmitter};
use zbus_polkit::policykit1::{AuthorityProxy, CheckAuthorizationFlags, Subject};

pub struct Amo {
    run_lock: Arc<Mutex<()>>,
    current_report: Arc<RwLock<Option<ResultReport>>>,
    current_version: AtomicU64,
}

impl Amo {
    pub fn new() -> Self {
        Self {
            run_lock: Arc::new(Mutex::new(())),
            current_report: Arc::new(RwLock::new(None)),
            current_version: AtomicU64::new(0),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ResultReport {
    pub version: u64,
    pub status: TaskStatus,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub enum TaskStatus {
    Success,
    Failed(String),
}

#[interface(name = "io.aosc.Amo1")]
impl Amo {
    async fn refresh(
        &self,
        #[zbus(signal_context)] ctxt: SignalEmitter<'_>,
    ) -> zbus::fdo::Result<u64> {
        let Ok(_guard) = self.run_lock.try_lock() else {
            return Err(zbus::fdo::Error::Failed(
                "Another task is already running".to_string(),
            ));
        };

        drop(_guard);

        let next_version = self
            .current_version
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;

        auth().await?;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();

        let ctxt_owned = ctxt.to_owned();

        tokio::spawn(async move {
            while let Some(status) = rx.recv().await {
                if let Err(e) = ctxt_owned.refresh_status(status.clone()).await {
                    error!(
                        msg = status,
                        error = e.to_string(),
                        "send refresh_satatus request got error"
                    );
                }
            }
        });

        let run_lock_clone = self.run_lock.clone();
        let report_saver = self.current_report.clone();

        tokio::task::spawn_blocking(move || {
            let Ok(_keep_lock_alive) = run_lock_clone.try_lock() else {
                return;
            };

            let outcome = refresh_impl(tx.clone());

            let status = match outcome {
                Ok(_) => TaskStatus::Success,
                Err(e) => TaskStatus::Failed(e.to_string()),
            };

            tokio::runtime::Handle::current().block_on(async {
                let mut writer = report_saver.write().await;
                *writer = Some(ResultReport {
                    version: next_version,
                    status,
                });
            });
        });

        Ok(next_version)
    }

    async fn get_last_result(&self) -> zbus::fdo::Result<String> {
        let reader = self.current_report.read().await;
        Ok(serde_json::to_string(&*reader).unwrap())
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
