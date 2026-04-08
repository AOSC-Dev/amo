use std::{
    fmt::Display,
    thread::{self, JoinHandle},
};

use zbus::{Connection, fdo, interface};
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

pub struct Amo {
    work: Option<Work>,
    work_inner: Option<JoinHandle<Result<(), zbus::fdo::Error>>>,
}

impl Amo {
    pub fn new() -> Self {
        Self {
            work: None,
            work_inner: None,
        }
    }
}

#[interface(name = "io.aosc.Amo1")]
impl Amo {
    fn refresh(&mut self) -> zbus::fdo::Result<()> {
        self.check_work()?;

        let handle = thread::spawn(move || -> Result<(), zbus::fdo::Error> {
            let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
            rt.block_on(refresh_impl())
        });

        self.work_inner = Some(handle);
        self.work = Some(Work::Refresh);

        Ok(())
    }

    fn work(&self) -> String {
        self.work
            .map_or_else(|| "none".to_string(), |w| w.to_string())
    }

    fn get_result(&mut self) -> fdo::Result<String> {
        if let Some(handle) = self.work_inner.take() {
            if handle.is_finished() {
                let handle = handle.join();
                match handle {
                    Ok(Ok(())) => return Ok("ok".to_string()),
                    Ok(Err(e)) => return Err(fdo::Error::Failed(e.to_string())),
                    Err(e) => return Err(fdo::Error::Failed(format!("thread panicked: {e:?}"))),
                }
            }

            self.work_inner = Some(handle);
            self.work = None;
        }

        Ok("none".to_string())
    }
}

impl Amo {
    fn check_work(&self) -> fdo::Result<()> {
        if let Some(work) = self.work {
            return Err(fdo::Error::Failed(format!(
                "work {} is still running",
                work.to_string()
            )));
        }

        Ok(())
    }
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
