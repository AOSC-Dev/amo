use std::{
    fmt::Display,
    path::PathBuf,
    thread::{self, JoinHandle},
};

use apt_auth_config::AuthConfig;
use oma_pm::apt::AptConfig;
use oma_refresh::db::OmaRefresh;
use oma_utils::dpkg::dpkg_arch;
use reqwest::ClientBuilder;
use zbus::{Connection, fdo, interface};
use zbus_polkit::policykit1::{AuthorityProxy, CheckAuthorizationFlags, Subject};

const USER_AGENT: &str = concat!("amo/", env!("CARGO_PKG_VERSION"));

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
    pub fn refresh(&mut self) -> zbus::fdo::Result<()> {
        if let Some(work) = self.work {
            return Err(zbus::fdo::Error::Failed(format!(
                "work {} is still running",
                work.to_string()
            )));
        }

        let handle = thread::spawn(move || -> Result<(), zbus::fdo::Error> {
            let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
            rt.block_on(refresh_impl())
        });

        self.work_inner = Some(handle);
        self.work = Some(Work::Refresh);

        Ok(())
    }

    pub fn work(&self) -> String {
        self.work
            .map_or_else(|| "none".to_string(), |w| w.to_string())
    }

    pub fn get_result(&mut self) -> fdo::Result<String> {
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

async fn refresh_impl() -> zbus::fdo::Result<()> {
    auth().await?;

    let client = ClientBuilder::new()
        .user_agent(USER_AGENT)
        .build()
        .map_err(|e| zbus::fdo::Error::Failed(e.to_string()))?;

    let auth_config = AuthConfig::system("/").ok();
    let config = AptConfig::new();

    let r = OmaRefresh::builder()
        .download_dir(PathBuf::from(
            AptConfig::new().dir("Dir::State::lists", "lists/"),
        ))
        .source(PathBuf::from("/"))
        .threads(4)
        .arch(dpkg_arch("/").map_err(|e| zbus::fdo::Error::Failed(e.to_string()))?)
        .client(&client)
        .maybe_auth_config(auth_config.as_ref())
        .refresh_topics(true)
        .apt_config(&config)
        .topic_msg("")
        .build();

    r.start(|ev| async {
        match ev {
            oma_refresh::db::Event::DownloadEvent(event) => {
                println!("Download event: {event:#?}");
            }
            oma_refresh::db::Event::ScanningTopic => {
                println!("Scanning topic...");
            }
            oma_refresh::db::Event::ClosingTopic(name) => {
                println!("Closing topic: {name}");
            }
            oma_refresh::db::Event::TopicNotInMirror { topic, mirror } => {
                println!("Topic {topic} not in mirror {mirror}");
            }
            oma_refresh::db::Event::RunInvokeScript => {
                println!("Running invoke script...");
            }
            oma_refresh::db::Event::SourceListFileNotSupport { path } => {
                println!("Source list file not support: {}", path.display());
            }
            oma_refresh::db::Event::Done => {
                println!("Done!");
            }
        }
    })
    .await
    .map_err(|e| zbus::fdo::Error::Failed(e.to_string()))?;

    Ok(())
}

async fn auth() -> Result<(), fdo::Error> {
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
