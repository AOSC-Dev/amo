use std::{
    env,
    io::{IsTerminal, stdout},
    os::unix::fs::PermissionsExt,
    process::Command,
};

use anyhow::bail;
use debconf::{Capability, DebconfCommand, DebconfResponse, DescriptionContent, parse_line};

use dialoguer::theme::ColorfulTheme;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use zbus::{Connection, proxy};

#[proxy(
    interface = "io.aosc.Amo1",
    default_service = "io.aosc.Amo",
    default_path = "/io/aosc/Amo"
)]
trait AmoContract {
    async fn apply_changes(
        &self,
        install: Vec<&str>,
        remove: Vec<&str>,
        upgrade: bool,
    ) -> zbus::Result<String>;
    async fn get_last_result(&self, version: String) -> zbus::Result<String>;

    #[zbus(signal)]
    async fn status(&self, status: String) -> zbus::Result<()>;
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct ResultReport {
    version: String,
    status: TaskStatus,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
enum TaskStatus {
    Success,
    Failed(String),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct DpkgProgress {
    status: String,
    stage: String,
    package_or_dpkg_exec: String,
    percent: f32,
    description: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum Progress {
    Dpkg(DpkgProgress),
    Oma(oma_fetch::Event),
    Done { status: String, version: String },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if env::var("DISPLAY").is_ok() || env::var("WAYLAND_DISPLAY").is_ok() {
        Command::new("debconf-kde-helper")
            .arg("--socket-path")
            .arg("/tmp/amo-debconf-sock")
            .spawn()?;
    } else if stdout().is_terminal() {
        simple_text_debconf()?;
    }

    println!("Connecting to System D-Bus...");
    let connection = Connection::system().await?;
    let proxy = AmoContractProxy::new(&connection).await?;
    let mut status_stream = proxy.receive_status().await?;

    let packages_to_install = vec!["fish"];
    println!(
        "[Step 1] Requesting install marking for: {:?}",
        packages_to_install
    );

    println!("[Step 2] Triggering transaction commit...");
    let id = match proxy
        .apply_changes(packages_to_install, vec![], false)
        .await
    {
        Ok(id) => {
            println!("[Step 2 Dispatched] Commit request accepted by server.");
            println!(
                "The server is processing the download/installation asynchronously in the background."
            );
            id
        }
        Err(e) => {
            bail!("[Step 2 Failed] Failed to trigger commit: {}", e);
        }
    };

    println!("[Signal Listener] Thread started, waiting for progress events...");
    while let Some(signal) = status_stream.next().await {
        let status = signal.args()?.status;
        let status: Progress = serde_json::from_str(&status)?;
        println!("Status: {:?}", status);
        if let Progress::Done { status, version } = status
            && version == id
        {
            println!("Status: {}({})", status, version);
            break;
        }
    }

    let result = proxy.get_last_result(id).await?;
    let result: Option<ResultReport> = serde_json::from_str(&result)?;

    println!("Client finished successfully.");
    println!("{:#?}", result);

    Ok(())
}

fn simple_text_debconf() -> anyhow::Result<()> {
    let socket_path = "/tmp/amo-debconf-sock";
    let _ = std::fs::remove_file(socket_path);
    let listener = tokio::net::UnixListener::bind(socket_path)?;
    let _ = std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o666));

    tokio::spawn(async move {
        loop {
            if let Ok((backend_stream, _)) = listener.accept().await {
                use dialoguer::{Confirm, Input, Select};
                use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

                let (backend_reader, mut backend_writer) = backend_stream.into_split();
                let mut reader = BufReader::new(backend_reader);
                let mut line = String::new();

                let mut current_description = String::from("Please select configure item");
                let mut select_choices: Vec<String> = Vec::new();
                let mut last_user_answer = String::new();
                let mut current_question_type = String::new();

                while let Ok(n) = reader.read_line(&mut line).await {
                    if n == 0 {
                        break;
                    }

                    let cmd = parse_line(&line);
                    println!("[Debconf Gateway] Received command: {:?}", cmd);

                    let response = match cmd {
                        DebconfCommand::Capb(_) => Some(DebconfResponse::CapbSuccess(
                            Capability::Multiselect | Capability::Escape,
                        )),
                        DebconfCommand::Title(title) => {
                            println!("=== {} ===", title);
                            Some(DebconfResponse::Ok)
                        }
                        DebconfCommand::Description {
                            question: _,
                            content,
                        } => {
                            match content {
                                DescriptionContent::Type(t) => {
                                    current_question_type = t;
                                }
                                DescriptionContent::Short(text) => {
                                    current_description = text;
                                }
                                DescriptionContent::Extended(text) => {
                                    current_description = text;
                                }
                                DescriptionContent::Unknown(text) => {
                                    current_description = text;
                                }
                            }
                            Some(DebconfResponse::Ok)
                        }
                        DebconfCommand::Choices(choices) => {
                            select_choices = choices;
                            Some(DebconfResponse::Ok)
                        }
                        DebconfCommand::Input {
                            priority: _,
                            question,
                        } => {
                            if current_question_type == "string" {
                                if question.contains("boolean") {
                                    current_question_type = String::from("boolean");
                                } else if question.contains("select") {
                                    current_question_type = String::from("select");
                                } else if question.contains("note") || question.contains("error") {
                                    current_question_type = String::from("note");
                                }
                            }

                            Some(DebconfResponse::Ok)
                        }
                        DebconfCommand::Go => {
                            let desc = current_description.clone();
                            let q_type = current_question_type.clone();
                            let choices = select_choices.clone();

                            let answer_result = tokio::task::spawn_blocking(move || -> String {
                                match q_type.as_str() {
                                    "note" | "error" => {
                                        println!("\n Notice:");
                                        println!("\n{}", desc);

                                        let _ =
                                            Input::<String>::with_theme(&ColorfulTheme::default())
                                                .with_prompt("Press [Enter] to continue")
                                                .allow_empty(true)
                                                .report(false)
                                                .interact_text()
                                                .unwrap_or_default();

                                        String::new()
                                    }
                                    "boolean" => {
                                        if Confirm::with_theme(&ColorfulTheme::default())
                                            .with_prompt(&desc)
                                            .default(true)
                                            .interact()
                                            .unwrap_or(true)
                                        {
                                            "true".to_string()
                                        } else {
                                            "false".to_string()
                                        }
                                    }
                                    "select" => {
                                        if !choices.is_empty() {
                                            let selection =
                                                Select::with_theme(&ColorfulTheme::default())
                                                    .with_prompt(&desc)
                                                    .items(&choices)
                                                    .default(0)
                                                    .interact()
                                                    .unwrap_or(0);
                                            choices[selection].clone()
                                        } else {
                                            String::new()
                                        }
                                    }
                                    _ => Input::<String>::with_theme(&ColorfulTheme::default())
                                        .with_prompt(&desc)
                                        .allow_empty(true)
                                        .interact_text()
                                        .unwrap_or_default(),
                                }
                            })
                            .await;

                            if let Ok(ans) = answer_result {
                                last_user_answer = ans;
                            }

                            select_choices.clear();
                            Some(DebconfResponse::Ok)
                        }
                        DebconfCommand::Get(_) => {
                            Some(DebconfResponse::Answer(last_user_answer.clone()))
                        }
                        DebconfCommand::Goodbye => None,
                        _ => Some(DebconfResponse::Ok),
                    };

                    match response {
                        Some(resp) => {
                            println!("[Debconf Gateway] Sending response: {:?}", resp);
                            let raw_resp = format!("{}\n", resp);
                            if backend_writer.write_all(raw_resp.as_bytes()).await.is_err() {
                                break;
                            }
                            let _ = backend_writer.flush().await;
                        }
                        None => {
                            break;
                        }
                    }

                    line.clear();
                }
            }
        }
    });

    Ok(())
}
