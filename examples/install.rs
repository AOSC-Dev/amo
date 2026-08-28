use std::{
    env,
    io::{IsTerminal, stdout},
    os::unix::fs::PermissionsExt,
    process::Command,
};

use anyhow::bail;
use debconf::{Capability, DebconfCommand, DebconfResponse, DescriptionContent, parse_line};

use dialoguer::theme::ColorfulTheme;
use serde::{Deserialize, Serialize};

#[path = "common/mod.rs"]
mod common;
use common::{TaskStatus, TransactionClient, TxEvent};

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
    Done { status: String, request_id: u64 },
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
    let client = TransactionClient::connect().await?;
    let mut tx = client.create().await?;

    let packages_to_install = vec!["fish"];
    println!(
        "[Step 1] Requesting install marking for: {:?}",
        packages_to_install
    );

    println!("[Step 2] Triggering transaction commit...");
    if let Err(e) = tx
        .proxy
        .apply_changes(packages_to_install, vec![], false)
        .await
    {
        bail!("[Step 2 Failed] Failed to trigger commit: {}", e);
    }
    println!("[Step 2 Dispatched] Commit request accepted by server.");
    println!(
        "The server is processing the download/installation asynchronously in the background."
    );

    println!("[Signal Listener] Thread started, waiting for progress events...");
    while let Some(event) = tx.next_event().await? {
        match event {
            TxEvent::Status(event) => {
                let status: Progress = serde_json::from_value(event)?;
                println!("Status: {:?}", status);
                if let Progress::Done { status, request_id } = status {
                    let date = request_id >> 32;
                    let seq = request_id & 0xFFFFFFFF;
                    println!(
                        "Status: {}({}) date: {}, seq: {}",
                        status, request_id, date, seq
                    );
                }
            }
            TxEvent::Result(report) => {
                // 先检查 status：包解析/提交/缓存刷新失败时服务端发
                // TaskStatus::Failed，不能当成功处理。
                if let TaskStatus::Failed(e) = &report.status {
                    bail!("apply failed: {e}");
                }
                println!("Client finished successfully.");
                println!("{:#?}", report);
                return Ok(());
            }
            TxEvent::State(state) => {
                println!("State: {:?}", state);
                if state == common::TxState::Cancelled {
                    bail!("transaction cancelled");
                }
            }
        }
    }

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
