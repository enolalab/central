//! CLI command implementations. All run operations go through the
//! daemon API; the only local files touched are the daemon discovery
//! files (`server.json`, `daemon.token`, `daemon.log`).

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use central_domain::{
    plan_hash, Command, CommandEnvelope, CommandId, PlanDocumentV1, Revision, RunId,
};
use central_infrastructure::process::{spawn_detached, ALREADY_RUNNING_EXIT_CODE};

use crate::client::{health_ok, read_server_info, resolve_state_dir, DaemonClient};
use crate::{Cli, Commands, DaemonCommands, PlanCommands};

fn print_json(value: &serde_json::Value) {
    println!(
        "{}",
        serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
    );
}

fn fail(message: &str) -> i32 {
    eprintln!("central: {message}");
    1
}

pub fn execute(cli: Cli) -> i32 {
    let state_dir = resolve_state_dir(cli.state_dir.clone());
    match &cli.command {
        Commands::Daemon { command } => daemon(&state_dir, command),
        Commands::Init { goal, repository } => {
            let client = match client(&state_dir) {
                Ok(client) => client,
                Err(code) => return code,
            };
            match send_command(
                &client,
                None,
                Command::CreateRun {
                    goal: goal.clone(),
                    repository: repository.clone(),
                },
                None,
            ) {
                Ok(receipt) => {
                    print_json(&receipt);
                    0
                }
                Err(message) => fail(&message),
            }
        }
        Commands::Start { run } => {
            let client = match client(&state_dir) {
                Ok(client) => client,
                Err(code) => return code,
            };
            match client.post_json(&format!("/v1/runs/{run}/dispatch"), &serde_json::json!({})) {
                Ok(summary) => {
                    print_json(&summary);
                    0
                }
                Err(message) => fail(&message),
            }
        }
        Commands::Status { run } => {
            let client = match client(&state_dir) {
                Ok(client) => client,
                Err(code) => return code,
            };
            let result = match run {
                Some(run) => client.get(&format!("/v1/runs/{run}")),
                None => client.get("/v1/runs"),
            };
            match result {
                Ok(value) => {
                    print_json(&value);
                    0
                }
                Err(message) => fail(&message),
            }
        }
        Commands::Plan { command } => plan(&state_dir, command),
        Commands::Pause { run } => simple(&state_dir, run, Command::Pause),
        Commands::Stop { run } => simple(&state_dir, run, Command::Stop),
        Commands::Resume { run } => simple(&state_dir, run, Command::Resume),
    }
}

fn client(state_dir: &Path) -> Result<DaemonClient, i32> {
    DaemonClient::discover(state_dir).map_err(|message| fail(&message))
}

fn current_revision(client: &DaemonClient, run: &RunId) -> Result<Revision, String> {
    let view = client.get(&format!("/v1/runs/{run}"))?;
    let revision = view["state"]["revision"]
        .as_u64()
        .ok_or("run response has no revision")?;
    Ok(Revision(revision))
}

fn send_command(
    client: &DaemonClient,
    run: Option<&RunId>,
    command: Command,
    expected_revision: Option<Revision>,
) -> Result<serde_json::Value, String> {
    let envelope = CommandEnvelope {
        id: CommandId::new(),
        expected_revision,
        command,
    };
    let body = serde_json::to_value(&envelope)
        .map_err(|error| format!("cannot encode command: {error}"))?;
    let path = match run {
        Some(run) => format!("/v1/commands?run={run}"),
        None => "/v1/commands".to_string(),
    };
    client.post_json(&path, &body)
}

fn simple(state_dir: &Path, run: &RunId, command: Command) -> i32 {
    let client = match client(state_dir) {
        Ok(client) => client,
        Err(code) => return code,
    };
    let revision = match current_revision(&client, run) {
        Ok(revision) => revision,
        Err(message) => return fail(&message),
    };
    match send_command(&client, Some(run), command, Some(revision)) {
        Ok(receipt) => {
            print_json(&receipt);
            0
        }
        Err(message) => fail(&message),
    }
}

fn plan(state_dir: &Path, command: &PlanCommands) -> i32 {
    let client = match client(state_dir) {
        Ok(client) => client,
        Err(code) => return code,
    };
    match command {
        PlanCommands::Import { run, file } => {
            let text = match std::fs::read_to_string(file) {
                Ok(text) => text,
                Err(error) => return fail(&format!("cannot read plan file: {error}")),
            };
            let document: PlanDocumentV1 = match serde_json::from_str(&text) {
                Ok(document) => document,
                Err(error) => return fail(&format!("invalid plan JSON: {error}")),
            };
            let hash = plan_hash(&document);
            let revision = match current_revision(&client, run) {
                Ok(revision) => revision,
                Err(message) => return fail(&message),
            };
            match send_command(
                &client,
                Some(run),
                Command::ProposePlan { plan: document },
                Some(revision),
            ) {
                Ok(receipt) => {
                    print_json(&serde_json::json!({
                        "plan_hash": hash.to_string(),
                        "receipt": receipt,
                    }));
                    0
                }
                Err(message) => fail(&message),
            }
        }
        PlanCommands::Approve { run, hash } => {
            let hash = match hash {
                Some(hash) => hash.0.clone(),
                None => {
                    let view = match client.get(&format!("/v1/runs/{run}")) {
                        Ok(view) => view,
                        Err(message) => return fail(&message),
                    };
                    match view["state"]["pending_plan"].as_str() {
                        Some(pending) => central_domain::PlanHash::from_hex(pending),
                        None => return fail("no pending plan to approve; import one first"),
                    }
                }
            };
            let revision = match current_revision(&client, run) {
                Ok(revision) => revision,
                Err(message) => return fail(&message),
            };
            match send_command(
                &client,
                Some(run),
                Command::ApprovePlan { plan_hash: hash },
                Some(revision),
            ) {
                Ok(receipt) => {
                    print_json(&receipt);
                    0
                }
                Err(message) => fail(&message),
            }
        }
    }
}

fn daemon(state_dir: &Path, command: &DaemonCommands) -> i32 {
    match command {
        DaemonCommands::Start { listen, fake_agent } => {
            if let Some(info) = read_server_info(state_dir) {
                if health_ok(&info.addr) {
                    print_json(&serde_json::json!({
                        "status": "already-running",
                        "addr": info.addr,
                        "pid": info.pid,
                    }));
                    return ALREADY_RUNNING_EXIT_CODE;
                }
            }
            let server_exe = match sibling_server_exe() {
                Ok(path) => path,
                Err(message) => return fail(&message),
            };
            if let Err(error) = std::fs::create_dir_all(state_dir) {
                return fail(&format!("cannot create state dir: {error}"));
            }
            let mut args = vec![
                "--state-dir".to_string(),
                state_dir.to_string_lossy().to_string(),
                "--listen".to_string(),
                listen.clone(),
            ];
            if *fake_agent {
                args.push("--fake-agent".to_string());
            }
            let log_path = state_dir.join("daemon.log");
            let mut child = match spawn_detached(&server_exe, &args, &log_path) {
                Ok(child) => child,
                Err(error) => return fail(&format!("cannot spawn daemon: {error}")),
            };
            let child_pid = child.id();
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                if let Ok(Some(status)) = child.try_wait() {
                    let code = status.code().unwrap_or(2);
                    if code == ALREADY_RUNNING_EXIT_CODE {
                        print_json(&serde_json::json!({ "status": "already-running" }));
                        return ALREADY_RUNNING_EXIT_CODE;
                    }
                    return fail(&format!(
                        "daemon exited with code {code}; see {}",
                        log_path.display()
                    ));
                }
                if let Some(info) = read_server_info(state_dir) {
                    if info.pid == child_pid && health_ok(&info.addr) {
                        print_json(&serde_json::json!({
                            "status": "started",
                            "addr": info.addr,
                            "pid": info.pid,
                            "fake_agent": info.fake_agent,
                        }));
                        return 0;
                    }
                }
                if Instant::now() > deadline {
                    return fail("daemon did not become healthy in time; see daemon.log");
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
        DaemonCommands::Status => match read_server_info(state_dir) {
            Some(info) if health_ok(&info.addr) => {
                print_json(&serde_json::json!({
                    "status": "running",
                    "addr": info.addr,
                    "pid": info.pid,
                    "fake_agent": info.fake_agent,
                }));
                0
            }
            Some(info) => {
                print_json(&serde_json::json!({
                    "status": "not-responding",
                    "addr": info.addr,
                    "pid": info.pid,
                }));
                1
            }
            None => {
                print_json(&serde_json::json!({ "status": "not-running" }));
                1
            }
        },
        DaemonCommands::Shutdown => {
            let client = match client(state_dir) {
                Ok(client) => client,
                Err(code) => return code,
            };
            match client.post_json("/v1/shutdown", &serde_json::json!({})) {
                Ok(value) => {
                    print_json(&value);
                    // Wait briefly for the listener to go away so a
                    // following `daemon start` does not race the lock.
                    if let Some(info) = read_server_info(state_dir) {
                        let deadline = Instant::now() + Duration::from_secs(10);
                        while Instant::now() < deadline && health_ok(&info.addr) {
                            std::thread::sleep(Duration::from_millis(100));
                        }
                    }
                    0
                }
                Err(message) => fail(&message),
            }
        }
    }
}

fn sibling_server_exe() -> Result<PathBuf, String> {
    let current =
        std::env::current_exe().map_err(|error| format!("cannot locate current exe: {error}"))?;
    let dir = current
        .parent()
        .ok_or("current exe has no parent directory")?;
    let name = format!("central-server{}", std::env::consts::EXE_SUFFIX);
    let candidate = dir.join(name);
    if candidate.exists() {
        Ok(candidate)
    } else {
        Err(format!(
            "central-server binary not found next to the CLI at {}",
            candidate.display()
        ))
    }
}
