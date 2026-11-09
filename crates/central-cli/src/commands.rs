//! CLI command implementations. All run operations go through the
//! daemon API; the only local files touched are the daemon discovery
//! files (`server.json`, `daemon.token`, `daemon.log`).

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use central_domain::{
    plan_hash, Command, CommandEnvelope, CommandId, PlanDocumentV1, Revision, RunId,
};
use central_infrastructure::process::{spawn_detached, DetachPlacement, ALREADY_RUNNING_EXIT_CODE};

use crate::client::{health_ok, read_server_info, resolve_state_dir, DaemonClient};
use crate::{Cli, Commands, DaemonCommands, HandoverCommands, PlanCommands, ResultCommands};

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
        Commands::Result { command } => result(&state_dir, command),
        Commands::Handover { command } => handover_cmd(&state_dir, command),
        Commands::Pause { run } => simple(&state_dir, run, Command::Pause),
        Commands::Stop { run } => simple(&state_dir, run, Command::Stop),
        Commands::Resume { run } => simple(&state_dir, run, Command::Resume),
        Commands::Inbox { run } => inbox(&state_dir, run),
        Commands::Approve {
            run,
            key,
            preview_hash,
        } => inbox_approve(&state_dir, run, key, preview_hash),
        Commands::Decline {
            run,
            key,
            preview_hash,
            reason,
        } => inbox_decline(&state_dir, run, key, preview_hash, reason),
        Commands::Answer {
            run,
            key,
            preview_hash,
            message,
            secret,
        } => inbox_answer(&state_dir, run, key, preview_hash, message, secret),
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
                        "schema": "central/plan-v1",
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

/// Validate a worker-result file: schema, attempt scope against the flag
/// and the run state, contained paths. Prints the content hash; the report
/// proposes a candidate only — committing it lands in P2/T5 verification.
fn result(state_dir: &Path, command: &ResultCommands) -> i32 {
    let ResultCommands::Import { run, attempt, file } = command;
    let text = match std::fs::read_to_string(file) {
        Ok(text) => text,
        Err(error) => return fail(&format!("cannot read result file: {error}")),
    };
    let result = match central_application::handover::parse_worker_result(&text) {
        Ok(result) => result,
        Err(error) => return fail(&format!("invalid worker result: {error}")),
    };
    if result.attempt_id != *attempt {
        return fail(&format!(
            "result names attempt {} but --attempt is {attempt}",
            result.attempt_id,
        ));
    }
    let client = match client(state_dir) {
        Ok(client) => client,
        Err(code) => return code,
    };
    match client.get(&format!("/v1/runs/{run}")) {
        Ok(view) => {
            if view["state"]["attempts"][attempt.to_string()].is_null() {
                return fail(&format!(
                    "foreign candidate: attempt {attempt} does not belong to run {run}"
                ));
            }
        }
        Err(message) => return fail(&message),
    }
    print_json(&serde_json::json!({
        "schema": central_application::handover::WORKER_RESULT_SCHEMA,
        "attempt_id": attempt.to_string(),
        "candidate_path": result.candidate_path,
        "candidate_hash": result.candidate_hash.as_str(),
        "result_hash": central_application::handover::worker_result_hash(&result).as_str(),
        "status": "validated; committing the candidate lands in P2/T5 verification",
    }));
    0
}

/// Validate a handover file: schema, contained artifact paths, and the
/// verified/unverified separation. Prints the content hash for the new
/// session to quote when it resumes.
fn handover_cmd(state_dir: &Path, command: &HandoverCommands) -> i32 {
    let HandoverCommands::Import { run, file } = command;
    let text = match std::fs::read_to_string(file) {
        Ok(text) => text,
        Err(error) => return fail(&format!("cannot read handover file: {error}")),
    };
    let handover = match central_application::handover::parse_handover(&text) {
        Ok(handover) => handover,
        Err(error) => return fail(&format!("invalid handover: {error}")),
    };
    let client = match client(state_dir) {
        Ok(client) => client,
        Err(code) => return code,
    };
    match client.get(&format!("/v1/runs/{run}")) {
        Ok(view) => {
            if view["state"]["attempts"][handover.predecessor.to_string()].is_null() {
                return fail(&format!(
                    "foreign handover: predecessor {} does not belong to run {run}",
                    handover.predecessor,
                ));
            }
        }
        Err(message) => return fail(&message),
    }
    print_json(&serde_json::json!({
        "schema": central_application::handover::HANDOVER_SCHEMA,
        "predecessor": handover.predecessor.to_string(),
        "handover_hash": central_application::handover::handover_hash(&handover).as_str(),
        "verified_facts": handover.verified_facts.len(),
        "unverified_notes": handover.unverified_notes.len(),
        "status": "validated; the new session must inspect evidence before writing",
    }));
    0
}

/// List the durable inbox entries of a run: pending, answered, resolved
/// and stale requests with the preview hash the user must quote back.
fn inbox(state_dir: &Path, run: &RunId) -> i32 {
    let client = match client(state_dir) {
        Ok(client) => client,
        Err(code) => return code,
    };
    match client.get(&format!("/v1/runs/{run}/inbox")) {
        Ok(value) => {
            print_json(&value);
            0
        }
        Err(message) => fail(&message),
    }
}

/// Approve a pending action approval. The key is a canonical inbox key,
/// never a plan hash: plans approve through `central plan approve` only.
fn inbox_approve(state_dir: &Path, run: &RunId, key: &str, preview_hash: &str) -> i32 {
    let client = match client(state_dir) {
        Ok(client) => client,
        Err(code) => return code,
    };
    match client.post_json(
        &format!("/v1/runs/{run}/inbox/approve"),
        &serde_json::json!({ "key": key, "preview_hash": preview_hash }),
    ) {
        Ok(receipt) => {
            print_json(&receipt);
            0
        }
        Err(message) => fail(&message),
    }
}

/// Decline a pending action approval with a reason for the harness.
fn inbox_decline(
    state_dir: &Path,
    run: &RunId,
    key: &str,
    preview_hash: &str,
    reason: &Option<String>,
) -> i32 {
    let client = match client(state_dir) {
        Ok(client) => client,
        Err(code) => return code,
    };
    match client.post_json(
        &format!("/v1/runs/{run}/inbox/decline"),
        &serde_json::json!({ "key": key, "preview_hash": preview_hash, "reason": reason }),
    ) {
        Ok(receipt) => {
            print_json(&receipt);
            0
        }
        Err(message) => fail(&message),
    }
}

/// Answer a pending question. Exactly one of message (plain) or secret
/// (memory-only, never persisted) is sent; the daemon rejects both or
/// neither before anything is committed.
fn inbox_answer(
    state_dir: &Path,
    run: &RunId,
    key: &str,
    preview_hash: &str,
    message: &Option<String>,
    secret: &Option<String>,
) -> i32 {
    let client = match client(state_dir) {
        Ok(client) => client,
        Err(code) => return code,
    };
    match client.post_json(
        &format!("/v1/runs/{run}/inbox/answer"),
        &serde_json::json!({ "key": key, "preview_hash": preview_hash, "message": message, "secret": secret }),
    ) {
        Ok(receipt) => {
            print_json(&receipt);
            0
        }
        Err(message) => fail(&message),
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
            let detached = match spawn_detached(&server_exe, &args, &log_path) {
                Ok(detached) => detached,
                Err(error) => return fail(&format!("cannot spawn daemon: {error}")),
            };
            if let DetachPlacement::JoinedParentJob(reason) = &detached.placement {
                eprintln!("warning: daemon runs in the inherited job ({reason})");
            }
            let mut child = detached.child;
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
                        let job = match &detached.placement {
                            DetachPlacement::EscapedJob => "escaped",
                            DetachPlacement::JoinedParentJob(_) => "inherited",
                        };
                        print_json(&serde_json::json!({
                            "status": "started",
                            "addr": info.addr,
                            "pid": info.pid,
                            "fake_agent": info.fake_agent,
                            "job": job,
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
