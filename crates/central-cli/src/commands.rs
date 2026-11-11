//! CLI command implementations. All run operations go through the
//! daemon API; the only local files touched are the daemon discovery
//! files (`server.json`, `daemon.token`, `daemon.log`). Every command
//! prints stable JSON and exits nonzero with the daemon's error code
//! on failure. Secrets never travel via argv: `--secret-stdin` reads
//! them from stdin.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use central_domain::{
    plan_hash, AttemptId, Command, CommandEnvelope, CommandId, PlanDocumentV1, Revision, RunId,
};
use central_infrastructure::process::{spawn_detached, DetachPlacement, ALREADY_RUNNING_EXIT_CODE};

use crate::client::{health_ok, read_server_info, resolve_state_dir, DaemonClient};
use crate::{
    Cli, ClientCommands, Commands, DaemonCommands, HandoverCommands, MaintenanceCommands,
    PlanCommands, ProfileCommands, ResultCommands,
};

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
        Commands::Meta => meta(&state_dir),
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
        Commands::Budget { run } => budget(&state_dir, run),
        Commands::UsageResolve {
            run,
            missing_key,
            note,
        } => usage_resolve(&state_dir, run, missing_key, note),
        Commands::Recovery { run } => recovery(&state_dir, run.as_ref()),
        Commands::Sync {
            run,
            commit,
            repository,
        } => sync(&state_dir, run, commit, repository.as_deref()),
        Commands::SetLimits {
            run,
            max_tokens,
            max_cost_minor,
            max_attempts,
            max_duration_ms,
        } => set_limits(
            &state_dir,
            run,
            *max_tokens,
            *max_cost_minor,
            *max_attempts,
            *max_duration_ms,
        ),
        Commands::Reconcile { run, attempt } => reconcile(&state_dir, run, attempt),
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
            secret_stdin,
        } => inbox_answer(&state_dir, run, key, preview_hash, message, *secret_stdin),
        Commands::Events {
            run,
            after,
            limit,
            follow,
        } => events(&state_dir, run, after.as_deref(), *limit, *follow),
        Commands::Logs {
            run,
            attempt,
            limit,
            follow,
        } => logs(&state_dir, run, attempt.as_ref(), *limit, *follow),
        Commands::Artifacts {
            run,
            id,
            content,
            offset,
            limit,
        } => artifacts(&state_dir, run, id.as_deref(), *content, *offset, *limit),
        Commands::Chat {
            run,
            message,
            after,
            limit,
        } => chat(
            &state_dir,
            run,
            message.as_deref(),
            after.as_deref(),
            *limit,
        ),
        Commands::Profiles { command } => profiles(&state_dir, command),
        Commands::Clients { command } => clients(&state_dir, command),
        Commands::Maintenance { command } => maintenance(&state_dir, command),
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

/// Whether the run snapshot contains the attempt (the projected
/// `state.attempts` is an array, never a map).
fn snapshot_has_attempt(view: &serde_json::Value, attempt: &AttemptId) -> bool {
    view["state"]["attempts"]
        .as_array()
        .is_some_and(|attempts| {
            attempts
                .iter()
                .any(|entry| entry["attempt_id"] == attempt.to_string())
        })
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

/// Discovery: the daemon's version, instance and real capabilities.
/// The version check already ran in `DaemonClient::discover`; this
/// prints the same document for scripts and independent clients.
fn meta(state_dir: &Path) -> i32 {
    let client = match client(state_dir) {
        Ok(client) => client,
        Err(code) => return code,
    };
    match client.get("/v1/meta") {
        Ok(value) => {
            print_json(&value);
            0
        }
        Err(message) => fail(&message),
    }
}

/// Passively reconcile one attempt through the daemon: the harness
/// evidence is stored durably and returned. Nothing is resent, answered
/// or promoted by this call.
fn reconcile(state_dir: &Path, run: &RunId, attempt: &AttemptId) -> i32 {
    let client = match client(state_dir) {
        Ok(client) => client,
        Err(code) => return code,
    };
    match client.post_json(
        &format!("/v1/runs/{run}/attempts/{attempt}/reconcile"),
        &serde_json::json!({}),
    ) {
        Ok(value) => {
            print_json(&value);
            0
        }
        Err(message) => fail(&message),
    }
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

/// Show the shared budget ledger of a run: limits, totals, unknown
/// usage with its evidence, and the current holds.
fn budget(state_dir: &Path, run: &RunId) -> i32 {
    let client = match client(state_dir) {
        Ok(client) => client,
        Err(code) => return code,
    };
    match client.get(&format!("/v1/runs/{run}/usage")) {
        Ok(view) => {
            print_json(&view);
            0
        }
        Err(message) => fail(&message),
    }
}

/// Resolve one missing-usage gap by explicit audited decision. The
/// hold releases without converting anything missing into observed.
fn usage_resolve(state_dir: &Path, run: &RunId, missing_key: &str, note: &str) -> i32 {
    let client = match client(state_dir) {
        Ok(client) => client,
        Err(code) => return code,
    };
    match client.post_json(
        &format!("/v1/runs/{run}/usage/resolve"),
        &serde_json::json!({ "missing_key": missing_key, "note": note }),
    ) {
        Ok(receipt) => {
            print_json(&receipt);
            0
        }
        Err(message) => fail(&message),
    }
}

/// Recovery report for one run (or the daemon overview without --run).
/// Passive findings plus required actions; never dispatches or resumes.
fn recovery(state_dir: &Path, run: Option<&RunId>) -> i32 {
    let client = match client(state_dir) {
        Ok(client) => client,
        Err(code) => return code,
    };
    let result = match run {
        Some(run) => client.get(&format!("/v1/runs/{run}/recovery")),
        None => client.get("/v1/recovery"),
    };
    match result {
        Ok(view) => {
            print_json(&view);
            0
        }
        Err(message) => fail(&message),
    }
}

/// Queue the user's commit through the daemon: the commit is verified
/// read-only daemon-side and the worktree is never touched — no
/// checkout, stash or pull. The CLI never writes queue files.
fn sync(state_dir: &Path, run: &RunId, commit: &str, repository: Option<&Path>) -> i32 {
    let client = match client(state_dir) {
        Ok(client) => client,
        Err(code) => return code,
    };
    let mut body = serde_json::json!({ "commit": commit });
    if let Some(repository) = repository {
        body["repository"] = serde_json::Value::String(repository.to_string_lossy().into_owned());
    }
    match client.post_json(&format!("/v1/runs/{run}/sync"), &body) {
        Ok(value) => {
            print_json(&value);
            0
        }
        Err(message) => fail(&message),
    }
}

/// Set the run's shared limits. The daemon records the authenticated
/// user as the limits' author for audit.
fn set_limits(
    state_dir: &Path,
    run: &RunId,
    max_tokens: Option<u64>,
    max_cost_minor: Option<u64>,
    max_attempts: Option<u64>,
    max_duration_ms: Option<i64>,
) -> i32 {
    let client = match client(state_dir) {
        Ok(client) => client,
        Err(code) => return code,
    };
    let revision = match current_revision(&client, run) {
        Ok(revision) => revision,
        Err(message) => return fail(&message),
    };
    match send_command(
        &client,
        Some(run),
        Command::SetLimits {
            limits: central_domain::UsageLimits {
                max_tokens,
                max_cost_minor,
                max_attempts,
                max_duration_ms,
                extra_caps: Default::default(),
                updated_by: None,
            },
        },
        Some(revision),
    ) {
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
        PlanCommands::Show { run, hash } => {
            let path = match hash {
                Some(hash) => format!("/v1/runs/{run}/plans/{hash}"),
                None => format!("/v1/runs/{run}/plans"),
            };
            match client.get(&path) {
                Ok(value) => {
                    print_json(&value);
                    0
                }
                Err(message) => fail(&message),
            }
        }
        PlanCommands::Diff { run, from, to } => {
            match client.get(&format!("/v1/runs/{run}/plans/diff?from={from}&to={to}")) {
                Ok(value) => {
                    print_json(&value);
                    0
                }
                Err(message) => fail(&message),
            }
        }
    }
}

/// Import a worker-result file through the daemon: validated, sealed
/// and parked for verification. Importing never marks a task passed —
/// only an independent verifier verdict can do that.
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
            if !snapshot_has_attempt(&view, attempt) {
                return fail(&format!(
                    "foreign candidate: attempt {attempt} does not belong to run {run}"
                ));
            }
        }
        Err(message) => return fail(&message),
    }
    match client.post_text(&format!("/v1/runs/{run}/attempts/{attempt}/results"), &text) {
        Ok(value) => {
            print_json(&value);
            0
        }
        Err(message) => fail(&message),
    }
}

/// Import a handover file through the daemon with evidence inspection.
/// Unbacked claims stay unverified; the new session must inspect
/// evidence before writing.
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
            if !snapshot_has_attempt(&view, &handover.predecessor) {
                return fail(&format!(
                    "foreign handover: predecessor {} does not belong to run {run}",
                    handover.predecessor,
                ));
            }
        }
        Err(message) => return fail(&message),
    }
    match client.post_text(&format!("/v1/runs/{run}/handover"), &text) {
        Ok(value) => {
            print_json(&value);
            0
        }
        Err(message) => fail(&message),
    }
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

/// Answer a pending question. Exactly one of message (plain) or a
/// secret read from stdin (memory-only, never argv or shell history)
/// is sent; the daemon rejects both or neither before anything commits.
fn inbox_answer(
    state_dir: &Path,
    run: &RunId,
    key: &str,
    preview_hash: &str,
    message: &Option<String>,
    secret_stdin: bool,
) -> i32 {
    let secret = if secret_stdin {
        let mut text = String::new();
        match std::io::stdin().read_to_string(&mut text) {
            Ok(_) => Some(text.trim_end_matches(['\r', '\n']).to_string()),
            Err(error) => return fail(&format!("cannot read secret from stdin: {error}")),
        }
    } else {
        None
    };
    if message.is_some() == secret.as_ref().is_some_and(|text| !text.is_empty()) {
        return fail("answer needs exactly one of --message or --secret-stdin");
    }
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

/// Ordered event replay with durable cursors. `--follow` polls from
/// the cursor every second until interrupted; every page prints the
/// `next_cursor` to resume from.
fn events(
    state_dir: &Path,
    run: &RunId,
    after: Option<&str>,
    limit: Option<u32>,
    follow: bool,
) -> i32 {
    let client = match client(state_dir) {
        Ok(client) => client,
        Err(code) => return code,
    };
    let mut cursor = after.map(|text| text.to_string());
    loop {
        let mut path = format!("/v1/runs/{run}/events");
        let mut query = Vec::new();
        if let Some(after) = &cursor {
            query.push(format!("after={after}"));
        }
        if let Some(limit) = limit {
            query.push(format!("limit={limit}"));
        }
        if !query.is_empty() {
            path.push('?');
            path.push_str(&query.join("&"));
        }
        match client.get(&path) {
            Ok(page) => {
                print_json(&page);
                match page["next_cursor"].as_str() {
                    Some(next) if follow => {
                        cursor = Some(next.to_string());
                        std::thread::sleep(Duration::from_secs(1));
                    }
                    _ => return 0,
                }
            }
            Err(message) => return fail(&message),
        }
    }
}

/// Bounded evidence reads, optionally for one attempt. `--follow`
/// polls until interrupted.
fn logs(
    state_dir: &Path,
    run: &RunId,
    attempt: Option<&AttemptId>,
    limit: Option<u32>,
    follow: bool,
) -> i32 {
    let client = match client(state_dir) {
        Ok(client) => client,
        Err(code) => return code,
    };
    loop {
        let mut path = format!("/v1/runs/{run}/logs");
        let mut query = Vec::new();
        if let Some(attempt) = attempt {
            query.push(format!("attempt={attempt}"));
        }
        if let Some(limit) = limit {
            query.push(format!("limit={limit}"));
        }
        if !query.is_empty() {
            path.push('?');
            path.push_str(&query.join("&"));
        }
        match client.get(&path) {
            Ok(value) => {
                print_json(&value);
                if !follow {
                    return 0;
                }
                std::thread::sleep(Duration::from_secs(1));
            }
            Err(message) => return fail(&message),
        }
    }
}

/// Artifact metadata, or a hash-verified content window for one id.
fn artifacts(
    state_dir: &Path,
    run: &RunId,
    id: Option<&str>,
    content: bool,
    offset: Option<usize>,
    limit: Option<usize>,
) -> i32 {
    let client = match client(state_dir) {
        Ok(client) => client,
        Err(code) => return code,
    };
    let result = match (id, content) {
        (None, _) => client.get(&format!("/v1/runs/{run}/artifacts")),
        (Some(id), false) => client.get(&format!("/v1/runs/{run}/artifacts/{id}")),
        (Some(id), true) => {
            let mut path = format!("/v1/runs/{run}/artifacts/{id}/content");
            let mut query = Vec::new();
            if let Some(offset) = offset {
                query.push(format!("offset={offset}"));
            }
            if let Some(limit) = limit {
                query.push(format!("limit={limit}"));
            }
            if !query.is_empty() {
                path.push('?');
                path.push_str(&query.join("&"));
            }
            client.get(&path)
        }
    };
    match result {
        Ok(value) => {
            print_json(&value);
            0
        }
        Err(message) => fail(&message),
    }
}

/// Post a discussion message, or read history. Posting never approves,
/// answers or dispatches — it is discussion only.
fn chat(
    state_dir: &Path,
    run: &RunId,
    message: Option<&str>,
    after: Option<&str>,
    limit: Option<u32>,
) -> i32 {
    let client = match client(state_dir) {
        Ok(client) => client,
        Err(code) => return code,
    };
    if let Some(message) = message {
        return match client.post_json(
            &format!("/v1/runs/{run}/chat"),
            &serde_json::json!({ "body": message }),
        ) {
            Ok(value) => {
                print_json(&value);
                0
            }
            Err(message) => fail(&message),
        };
    }
    let mut path = format!("/v1/runs/{run}/chat");
    let mut query = Vec::new();
    if let Some(after) = after {
        query.push(format!("after={after}"));
    }
    if let Some(limit) = limit {
        query.push(format!("limit={limit}"));
    }
    if !query.is_empty() {
        path.push('?');
        path.push_str(&query.join("&"));
    }
    match client.get(&path) {
        Ok(value) => {
            print_json(&value);
            0
        }
        Err(message) => fail(&message),
    }
}

fn profiles(state_dir: &Path, command: &ProfileCommands) -> i32 {
    let client = match client(state_dir) {
        Ok(client) => client,
        Err(code) => return code,
    };
    let result = match command {
        ProfileCommands::List => client.get("/v1/profiles"),
        ProfileCommands::Get { id } => client.get(&format!("/v1/profiles/{id}")),
        ProfileCommands::Create {
            id,
            name,
            provider,
            model,
            effort,
        } => {
            let mut body = serde_json::json!({
                "name": name,
                "provider": provider,
                "model": model,
            });
            if let Some(id) = id {
                body["profile_id"] = serde_json::Value::String(id.clone());
            }
            if let Some(effort) = effort {
                body["effort"] = serde_json::Value::String(effort.clone());
            }
            client.post_json("/v1/profiles", &body)
        }
    };
    match result {
        Ok(value) => {
            print_json(&value);
            0
        }
        Err(message) => fail(&message),
    }
}

/// Client credentials (owner only). Issuance prints the secret once —
/// it is never stored, and `--secret`-style argv never carries one.
fn clients(state_dir: &Path, command: &ClientCommands) -> i32 {
    let client = match client(state_dir) {
        Ok(client) => client,
        Err(code) => return code,
    };
    let result = match command {
        ClientCommands::List => client.get("/v1/clients"),
        ClientCommands::Create {
            name,
            scope,
            role,
            attempt,
        } => {
            let mut body = serde_json::json!({ "name": name, "scope": scope });
            if let Some(role) = role {
                body["role"] = serde_json::Value::String(role.clone());
            }
            if let Some(attempt) = attempt {
                body["attempt"] = serde_json::Value::String(attempt.to_string());
            }
            client.post_json("/v1/clients", &body)
        }
        ClientCommands::Revoke { id } => {
            client.post_json(&format!("/v1/clients/{id}/revoke"), &serde_json::json!({}))
        }
    };
    match result {
        Ok(value) => {
            print_json(&value);
            0
        }
        Err(message) => fail(&message),
    }
}

/// Owner-only maintenance over the same use cases as the API: a
/// redacted report, or an explicit dry-run collection preview.
fn maintenance(state_dir: &Path, command: &MaintenanceCommands) -> i32 {
    let client = match client(state_dir) {
        Ok(client) => client,
        Err(code) => return code,
    };
    let result = match command {
        MaintenanceCommands::Export => {
            client.post_json("/v1/maintenance/export", &serde_json::json!({}))
        }
        MaintenanceCommands::Gc { dry_run } => client.post_json(
            "/v1/maintenance/gc",
            &serde_json::json!({ "dry_run": dry_run }),
        ),
    };
    match result {
        Ok(value) => {
            print_json(&value);
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
                    // Never abandon a half-started daemon: a process left
                    // behind here holds the owner lock with no listener —
                    // a zombie that blocks every later start with a
                    // misleading already-running. Re-check once (it may
                    // have become healthy since the last poll), then stop
                    // our own child before reporting the failure.
                    let healthy = read_server_info(state_dir)
                        .is_some_and(|info| info.pid == child_pid && health_ok(&info.addr));
                    if !healthy {
                        let _ = child.kill();
                        let _ = child.wait();
                    }
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
