//! P4 workflow tests: chat/profile/sync/usage/import command
//! boundary over the daemon surface. Worker results park for
//! verification and never pass; chat never approves; public commands
//! never grant system rights.

#[path = "common/mod.rs"]
mod common;

use common::*;

#[test]
fn chat_posts_without_approving() {
    let dir = state_dir("flow-chat");
    let server = start_server(&dir);
    let token = server.token();
    let run = create_run(&server, 181);
    let before = get_snapshot(&server, &run);
    let revision_before = before["state"]["revision"].as_u64().expect("rev");
    let (status, body) = http(
        "POST",
        &server.addr,
        &format!("/v1/runs/{run}/chat"),
        Some(&token),
        Some(&serde_json::json!({"body": "Should we split task A?"}).to_string()),
        &[],
    );
    assert_eq!(status, 200, "chat: {body}");
    let message: serde_json::Value = serde_json::from_str(&body).expect("chat json");
    assert_eq!(message["seq"], 1, "first message: {message}");
    // Chat is discussion only: the run revision does not move and no
    // plan is proposed, approved or dispatched.
    let after = get_snapshot(&server, &run);
    assert_eq!(
        after["state"]["revision"].as_u64().expect("rev"),
        revision_before
    );
    assert!(
        after["state"]["pending_plan"].is_null(),
        "no proposal: {after}"
    );
    assert_eq!(server.fake_sends(), 0, "chat sends nothing");
    let (status, body) = http(
        "GET",
        &server.addr,
        &format!("/v1/runs/{run}/chat?after=0"),
        Some(&token),
        None,
        &[],
    );
    assert_eq!(status, 200, "history: {body}");
    let page: serde_json::Value = serde_json::from_str(&body).expect("page");
    assert_eq!(page["items"].as_array().expect("items").len(), 1);
    // A read-scoped client may read chat but never post.
    let issued = issue_client(&server, "reader", "read");
    let reader = client_token(&issued);
    let (status, _) = http(
        "POST",
        &server.addr,
        &format!("/v1/runs/{run}/chat"),
        Some(&reader),
        Some(&serde_json::json!({"body": "hi"}).to_string()),
        &[],
    );
    assert_eq!(status, 403, "read scope cannot chat");
}

#[test]
fn profiles_are_user_supplied_and_frozen_for_attempts() {
    let dir = state_dir("flow-profiles");
    let server = start_server(&dir);
    let token = server.token();
    let profile = serde_json::json!({
        "profile_id": "codex-low",
        "name": "Codex low effort",
        "provider": "codex",
        "model": "gpt-6-luna",
        "effort": "low",
        "config": {"skills": [], "subagents": []},
    });
    let (status, body) = http(
        "POST",
        &server.addr,
        "/v1/profiles",
        Some(&token),
        Some(&profile.to_string()),
        &[],
    );
    assert_eq!(status, 200, "create profile: {body}");
    let (status, body) = http("GET", &server.addr, "/v1/profiles", Some(&token), None, &[]);
    assert_eq!(status, 200, "list: {body}");
    let list: serde_json::Value = serde_json::from_str(&body).expect("list");
    assert_eq!(list["items"].as_array().expect("items").len(), 1);
    let (status, body) = http(
        "GET",
        &server.addr,
        "/v1/profiles/codex-low",
        Some(&token),
        None,
        &[],
    );
    assert_eq!(status, 200, "get: {body}");
    let one: serde_json::Value = serde_json::from_str(&body).expect("profile");
    assert_eq!(one["model"], "gpt-6-luna", "model: {one}");
    assert!(one["capability"].is_object(), "capability assessed: {one}");
    let (status, _) = http(
        "GET",
        &server.addr,
        "/v1/profiles/missing",
        Some(&token),
        None,
        &[],
    );
    assert_eq!(status, 404, "unknown profile");
}

#[test]
fn public_commands_never_grant_system_rights() {
    let dir = state_dir("flow-system");
    let server = start_server(&dir);
    let run = create_run(&server, 191);
    // Scheduler-internal commands are not operator rights: the public
    // gateway refuses them even for the owner.
    for (id, command) in [
        (
            192,
            serde_json::json!({"QueueAttempt": {"attempt": "00000000-0000-0000-0000-000000000001", "task": "00000000-0000-0000-0000-000000000001", "input": "x", "resources": [], "max_parallel": 1}}),
        ),
        (
            193,
            serde_json::json!({"StartDispatch": {"attempt": "00000000-0000-0000-0000-000000000001"}}),
        ),
        (
            194,
            serde_json::json!({"AttemptRunning": {"attempt": "00000000-0000-0000-0000-000000000001", "session": {"provider": "p", "instance_epoch": "e", "thread_id": "t"}}}),
        ),
    ] {
        let (status, body) = post_command(&server, Some(&run), id, command);
        assert_eq!(status, 403, "system command over public API: {body}");
        let value: serde_json::Value = body;
        assert_eq!(value["error"]["code"], "forbidden", "code: {value}");
    }
}

#[test]
fn usage_ledger_and_gap_resolution() {
    let dir = state_dir("flow-usage");
    let server = start_server(&dir);
    let token = server.token();
    let run = create_run(&server, 201);
    let (status, body) = http(
        "GET",
        &server.addr,
        &format!("/v1/runs/{run}/usage"),
        Some(&token),
        None,
        &[],
    );
    assert_eq!(status, 200, "usage: {body}");
    let usage: serde_json::Value = serde_json::from_str(&body).expect("usage");
    assert!(usage["ledger"].is_object(), "ledger: {usage}");
    assert!(usage["admission"].is_object(), "admission: {usage}");
    let (status, body) = http(
        "GET",
        &server.addr,
        &format!("/v1/runs/{run}/recovery"),
        Some(&token),
        None,
        &[],
    );
    assert_eq!(status, 200, "recovery: {body}");
    let recovery: serde_json::Value = serde_json::from_str(&body).expect("recovery");
    assert!(recovery["actions"].is_array(), "actions: {recovery}");
    // Resolving an unknown gap without a key is a 400, not a hold clear.
    let (status, body) = http(
        "POST",
        &server.addr,
        &format!("/v1/runs/{run}/usage/resolve"),
        Some(&token),
        Some(&serde_json::json!({"missing_key": "", "note": "x"}).to_string()),
        &[],
    );
    assert_eq!(status, 400, "empty gap key: {body}");
}

#[test]
fn usage_reconcile_folds_audited_observation() {
    let dir = state_dir("flow-usage-fold");
    let server = start_server(&dir);
    let token = server.token();
    let run = create_run(&server, 221);
    // Missing identity fields are a 400, never a silent ledger entry.
    let (status, body) = http(
        "POST",
        &server.addr,
        &format!("/v1/runs/{run}/usage/reconcile"),
        Some(&token),
        Some(&serde_json::json!({"provider": "", "epoch": "e", "turn": "t"}).to_string()),
        &[],
    );
    assert_eq!(status, 400, "missing identity: {body}");
    // An audited fold lands in the ledger as observed tokens.
    let (status, body) = http(
        "POST",
        &server.addr,
        &format!("/v1/runs/{run}/usage/reconcile"),
        Some(&token),
        Some(
            &serde_json::json!({
                "provider": "codex",
                "epoch": "epoch-1",
                "turn": "turn-7",
                "sequence": 1,
                "tokens": 21601,
                "metric": "tokens",
                "source": "live",
                "finality": "observed",
            })
            .to_string(),
        ),
        &[],
    );
    assert_eq!(status, 200, "reconcile: {body}");
    let (status, body) = http(
        "GET",
        &server.addr,
        &format!("/v1/runs/{run}/usage"),
        Some(&token),
        None,
        &[],
    );
    assert_eq!(status, 200, "usage: {body}");
    let usage: serde_json::Value = serde_json::from_str(&body).expect("usage");
    assert_eq!(
        usage["ledger"]["observed_tokens"], 21601,
        "observed total: {usage}"
    );
    // Replaying the same provider/epoch/turn/sequence is a no-op: the
    // ledger counts each turn exactly once.
    let (status, body) = http(
        "POST",
        &server.addr,
        &format!("/v1/runs/{run}/usage/reconcile"),
        Some(&token),
        Some(
            &serde_json::json!({
                "provider": "codex",
                "epoch": "epoch-1",
                "turn": "turn-7",
                "sequence": 1,
                "tokens": 99999,
                "metric": "tokens",
                "source": "live",
                "finality": "observed",
            })
            .to_string(),
        ),
        &[],
    );
    assert_eq!(status, 200, "reconcile replay: {body}");
    let (_status, body) = http(
        "GET",
        &server.addr,
        &format!("/v1/runs/{run}/usage"),
        Some(&token),
        None,
        &[],
    );
    let usage: serde_json::Value = serde_json::from_str(&body).expect("usage");
    assert_eq!(
        usage["ledger"]["observed_tokens"], 21601,
        "no double count: {usage}"
    );
}

#[test]
fn plans_tasks_attempts_read_models() {
    let dir = state_dir("flow-reads");
    let server = start_server(&dir);
    let token = server.token();
    let run = create_run(&server, 211);
    let _hash = approve_fixture_plan(&server, &run, 212);
    let (status, body) = http(
        "GET",
        &server.addr,
        &format!("/v1/runs/{run}/plans"),
        Some(&token),
        None,
        &[],
    );
    assert_eq!(status, 200, "plans: {body}");
    let plans: serde_json::Value = serde_json::from_str(&body).expect("plans");
    assert_eq!(plans["items"].as_array().expect("items").len(), 1);
    let hash = plans["items"][0]["hash"]
        .as_str()
        .expect("hash")
        .to_string();
    let (status, body) = http(
        "GET",
        &server.addr,
        &format!("/v1/runs/{run}/plans/{hash}"),
        Some(&token),
        None,
        &[],
    );
    assert_eq!(status, 200, "plan: {body}");
    let (status, body) = http(
        "GET",
        &server.addr,
        &format!("/v1/runs/{run}/tasks"),
        Some(&token),
        None,
        &[],
    );
    assert_eq!(status, 200, "tasks: {body}");
    let tasks: serde_json::Value = serde_json::from_str(&body).expect("tasks");
    assert_eq!(
        tasks["items"].as_array().expect("items").len(),
        2,
        "two tasks: {tasks}"
    );
    let task_id = tasks["items"][0]["task_id"]
        .as_str()
        .expect("task")
        .to_string();
    let (status, body) = http(
        "GET",
        &server.addr,
        &format!("/v1/runs/{run}/tasks/{task_id}"),
        Some(&token),
        None,
        &[],
    );
    assert_eq!(status, 200, "task: {body}");
    let (status, _) = http(
        "GET",
        &server.addr,
        &format!("/v1/runs/{run}/tasks/00000000-0000-0000-0000-000000000000"),
        Some(&token),
        None,
        &[],
    );
    assert_eq!(status, 404, "unknown task");
}

fn git_repo(name: &str) -> (std::path::PathBuf, String) {
    let root = state_dir(name);
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).expect("repo dir");
    let git = |args: &[&str]| {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(&repo)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_AUTHOR_NAME", "Central Test")
            .env("GIT_AUTHOR_EMAIL", "central-test@local")
            .env("GIT_COMMITTER_NAME", "Central Test")
            .env("GIT_COMMITTER_EMAIL", "central-test@local")
            .output()
            .expect("spawn git");
        assert!(output.status.success(), "git {args:?} failed");
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    };
    git(&["init"]);
    std::fs::write(repo.join("calc.py"), "VALUE = 1\n").expect("fixture file");
    git(&["add", "-A"]);
    git(&["commit", "-m", "base"]);
    let head = git(&["rev-parse", "HEAD"]);
    (repo, head)
}

fn poll_running(server: &Server, run: &str, task: &str) -> (String, serde_json::Value) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let snapshot = get_snapshot(server, run);
        if let Some(attempt) = snapshot["state"]["attempts"]
            .as_array()
            .expect("attempts")
            .iter()
            .find(|a| {
                a["task_id"] == task
                    && (a["status"] == "Running" || a["status"] == "UnknownOutcome")
            })
        {
            return (
                attempt["attempt_id"]
                    .as_str()
                    .expect("attempt id")
                    .to_string(),
                snapshot,
            );
        }
        assert!(
            std::time::Instant::now() < deadline,
            "scan never dispatched task {task}"
        );
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

fn import_result(
    server: &Server,
    token: &str,
    run: &str,
    attempt: &str,
    input: &str,
    candidate: &str,
) -> serde_json::Value {
    let (status, body) = post_until_settled(
        server,
        token,
        &format!("/v1/runs/{run}/attempts/{attempt}/results"),
        &serde_json::json!({"version": 1, "attemptId": attempt, "inputHash": input, "candidatePath": "out.txt", "candidateHash": candidate, "evidencePaths": [], "summary": "worker done"})
            .to_string(),
    );
    assert_eq!(status, 200, "result import: {body}");
    body
}

/// POST until the run stops moving under the scan loop. A 409 means the
/// background tick committed first — nothing was applied, so the identical
/// body is safe to resend. Any other status returns immediately.
fn post_until_settled(
    server: &Server,
    token: &str,
    path: &str,
    body: &str,
) -> (u16, serde_json::Value) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let (status, text) = http("POST", &server.addr, path, Some(token), Some(body), &[]);
        let value: serde_json::Value =
            serde_json::from_str(&text).unwrap_or(serde_json::Value::Null);
        let code = value["error"]["code"].as_str().unwrap_or("");
        let message = value["error"]["message"].as_str().unwrap_or("");
        let raced =
            status == 409 && (code == "stale_revision" || message.contains("expected revision"));
        if !raced {
            return (status, value);
        }
        assert!(
            std::time::Instant::now() < deadline,
            "run never settled for {path}: {text}"
        );
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

fn verdict_body(
    task: &str,
    attempt: &str,
    subject: &str,
    criteria: &str,
    outcome: &str,
    failed: Vec<serde_json::Value>,
) -> String {
    serde_json::json!({
        "task": task,
        "attempt": attempt,
        "verdict": {"version": 1, "subjectHash": subject, "criteriaHash": criteria, "outcome": outcome, "evidenceIds": ["input-seal"], "findings": "verifier judged the sealed candidate"},
        "context": {"verifierThread": "verifier-9", "environment": "test-env", "checkRefs": ["check-1"], "failedCriteria": failed, "verifierWroteTrackedSource": false, "taintedEvidence": [], "sameSessionOverride": false},
    })
    .to_string()
}

fn task_status(snapshot: &serde_json::Value, task: &str) -> String {
    snapshot["state"]["tasks"]
        .as_array()
        .expect("tasks")
        .iter()
        .find(|t| t["task_id"] == task)
        .unwrap_or_else(|| panic!("no task {task}"))["status"]
        .as_str()
        .expect("status")
        .to_string()
}

#[test]
fn verdict_pass_moves_task_to_succeeded() {
    let (repo, head) = git_repo("verdict-pass");
    let dir = state_dir("verdict-pass-server");
    let server = start_server_with_scan(&dir, "200");
    let token = server.token();
    let run = create_run(&server, 301);
    let _hash = approve_fixture_plan(&server, &run, 302);
    let (status, body) = http(
        "POST",
        &server.addr,
        &format!("/v1/runs/{run}/sync"),
        Some(&token),
        Some(
            &serde_json::json!({"commit": head, "repository": repo.to_string_lossy()}).to_string(),
        ),
        &[],
    );
    assert_eq!(status, 200, "sync: {body}");
    let (status, body) = http(
        "POST",
        &server.addr,
        &format!("/v1/runs/{run}/dispatch"),
        Some(&token),
        Some("{}"),
        &[],
    );
    assert_eq!(status, 200, "dispatch: {body}");

    let task = "00000000-0000-0000-0000-000000000001";
    let (attempt, _) = poll_running(&server, &run, task);
    let seal = import_result(
        &server,
        &token,
        &run,
        &attempt,
        "input-seal",
        "candidate-seal",
    );
    let criteria = seal["sealed"]["criteria_hash"]
        .as_str()
        .expect("criteria")
        .to_string();
    assert_eq!(
        seal["sealed"]["status"], "awaiting_verification",
        "seal parks: {seal}"
    );

    let (status, body) = post_until_settled(
        &server,
        &token,
        &format!("/v1/runs/{run}/verdicts"),
        &verdict_body(task, &attempt, "candidate-seal", &criteria, "pass", vec![]),
    );
    assert_eq!(status, 200, "verdict: {body}");
    let snapshot = get_snapshot(&server, &run);
    assert_eq!(
        task_status(&snapshot, task),
        "Succeeded",
        "pass promotes: {snapshot}"
    );

    // A second verdict on the closed task is stale, never a double apply.
    let (status, body) = post_until_settled(
        &server,
        &token,
        &format!("/v1/runs/{run}/verdicts"),
        &verdict_body(task, &attempt, "candidate-seal", &criteria, "pass", vec![]),
    );
    assert_eq!(status, 409, "re-verdict refused: {body}");
}

#[test]
fn verdict_guards_seal_shape_and_scope() {
    let (repo, head) = git_repo("verdict-guards");
    let dir = state_dir("verdict-guards-server");
    let server = start_server_with_scan(&dir, "200");
    let token = server.token();
    let run = create_run(&server, 311);
    let _hash = approve_fixture_plan(&server, &run, 312);
    let (status, body) = http(
        "POST",
        &server.addr,
        &format!("/v1/runs/{run}/sync"),
        Some(&token),
        Some(
            &serde_json::json!({"commit": head, "repository": repo.to_string_lossy()}).to_string(),
        ),
        &[],
    );
    assert_eq!(status, 200, "sync: {body}");
    let (status, body) = http(
        "POST",
        &server.addr,
        &format!("/v1/runs/{run}/dispatch"),
        Some(&token),
        Some("{}"),
        &[],
    );
    assert_eq!(status, 200, "dispatch: {body}");

    let task = "00000000-0000-0000-0000-000000000002";
    let (attempt, _) = poll_running(&server, &run, task);
    let seal = import_result(
        &server,
        &token,
        &run,
        &attempt,
        "input-seal",
        "candidate-seal",
    );
    let criteria = seal["sealed"]["criteria_hash"]
        .as_str()
        .expect("criteria")
        .to_string();

    // A verdict for another subject is stale: the seal stays parked.
    let (status, body) = post_until_settled(
        &server,
        &token,
        &format!("/v1/runs/{run}/verdicts"),
        &verdict_body(task, &attempt, "candidate-other", &criteria, "pass", vec![]),
    );
    assert_eq!(status, 409, "foreign subject: {body}");
    let snapshot = get_snapshot(&server, &run);
    assert_eq!(
        task_status(&snapshot, task),
        "AwaitingVerification",
        "still parked: {snapshot}"
    );

    // A fail that names no failed criteria is a 400, not a silent repair.
    let (status, body) = post_until_settled(
        &server,
        &token,
        &format!("/v1/runs/{run}/verdicts"),
        &verdict_body(task, &attempt, "candidate-seal", &criteria, "fail", vec![]),
    );
    assert_eq!(status, 400, "empty fail: {body}");

    // A read-scoped client may read but never judge.
    let issued = issue_client(&server, "reader", "read");
    let reader = client_token(&issued);
    let (status, _) = http(
        "POST",
        &server.addr,
        &format!("/v1/runs/{run}/verdicts"),
        Some(&reader),
        Some(&verdict_body(
            task,
            &attempt,
            "candidate-seal",
            &criteria,
            "fail",
            vec![serde_json::json!("b done")],
        )),
        &[],
    );
    assert_eq!(status, 403, "read scope cannot judge");
}

#[test]
fn worker_result_import_parks_without_passing() {
    let dir = state_dir("flow-results");
    let server = start_server(&dir);
    let token = server.token();
    let run = create_run(&server, 221);
    let _hash = approve_fixture_plan(&server, &run, 222);
    // A foreign attempt is refused, never recorded. The body is a
    // well-formed report so the refusal proves the membership gate,
    // not the parser.
    let (status, body) = http(
        "POST",
        &server.addr,
        &format!("/v1/runs/{run}/attempts/00000000-0000-0000-0000-000000000099/results"),
        Some(&token),
        Some(
            &serde_json::json!({"version": 1, "attemptId": "00000000-0000-0000-0000-000000000099", "inputHash": "bb", "candidatePath": "out.txt", "candidateHash": "aa", "evidencePaths": [], "summary": "done"})
                .to_string(),
        ),
        &[],
    );
    assert!(status == 404 || status == 400, "foreign result: {body}");
    let value: serde_json::Value = serde_json::from_str(&body).unwrap_or(serde_json::Value::Null);
    assert!(value["error"]["code"].as_str().is_some(), "code: {body}");
    let snapshot = get_snapshot(&server, &run);
    let succeeded = snapshot["state"]["tasks"]
        .as_array()
        .expect("tasks")
        .iter()
        .any(|task| task["status"] == "Succeeded");
    assert!(!succeeded, "no pass from a refused import: {snapshot}");
}
