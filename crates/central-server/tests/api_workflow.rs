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
