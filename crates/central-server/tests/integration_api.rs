//! P3 Task 5 production path: owner-driven Git integration through the
//! daemon API — seal the attempt worktree, prepare the temporary branch,
//! run the approved checks, and promote the good ref by compare-and-swap.
//! Real fixture git repos, real checks, real ref moves; no model, no fake
//! harness output.

#[path = "common/mod.rs"]
mod common;

use common::*;
use serde_json::{json, Value};

fn git(repo: &std::path::Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(repo)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_AUTHOR_NAME", "Central Test")
        .env("GIT_AUTHOR_EMAIL", "central-test@local")
        .env("GIT_COMMITTER_NAME", "Central Test")
        .env("GIT_COMMITTER_EMAIL", "central-test@local")
        .output()
        .expect("spawn git");
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

fn fixture_repo(name: &str) -> (std::path::PathBuf, String) {
    let root = state_dir(name);
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).expect("repo dir");
    git(&repo, &["init"]);
    std::fs::write(repo.join("calc.py"), "VALUE = 1\n").expect("fixture file");
    git(&repo, &["add", "-A"]);
    git(&repo, &["commit", "-m", "base"]);
    let head = git(&repo, &["rev-parse", "HEAD"]);
    git(&repo, &["update-ref", "refs/heads/central-good", &head]);
    (repo, head)
}

/// Poll the run snapshot until the scan loop queued and dispatched an
/// attempt: queueing alone proves nothing, the worktree appears only
/// after the dispatch pass provisions it.
fn poll_attempts(server: &Server, run: &str) -> serde_json::Value {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let snapshot = get_snapshot(server, run);
        let attempts = snapshot["state"]["attempts"].as_array().expect("attempts");
        if attempts
            .iter()
            .any(|a| a["status"] == "Running" || a["status"] == "UnknownOutcome")
        {
            return snapshot;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "scan never dispatched an attempt"
        );
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

fn plan_hash() -> String {
    "dd".repeat(32)
}

fn approved_checks() -> Value {
    #[cfg(windows)]
    {
        json!({
            "baseline": [{"program": "cmd", "args": ["/C", "exit 0"]}],
            "affected": [],
            "acceptance": [{"program": "cmd", "args": ["/C", "if not exist calc.py exit 1"]}],
        })
    }
    #[cfg(not(windows))]
    {
        json!({
            "baseline": [{"program": "sh", "args": ["-c", "exit 0"]}],
            "affected": [],
            "acceptance": [{"program": "sh", "args": ["-c", "test -f calc.py"]}],
        })
    }
}

/// Full pass: fixture repo → sync → approve → dispatch (fake agent) →
/// write the worker file into the provisioned worktree → integration run
/// seals, checks and promotes; the good ref moves exactly once.
#[test]
fn integration_run_promotes_on_pass() {
    let (repo, head) = fixture_repo("api-integration-pass");
    let dir = state_dir("api-integration-server");
    let server = start_server_with_scan(&dir, "200");
    let token = server.token();

    // Run targets the real fixture repo so dispatch provisions a worktree.
    let (status, receipt) = post_command(
        &server,
        None,
        1,
        json!({"CreateRun": {"goal": "integrate", "repository": repo.to_string_lossy()}}),
    );
    assert_eq!(status, 200, "create run: {receipt}");
    let run = receipt["run_id"].as_str().expect("run id").to_string();

    let _hash = approve_fixture_plan(&server, &run, 2);
    let (status, body) = http(
        "POST",
        &server.addr,
        &format!("/v1/runs/{run}/sync"),
        Some(&token),
        Some(&json!({"commit": head, "repository": repo.to_string_lossy()}).to_string()),
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

    // The scan loop queues and dispatches; wait for the worker attempt.
    let snapshot = poll_attempts(&server, &run);
    let attempts = snapshot["state"]["attempts"].as_array().expect("attempts");
    let attempt = attempts
        .iter()
        .find(|a| a["task_id"] == "00000000-0000-0000-0000-000000000001")
        .expect("task A attempt");
    let attempt_id = attempt["attempt_id"].as_str().expect("attempt id");
    let worktrees = std::fs::read_dir(dir.join("worktrees"))
        .expect("worktrees dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(attempt_id))
        })
        .collect::<Vec<_>>();
    assert_eq!(worktrees.len(), 1, "one worktree for the attempt");
    std::fs::write(worktrees[0].join("calc.py"), "VALUE = 2\n").expect("worker edit");

    let (status, body) = http(
        "POST",
        &server.addr,
        &format!("/v1/runs/{run}/integration/run"),
        Some(&token),
        Some(
            &json!({
                "attempt": attempt_id,
                "base": head,
                "refName": "refs/heads/central-good",
                "expectedParent": head,
                "approvedPlan": plan_hash(),
                "checks": approved_checks(),
            })
            .to_string(),
        ),
        &[],
    );
    assert_eq!(status, 200, "integration run: {body}");
    let report: Value = serde_json::from_str(&body).expect("integration json");
    assert_eq!(report["summary"]["passed"], true, "checks pass: {report}");
    let new_head = report["receipt"]["new_head"].as_str().expect("new head");
    assert_ne!(new_head, head, "promotion moved the ref: {report}");
    assert_eq!(
        git(&repo, &["rev-parse", "--verify", "refs/heads/central-good"]),
        new_head,
        "good ref points at the merge"
    );
}

/// Failed checks never move the good ref; the attempt stays sealed and a
/// fresh candidate plus checks are required.
#[test]
fn integration_run_refuses_on_failed_checks() {
    let (repo, head) = fixture_repo("api-integration-fail");
    let dir = state_dir("api-integration-server-fail");
    let server = start_server_with_scan(&dir, "200");
    let token = server.token();

    let (status, receipt) = post_command(
        &server,
        None,
        11,
        json!({"CreateRun": {"goal": "integrate", "repository": repo.to_string_lossy()}}),
    );
    assert_eq!(status, 200, "create run: {receipt}");
    let run = receipt["run_id"].as_str().expect("run id").to_string();

    let _hash = approve_fixture_plan(&server, &run, 12);
    let (status, body) = http(
        "POST",
        &server.addr,
        &format!("/v1/runs/{run}/sync"),
        Some(&token),
        Some(&json!({"commit": head, "repository": repo.to_string_lossy()}).to_string()),
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

    // The scan loop queues and dispatches; wait for the worker attempt.
    let snapshot = poll_attempts(&server, &run);
    let attempts = snapshot["state"]["attempts"].as_array().expect("attempts");
    let attempt = attempts
        .iter()
        .find(|a| a["task_id"] == "00000000-0000-0000-0000-000000000001")
        .expect("task A attempt");
    let attempt_id = attempt["attempt_id"].as_str().expect("attempt id");
    let worktrees = std::fs::read_dir(dir.join("worktrees"))
        .expect("worktrees dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(attempt_id))
        })
        .collect::<Vec<_>>();
    assert_eq!(worktrees.len(), 1, "one worktree for the attempt");
    std::fs::write(worktrees[0].join("calc.py"), "VALUE = 2\n").expect("worker edit");

    let mut checks = approved_checks();
    #[cfg(windows)]
    {
        checks["acceptance"] = json!([{"program": "cmd", "args": ["/C", "exit 1"]}]);
    }
    #[cfg(not(windows))]
    {
        checks["acceptance"] = json!([{"program": "sh", "args": ["-c", "exit 1"]}]);
    }
    let (status, body) = http(
        "POST",
        &server.addr,
        &format!("/v1/runs/{run}/integration/run"),
        Some(&token),
        Some(
            &json!({
                "attempt": attempt_id,
                "base": head,
                "refName": "refs/heads/central-good",
                "expectedParent": head,
                "approvedPlan": plan_hash(),
                "checks": checks,
            })
            .to_string(),
        ),
        &[],
    );
    assert_eq!(
        status, 403,
        "failed checks refuse without moving the ref: {status} {body}"
    );
    assert_eq!(
        git(&repo, &["rev-parse", "--verify", "refs/heads/central-good"]),
        head,
        "good ref untouched after failed checks"
    );
}

/// Unknown attempts and unknown runs fail closed: scratch-backed or
/// foreign attempts can never Git-seal.
#[test]
fn integration_run_rejects_unknown_attempt() {
    let (_repo, _head) = fixture_repo("api-integration-unknown");
    let dir = state_dir("api-integration-server-unknown");
    let server = start_server_with_scan(&dir, "200");
    let token = server.token();

    let (status, receipt) = post_command(
        &server,
        None,
        21,
        json!({"CreateRun": {"goal": "integrate", "repository": "/tmp/central-no-such-repo"}}),
    );
    assert_eq!(status, 200, "create run: {receipt}");
    let run = receipt["run_id"].as_str().expect("run id").to_string();

    let (status, body) = http(
        "POST",
        &server.addr,
        &format!("/v1/runs/{run}/integration/run"),
        Some(&token),
        Some(
            &json!({
                "attempt": "00000000-0000-0000-0000-000000000099",
                "base": "0000000000000000000000000000000000000000",
                "refName": "refs/heads/central-good",
                "expectedParent": "0000000000000000000000000000000000000000",
                "approvedPlan": plan_hash(),
                "checks": approved_checks(),
            })
            .to_string(),
        ),
        &[],
    );
    assert_eq!(status, 404, "unknown attempt fails closed: {body}");
}
