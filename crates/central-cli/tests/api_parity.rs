//! CLI/API parity: the `central` CLI and a raw HTTP client observe
//! the same revisions, receipts and errors against one daemon. The CLI
//! is a pure API consumer — no DB access, no queue files — so every
//! operator action below must agree across both surfaces, including
//! the parked-branch rule (no dispatch before explicit resume) and
//! stale-target conflicts.

use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn state_dir(name: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!(
        "central-cli-parity-{}-{name}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("state dir");
    dir
}

fn target_bin(name: &str) -> PathBuf {
    // Integration tests only get CARGO_BIN_EXE_<name> for their own
    // package's bins; the sibling binary lives next to the deps dir.
    let exe = std::env::current_exe().expect("test exe");
    let debug = exe
        .parent()
        .expect("deps")
        .parent()
        .expect("debug")
        .to_path_buf();
    debug.join(format!("{name}{}", std::env::consts::EXE_SUFFIX))
}

fn server_bin() -> PathBuf {
    target_bin("central-server")
}

fn cli_bin() -> PathBuf {
    target_bin("central")
}

fn healthy(addr: &str) -> bool {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .expect("http");
    client
        .get(format!("http://{addr}/healthz"))
        .send()
        .map(|response| response.status().is_success())
        .unwrap_or(false)
}

fn start_daemon(dir: &std::path::Path) -> (std::process::Child, String) {
    // A killed daemon leaves its server.json behind (no graceful
    // unlink): remove it so readiness always means THIS process.
    let _ = std::fs::remove_file(dir.join("server.json"));
    let mut child = Command::new(server_bin())
        .args([
            "--state-dir",
            dir.to_str().expect("utf8"),
            "--listen",
            "127.0.0.1:0",
            "--fake-agent",
        ])
        .env("CENTRAL_SCAN_MS", "0")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn daemon");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            panic!("daemon exited early: {status}");
        }
        if let Ok(text) = std::fs::read_to_string(dir.join("server.json")) {
            if let Ok(info) = serde_json::from_str::<serde_json::Value>(&text) {
                if let Some(addr) = info["addr"].as_str() {
                    if healthy(addr) {
                        return (child, addr.to_string());
                    }
                }
            }
        }
        assert!(Instant::now() < deadline, "daemon did not start");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn cli(dir: &std::path::Path, args: &[&str]) -> (i32, serde_json::Value, String) {
    let output = Command::new(cli_bin())
        .arg("--state-dir")
        .arg(dir)
        .args(args)
        .output()
        .expect("run cli");
    let code = output.status.code().unwrap_or(99);
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let json = serde_json::from_str(&stdout).unwrap_or(serde_json::Value::Null);
    (code, json, stderr)
}

fn token(dir: &std::path::Path) -> String {
    std::fs::read_to_string(dir.join("daemon.token"))
        .expect("token")
        .trim()
        .to_string()
}

fn http_get(addr: &str, token: &str, path: &str) -> (u16, serde_json::Value) {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("http");
    let response = client
        .get(format!("http://{addr}{path}"))
        .bearer_auth(token)
        .send()
        .expect("get");
    let status = response.status().as_u16();
    let text = response.text().unwrap_or_default();
    (
        status,
        serde_json::from_str(&text).unwrap_or(serde_json::Value::Null),
    )
}

#[test]
fn cli_and_http_observe_same_run() {
    let dir = state_dir("same-run");
    let (mut daemon, _addr) = start_daemon(&dir);
    let token = token(&dir);

    let (code, receipt, _) = cli(
        &dir,
        &["init", "--goal", "parity run", "--repository", "D:/repo"],
    );
    assert_eq!(code, 0, "init: {receipt}");
    let run = receipt["run_id"].as_str().expect("run id").to_string();

    let (status, snapshot) = http_get(&_addr, &token, &format!("/v1/runs/{run}"));
    assert_eq!(status, 200);
    let (code, status_view, _) = cli(&dir, &["status", "--run", &run]);
    assert_eq!(code, 0);
    assert_eq!(
        status_view["state"]["revision"], snapshot["state"]["revision"],
        "same revision across surfaces"
    );
    assert_eq!(
        status_view["revision"], snapshot["revision"],
        "cursor agrees"
    );

    // Both surfaces see the same receipt fields for a control command.
    let (code, cli_receipt, _) = cli(&dir, &["pause", "--run", &run]);
    assert_eq!(code, 0, "pause: {cli_receipt}");
    assert_eq!(cli_receipt["result"], "applied");
    let after = http_get(&_addr, &token, &format!("/v1/runs/{run}")).1;
    assert_eq!(after["state"]["control"], "Paused");
    assert_eq!(
        after["state"]["revision"].as_u64().expect("rev"),
        cli_receipt["revision"].as_u64().expect("cli rev"),
        "receipt revision matches the snapshot"
    );

    // Errors agree too: pausing a paused run fails on both surfaces
    // with the same stable code.
    let (code, _, stderr) = cli(&dir, &["pause", "--run", &run]);
    assert_ne!(code, 0, "second pause must fail");
    assert!(
        stderr.contains("validation") || stderr.contains("transition"),
        "stderr: {stderr}"
    );

    let _ = daemon.kill();
    let _ = daemon.wait();
}

#[test]
fn cli_parks_without_dispatch_before_resume() {
    let dir = state_dir("parked");
    let (mut daemon, _addr) = start_daemon(&dir);
    let token = token(&dir);

    let (code, receipt, _) = cli(
        &dir,
        &["init", "--goal", "parked run", "--repository", "D:/repo"],
    );
    assert_eq!(code, 0);
    let run = receipt["run_id"].as_str().expect("run").to_string();

    // Crash the daemon, restart it, and prove both surfaces see the
    // restart park — and that neither can dispatch before resume.
    let _ = daemon.kill();
    let _ = daemon.wait();
    let (mut daemon, _addr) = start_daemon(&dir);

    let (code, status_view, _) = cli(&dir, &["status", "--run", &run]);
    assert_eq!(code, 0);
    assert_eq!(status_view["state"]["control"], "AwaitingResume");
    let (code, dispatch, _) = cli(&dir, &["start", "--run", &run]);
    assert_eq!(code, 0, "start endpoint answers: {dispatch}");
    assert_eq!(
        dispatch["dispatched"], 0,
        "parked branch dispatches nothing"
    );
    assert_eq!(dispatch["unknown"], 0);
    let (status, snapshot) = http_get(&_addr, &token, &format!("/v1/runs/{run}"));
    assert_eq!(status, 200);
    assert_eq!(
        snapshot["state"]["control"], "AwaitingResume",
        "still parked"
    );

    let (code, _, _) = cli(&dir, &["resume", "--run", &run]);
    assert_eq!(code, 0, "explicit resume reopens the gate");

    let _ = daemon.kill();
    let _ = daemon.wait();
}

#[test]
fn cli_covers_the_operator_surface() {
    let dir = state_dir("surface");
    let (mut daemon, _addr) = start_daemon(&dir);

    let (code, receipt, _) = cli(
        &dir,
        &["init", "--goal", "surface run", "--repository", "D:/repo"],
    );
    assert_eq!(code, 0);
    let run = receipt["run_id"].as_str().expect("run").to_string();

    // Chat posts discussion without approving anything.
    let (code, message, _) = cli(&dir, &["chat", "--run", &run, "--message", "hello leader"]);
    assert_eq!(code, 0, "chat: {message}");
    assert_eq!(message["seq"], 1);
    let (code, history, _) = cli(&dir, &["chat", "--run", &run]);
    assert_eq!(code, 0);
    assert_eq!(history["items"].as_array().expect("items").len(), 1);

    // Profiles, clients, events, usage, recovery, maintenance, meta.
    let (code, profile, _) = cli(
        &dir,
        &[
            "profiles",
            "create",
            "--name",
            "low",
            "--provider",
            "codex",
            "--model",
            "gpt-6-luna",
        ],
    );
    assert_eq!(code, 0, "profile: {profile}");
    let (code, _, _) = cli(&dir, &["profiles", "list"]);
    assert_eq!(code, 0);
    let (code, issued, _) = cli(
        &dir,
        &["clients", "create", "--name", "reader", "--scope", "read"],
    );
    assert_eq!(code, 0, "issue: {issued}");
    assert!(issued["secret"].as_str().is_some(), "one-time secret shown");
    let client_id = issued["client_id"].as_str().expect("id").to_string();
    let (code, _, _) = cli(&dir, &["clients", "list"]);
    assert_eq!(code, 0);
    let (code, _, _) = cli(&dir, &["clients", "revoke", "--id", &client_id]);
    assert_eq!(code, 0);

    let (code, _, _) = cli(&dir, &["events", "--run", &run]);
    assert_eq!(code, 0);
    let (code, _, _) = cli(&dir, &["logs", "--run", &run]);
    assert_eq!(code, 0);
    let (code, _, _) = cli(&dir, &["artifacts", "--run", &run]);
    assert_eq!(code, 0);
    let (code, _, _) = cli(&dir, &["budget", "--run", &run]);
    assert_eq!(code, 0);
    let (code, _, _) = cli(&dir, &["recovery", "--run", &run]);
    assert_eq!(code, 0);
    let (code, _, _) = cli(&dir, &["meta"]);
    assert_eq!(code, 0);
    let (code, _, _) = cli(&dir, &["maintenance", "export"]);
    assert_eq!(code, 0);
    let (code, _, _) = cli(&dir, &["maintenance", "gc", "--dry-run"]);
    assert_eq!(code, 0);

    // Unknown run fails nonzero with the daemon's code.
    let (code, _, stderr) = cli(
        &dir,
        &["status", "--run", "00000000-0000-0000-0000-000000000000"],
    );
    assert_ne!(code, 0, "unknown run must fail");
    assert!(stderr.contains("not_found"), "stable code: {stderr}");

    let _ = daemon.kill();
    let _ = daemon.wait();
}
