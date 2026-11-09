//! Daemon/API restart-barrier tests. Every run goes through the real
//! HTTP API of a spawned `central-server` binary (built with the
//! `test-support` feature so the fake agent is available); no test
//! edits the database to fake runtime behavior.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use central_domain::{plan_hash, PlanDocumentV1};

#[allow(non_upper_case_globals)]
const AlreadyRunning: i32 = 3;

static COUNTER: AtomicU64 = AtomicU64::new(0);

// ---------- process / http helpers ----------

fn state_dir(name: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!(
        "central-local-runtime-{}-{name}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create state dir");
    dir
}

struct Server {
    child: Child,
    dir: PathBuf,
    addr: String,
}

impl Server {
    fn token(&self) -> String {
        std::fs::read_to_string(self.dir.join("daemon.token"))
            .expect("read token")
            .trim()
            .to_string()
    }

    fn owner_actor(&self) -> String {
        let info: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(self.dir.join("server.json")).expect("server.json"),
        )
        .expect("parse server.json");
        info["actor_id"].as_str().expect("actor_id").to_string()
    }

    fn fake_sends(&self) -> u64 {
        let (status, body) = http(
            "GET",
            &self.addr,
            "/v1/test/fake-sends",
            Some(&self.token()),
            None,
        );
        assert_eq!(status, 200, "fake-sends: {body}");
        let value: serde_json::Value = serde_json::from_str(&body).expect("fake json");
        value["sends"].as_u64().expect("sends")
    }

    fn crash(&mut self) {
        // Hard kill: simulates a crash, no graceful shutdown path runs.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn_server(dir: &std::path::Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_central-server"));
    cmd.args([
        "--state-dir",
        dir.to_str().expect("utf8 dir"),
        "--listen",
        "127.0.0.1:0",
        "--fake-agent",
    ])
    .stdout(Stdio::null())
    .stderr(Stdio::piped());
    cmd
}

fn start_server(dir: &std::path::Path) -> Server {
    let mut child = spawn_server(dir).spawn().expect("spawn server");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            panic!("server exited early with {status} in {}", dir.display());
        }
        if let Ok(text) = std::fs::read_to_string(dir.join("server.json")) {
            if let Ok(info) = serde_json::from_str::<serde_json::Value>(&text) {
                if let Some(addr) = info["addr"].as_str() {
                    let (status, _) = http("GET", addr, "/healthz", None, None);
                    if status == 200 {
                        return Server {
                            child,
                            dir: dir.to_path_buf(),
                            addr: addr.to_string(),
                        };
                    }
                }
            }
        }
        assert!(Instant::now() < deadline, "server did not become healthy");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn http(
    method: &str,
    addr: &str,
    path: &str,
    token: Option<&str>,
    body: Option<&str>,
) -> (u16, String) {
    let Ok(mut stream) = TcpStream::connect(addr) else {
        return (0, String::new());
    };
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("timeout");
    let mut request = format!("{method} {path} HTTP/1.1\r\nhost: {addr}\r\nconnection: close\r\n");
    if let Some(token) = token {
        request.push_str(&format!("authorization: Bearer {token}\r\n"));
    }
    if let Some(body) = body {
        request.push_str(&format!(
            "content-type: application/json\r\ncontent-length: {}\r\n",
            body.len()
        ));
    }
    request.push_str("\r\n");
    if stream.write_all(request.as_bytes()).is_err() {
        return (0, String::new());
    }
    if let Some(body) = body {
        if stream.write_all(body.as_bytes()).is_err() {
            return (0, String::new());
        }
    }
    let mut raw = Vec::new();
    if stream.read_to_end(&mut raw).is_err() {
        return (0, String::new());
    }
    let text = String::from_utf8_lossy(&raw).to_string();
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or(0);
    let (head, body) = match text.split_once("\r\n\r\n") {
        Some((head, body)) => (head.to_string(), body.to_string()),
        None => (text.clone(), String::new()),
    };
    let decoded = if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        decode_chunked(&body)
    } else {
        body
    };
    (status, decoded)
}

fn decode_chunked(body: &str) -> String {
    let mut out = String::new();
    let mut rest = body;
    while let Some((size_line, after)) = rest.split_once("\r\n") {
        let Ok(size) = usize::from_str_radix(size_line.trim(), 16) else {
            break;
        };
        if size == 0 {
            break;
        }
        out.push_str(&after[..size.min(after.len())]);
        rest = &after[size.min(after.len())..];
        rest = rest.strip_prefix("\r\n").unwrap_or(rest);
    }
    out
}

// ---------- API helpers ----------

fn plan_json() -> (String, String) {
    let plan = r#"{
      "version": 1,
      "goal": "Build the durable runtime",
      "tasks": [
        {"id":"00000000-0000-0000-0000-000000000001","kind":"implementation","objective":"Task A","inputs":[],"scope":[],"outOfScope":[],"dependsOn":[],"dataDependsOn":[],"outputs":[],"verify":{"method":"check","criteria":["a done"]},"evidence":[],"profileId":null,"permissions":[],"maxRepairRounds":2,"requiresApproval":[],"handover":""},
        {"id":"00000000-0000-0000-0000-000000000002","kind":"implementation","objective":"Task B","inputs":[],"scope":[],"outOfScope":[],"dependsOn":[],"dataDependsOn":[],"outputs":[],"verify":{"method":"check","criteria":["b done"]},"evidence":[],"profileId":null,"permissions":[],"maxRepairRounds":2,"requiresApproval":[],"handover":""},
        {"id":"00000000-0000-0000-0000-000000000003","kind":"verification","objective":"Task C","inputs":[],"scope":[],"outOfScope":[],"dependsOn":["00000000-0000-0000-0000-000000000001"],"dataDependsOn":["00000000-0000-0000-0000-000000000001"],"outputs":[],"verify":{"method":"check","criteria":["c done"]},"evidence":[],"profileId":null,"permissions":[],"maxRepairRounds":2,"requiresApproval":[],"handover":""}
      ]
    }"#;
    let document: PlanDocumentV1 = serde_json::from_str(plan).expect("plan fixture parses");
    (plan.to_string(), plan_hash(&document).to_string())
}

fn envelope(id: u128, command: serde_json::Value) -> String {
    serde_json::json!({
        "id": format!("00000000-0000-0000-0000-{id:012x}"),
        "expected_revision": null,
        "command": command,
    })
    .to_string()
}

fn post_command(
    server: &Server,
    run: Option<&str>,
    id: u128,
    command: serde_json::Value,
) -> (u16, serde_json::Value) {
    let path = match run {
        Some(run) => format!("/v1/commands?run={run}"),
        None => "/v1/commands".to_string(),
    };
    let (status, body) = http(
        "POST",
        &server.addr,
        &path,
        Some(&server.token()),
        Some(&envelope(id, command)),
    );
    let value: serde_json::Value = serde_json::from_str(&body).unwrap_or(serde_json::Value::Null);
    (status, value)
}

fn create_run(server: &Server, id: u128) -> String {
    let (status, receipt) = post_command(
        server,
        None,
        id,
        serde_json::json!({"CreateRun": {"goal": "Build the durable runtime", "repository": "D:/Code/enolalab/central"}}),
    );
    assert_eq!(status, 200, "create run: {receipt}");
    receipt["run_id"].as_str().expect("run id").to_string()
}

fn get_run(server: &Server, run: &str) -> serde_json::Value {
    let (status, body) = http(
        "GET",
        &server.addr,
        &format!("/v1/runs/{run}"),
        Some(&server.token()),
        None,
    );
    assert_eq!(status, 200, "get run: {body}");
    serde_json::from_str(&body).expect("run json")
}

fn approve_plan(server: &Server, run: &str) {
    let (plan, hash) = plan_json();
    let plan_value: serde_json::Value = serde_json::from_str(&plan).expect("plan value");
    let (status, receipt) = post_command(
        server,
        Some(run),
        910,
        serde_json::json!({"ProposePlan": {"plan": plan_value}}),
    );
    assert_eq!(status, 200, "propose: {receipt}");
    let (status, receipt) = post_command(
        server,
        Some(run),
        911,
        serde_json::json!({"ApprovePlan": {"plan_hash": hash}}),
    );
    assert_eq!(status, 200, "approve: {receipt}");
}

// ---------- tests ----------

#[test]
fn daemon_is_single_owner() {
    let dir = state_dir("single-owner");
    let first = start_server(&dir);
    let output = spawn_server(&dir).output().expect("second spawn");
    let second_start_code = output.status.code().expect("second exit code");
    assert_eq!(second_start_code, AlreadyRunning);
    // The first daemon is untouched and still healthy.
    let (status, _) = http("GET", &first.addr, "/healthz", None, None);
    assert_eq!(status, 200);
}

#[test]
fn recover_requires_explicit_resume() {
    let dir = state_dir("recover");
    let mut server = start_server(&dir);
    let run = create_run(&server, 900);
    let other = create_run(&server, 920);
    approve_plan(&server, &run);
    let before = get_run(&server, &run);
    let events_before = before["events"].as_array().expect("events").len();
    assert_eq!(before["state"]["control"], "Running");

    server.crash();
    let restored = start_server(&dir);

    let after = get_run(&restored, &run);
    assert_eq!(after["state"]["control"], "AwaitingResume");
    assert_eq!(restored.fake_sends(), 0);
    // No duplicate events, and the independent run is intact.
    assert_eq!(
        after["events"].as_array().expect("events").len(),
        events_before
    );
    let other_after = get_run(&restored, &other);
    assert_eq!(other_after["state"]["goal"], "Build the durable runtime");
    assert_eq!(other_after["state"]["control"], "AwaitingResume");

    // Explicit resume reopens the gate; the fake workflow continues.
    let (status, receipt) = post_command(&restored, Some(&run), 912, serde_json::json!("Resume"));
    assert_eq!(status, 200, "resume: {receipt}");
    assert_eq!(get_run(&restored, &run)["state"]["control"], "Running");
    let (status, receipt) = post_command(&restored, Some(&run), 913, serde_json::json!("Pause"));
    assert_eq!(status, 200, "pause after resume: {receipt}");
    assert_eq!(get_run(&restored, &run)["state"]["control"], "Paused");
    assert_eq!(
        get_run(&restored, &run)["events"]
            .as_array()
            .expect("events")
            .len(),
        events_before + 2
    );
}

#[test]
fn auth_cannot_be_claimed_in_body() {
    let dir = state_dir("auth");
    let server = start_server(&dir);
    let claimed = "00000000-0000-0000-0000-000000000099";
    let mut body: serde_json::Value = serde_json::from_str(&envelope(
        900,
        serde_json::json!({"CreateRun": {"goal": "g", "repository": "r"}}),
    ))
    .expect("envelope");
    body["actor"] = serde_json::json!({"id": claimed, "kind": "User"});

    let bad_auth = http(
        "POST",
        &server.addr,
        "/v1/commands",
        Some("wrong-token"),
        Some(&body.to_string()),
    );
    assert_eq!(bad_auth.0, 401);

    // Even with a valid token, a body-claimed actor is ignored: the
    // token-derived owner actor is recorded instead.
    let (status, receipt) = http(
        "POST",
        &server.addr,
        "/v1/commands",
        Some(&server.token()),
        Some(&body.to_string()),
    );
    assert_eq!(status, 200, "claimed-actor create: {receipt}");
    let receipt: serde_json::Value = serde_json::from_str(&receipt).expect("receipt");
    let run = receipt["run_id"].as_str().expect("run");
    let view = get_run(&server, run);
    let created = &view["events"][0]["RunCreated"];
    assert_eq!(created["by"].as_str().expect("by"), server.owner_actor());
    assert_ne!(created["by"].as_str().expect("by"), claimed);
}

#[test]
fn storage_failure_is_not_success() {
    let dir = state_dir("storage-failure");
    // The database path is occupied by a directory: DuckDB cannot open
    // it, so the daemon must fail closed instead of serving success.
    std::fs::create_dir_all(dir.join("central.duckdb")).expect("occupy db path");
    let output = spawn_server(&dir).output().expect("spawn failing server");
    assert!(
        !output.status.success(),
        "storage failure must not exit 0: {output:?}"
    );
    let db_failure_status = {
        // Nothing may be serving health on this state dir.
        let info = std::fs::read_to_string(dir.join("server.json")).unwrap_or_default();
        let addr = serde_json::from_str::<serde_json::Value>(&info)
            .ok()
            .and_then(|v| v["addr"].as_str().map(str::to_string))
            .unwrap_or_else(|| "127.0.0.1:1".to_string());
        http("GET", &addr, "/healthz", None, None).0
    };
    assert_ne!(db_failure_status, 200);
}

#[test]
fn ui_not_built_returns_clear_503() {
    let dir = state_dir("ui");
    let server = start_server(&dir);
    let (status, body) = http("GET", &server.addr, "/", None, None);
    assert_eq!(status, 503);
    assert!(body.contains("not built"), "clear 503 body: {body}");
    let (status, _) = http("GET", &server.addr, "/healthz", None, None);
    assert_eq!(status, 200);
}
