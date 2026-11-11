//! Shared wire harness for the P4 API tests. Every test drives the
//! real daemon binary over loopback HTTP with raw TCP (no crate
//! imports, no DB access) — the same position an independent client
//! is in. The daemon runs with `--fake-agent` and `CENTRAL_SCAN_MS=0`
//! so command-path assertions stay deterministic.

#![allow(dead_code)]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn state_dir(name: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("central-p4-{}-{}-{n}", std::process::id(), name));
    std::fs::create_dir_all(&dir).expect("create state dir");
    dir
}

pub struct Server {
    pub child: Child,
    pub dir: PathBuf,
    pub addr: String,
}

impl Server {
    pub fn token(&self) -> String {
        std::fs::read_to_string(self.dir.join("daemon.token"))
            .expect("read token")
            .trim()
            .to_string()
    }

    pub fn fake_sends(&self) -> u64 {
        let (status, body) = http(
            "GET",
            &self.addr,
            "/v1/test/fake-sends",
            Some(&self.token()),
            None,
            &[],
        );
        assert_eq!(status, 200, "fake-sends: {body}");
        let value: serde_json::Value = serde_json::from_str(&body).expect("fake json");
        value["sends"].as_u64().expect("sends")
    }

    pub fn crash(&mut self) {
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

fn spawn_cmd(dir: &std::path::Path, fake_agent: bool) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_central-server"));
    cmd.args([
        "--state-dir",
        dir.to_str().expect("utf8 dir"),
        "--listen",
        "127.0.0.1:0",
    ]);
    if fake_agent {
        cmd.arg("--fake-agent");
    }
    cmd.stdout(Stdio::null()).stderr(Stdio::piped());
    cmd.env("CENTRAL_SCAN_MS", "0");
    cmd
}

/// Start the daemon against `dir`, which may already hold a database
/// and credentials from a previous boot (restart tests).
pub fn start_server_in(dir: &std::path::Path, fake_agent: bool) -> Server {
    let mut child = spawn_cmd(dir, fake_agent).spawn().expect("spawn server");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            panic!("server exited early with {status} in {}", dir.display());
        }
        if let Ok(text) = std::fs::read_to_string(dir.join("server.json")) {
            if let Ok(info) = serde_json::from_str::<serde_json::Value>(&text) {
                if let Some(addr) = info["addr"].as_str() {
                    let (status, _) = http("GET", addr, "/healthz", None, None, &[]);
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

pub fn start_server(dir: &std::path::Path) -> Server {
    start_server_in(dir, true)
}

/// Raw HTTP over one connection. Extra headers are `("name", "value")`
/// pairs (used for `Origin`, `Last-Event-ID`, ...). Returns the status
/// code and the decoded body.
pub fn http(
    method: &str,
    addr: &str,
    path: &str,
    token: Option<&str>,
    body: Option<&str>,
    extra: &[(&str, &str)],
) -> (u16, String) {
    let (status, _, body) = http_full(method, addr, path, token, body, extra);
    (status, body)
}

pub fn http_full(
    method: &str,
    addr: &str,
    path: &str,
    token: Option<&str>,
    body: Option<&str>,
    extra: &[(&str, &str)],
) -> (u16, String, String) {
    let Ok(mut stream) = TcpStream::connect(addr) else {
        return (0, String::new(), String::new());
    };
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("timeout");
    let mut request = format!("{method} {path} HTTP/1.1\r\nhost: {addr}\r\nconnection: close\r\n");
    if let Some(token) = token {
        request.push_str(&format!("authorization: Bearer {token}\r\n"));
    }
    for (name, value) in extra {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    if let Some(body) = body {
        request.push_str(&format!(
            "content-type: application/json\r\ncontent-length: {}\r\n",
            body.len()
        ));
    }
    request.push_str("\r\n");
    if stream.write_all(request.as_bytes()).is_err() {
        return (0, String::new(), String::new());
    }
    if let Some(body) = body {
        if stream.write_all(body.as_bytes()).is_err() {
            return (0, String::new(), String::new());
        }
    }
    let mut raw = Vec::new();
    if stream.read_to_end(&mut raw).is_err() {
        return (0, String::new(), String::new());
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
    (status, head, decoded)
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

/// Read an SSE stream until the first `data:` frame arrives (or a
/// short deadline passes). Returns the status and everything received.
/// Chunk framing is left intact; assertions match on frame substrings.
pub fn sse_first_bytes(
    addr: &str,
    path: &str,
    token: &str,
    extra: &[(&str, &str)],
) -> (u16, String) {
    let Ok(mut stream) = TcpStream::connect(addr) else {
        return (0, String::new());
    };
    stream
        .set_read_timeout(Some(Duration::from_millis(2500)))
        .expect("timeout");
    let mut request = format!(
        "GET {path} HTTP/1.1\r\nhost: {addr}\r\nconnection: close\r\nauthorization: Bearer {token}\r\naccept: text/event-stream\r\n"
    );
    for (name, value) in extra {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str("\r\n");
    if stream.write_all(request.as_bytes()).is_err() {
        return (0, String::new());
    }
    let mut raw = Vec::new();
    let _ = stream.read_to_end(&mut raw);
    let text = String::from_utf8_lossy(&raw).to_string();
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or(0);
    (status, text)
}

// ---------- API helpers ----------

pub fn plan_json() -> String {
    r#"{
      "version": 1,
      "goal": "Build the durable runtime",
      "tasks": [
        {"id":"00000000-0000-0000-0000-000000000001","kind":"implementation","objective":"Task A","inputs":[],"scope":[],"outOfScope":[],"dependsOn":[],"dataDependsOn":[],"outputs":[],"verify":{"method":"check","criteria":["a done"]},"evidence":[],"profileId":null,"permissions":[],"maxRepairRounds":2,"requiresApproval":[],"handover":""},
        {"id":"00000000-0000-0000-0000-000000000002","kind":"implementation","objective":"Task B","inputs":[],"scope":[],"outOfScope":[],"dependsOn":[],"dataDependsOn":[],"outputs":[],"verify":{"method":"check","criteria":["b done"]},"evidence":[],"profileId":null,"permissions":[],"maxRepairRounds":2,"requiresApproval":[],"handover":""}
      ]
    }"#
    .to_string()
}

pub fn envelope(id: u128, command: serde_json::Value) -> String {
    serde_json::json!({
        "id": format!("00000000-0000-0000-0000-{id:012x}"),
        "expected_revision": null,
        "command": command,
    })
    .to_string()
}

pub fn envelope_rev(id: u128, revision: u64, command: serde_json::Value) -> String {
    serde_json::json!({
        "id": format!("00000000-0000-0000-0000-{id:012x}"),
        "expected_revision": revision,
        "command": command,
    })
    .to_string()
}

pub fn post_command(
    server: &Server,
    run: Option<&str>,
    id: u128,
    command: serde_json::Value,
) -> (u16, serde_json::Value) {
    post_command_as(&server.addr, &server.token(), run, id, command)
}

pub fn post_command_as(
    addr: &str,
    token: &str,
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
        addr,
        &path,
        Some(token),
        Some(&envelope(id, command)),
        &[],
    );
    let value: serde_json::Value = serde_json::from_str(&body).unwrap_or(serde_json::Value::Null);
    (status, value)
}

pub fn create_run(server: &Server, id: u128) -> String {
    let (status, receipt) = post_command(
        server,
        None,
        id,
        serde_json::json!({"CreateRun": {"goal": "Build the durable runtime", "repository": "D:/Code/enolalab/central"}}),
    );
    assert_eq!(status, 200, "create run: {receipt}");
    receipt["run_id"].as_str().expect("run id").to_string()
}

pub fn get_snapshot(server: &Server, run: &str) -> serde_json::Value {
    get_snapshot_as(&server.addr, &server.token(), run)
}

pub fn get_snapshot_as(addr: &str, token: &str, run: &str) -> serde_json::Value {
    let (status, body) = http(
        "GET",
        addr,
        &format!("/v1/runs/{run}"),
        Some(token),
        None,
        &[],
    );
    assert_eq!(status, 200, "get run: {body}");
    serde_json::from_str(&body).expect("run json")
}

/// Propose the fixture plan and approve it, returning the plan hash.
/// Uses the plan endpoints when they answer, else the command gateway.
pub fn approve_fixture_plan(server: &Server, run: &str, base_id: u128) -> String {
    let plan = plan_json();
    let plan_value: serde_json::Value = serde_json::from_str(&plan).expect("plan value");
    let (status, receipt) = post_command(
        server,
        Some(run),
        base_id,
        serde_json::json!({"ProposePlan": {"plan": plan_value}}),
    );
    assert_eq!(status, 200, "propose: {receipt}");
    let snapshot = get_snapshot(server, run);
    let hash = snapshot["state"]["pending_plan"]
        .as_str()
        .expect("pending plan hash")
        .to_string();
    let revision = snapshot["state"]["revision"].as_u64().expect("revision");
    let path = format!("/v1/commands?run={run}");
    let (status, body) = http(
        "POST",
        &server.addr,
        &path,
        Some(&server.token()),
        Some(&envelope_rev(
            base_id + 1,
            revision,
            serde_json::json!({"ApprovePlan": {"plan_hash": hash}}),
        )),
        &[],
    );
    let receipt: serde_json::Value = serde_json::from_str(&body).unwrap_or(serde_json::Value::Null);
    assert_eq!(status, 200, "approve: {receipt}");
    hash
}

pub fn issue_client(server: &Server, name: &str, scope: &str) -> serde_json::Value {
    let (status, body) = http(
        "POST",
        &server.addr,
        "/v1/clients",
        Some(&server.token()),
        Some(&serde_json::json!({"name": name, "scope": scope}).to_string()),
        &[],
    );
    assert_eq!(status, 200, "issue client {name}/{scope}: {body}");
    serde_json::from_str(&body).expect("client json")
}

pub fn client_token(issued: &serde_json::Value) -> String {
    let id = issued["client_id"].as_str().expect("client_id");
    let secret = issued["secret"].as_str().expect("secret");
    format!("{id}.{secret}")
}
