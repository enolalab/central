//! P2 Task 2: process ownership (TDD gate).
//!
//! Owned agent processes live in a Windows Job Object with KILL_ON_JOB_CLOSE
//! so nothing outlives Central; the daemon itself is spawned detached and
//! never lives in a job tied to CLI lifetime. Termination is identity-checked
//! (PID + creation time + ownership): a stale record for a recycled PID must
//! kill nothing. The installed-CLI stdio smoke at the bottom is opt-in and
//! read-only (no inference); without a reachable CLI + auth it stays
//! unverified, never faked.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use central_application::contracts::{
    AppError, ExecutionRef, OpenSession, ProcessIdentity, ProcessSpec, ProcessState,
};
use central_application::ports::{AgentGateway, ProcessSupervisor};
use central_domain::{AttemptId, ContentHash, ProfileId, ResolvedProfile, Timestamp};
use central_infrastructure::codex::{CodexConfig, CodexGateway};
#[cfg(windows)]
use central_infrastructure::process::{
    current_process_in_job, process_creation_time_ms, process_in_job,
};
use central_infrastructure::process::{
    spawn_detached, DetachPlacement, JobError, JobObject, JobPlacement, OwnedProcessSupervisor,
};

/// Helper sleeper: Windows and portable variants so supervision itself is
/// exercised on every platform; only job-object containment is Windows-only.
#[cfg(windows)]
fn sleeper_spec() -> ProcessSpec {
    ProcessSpec {
        program: "powershell.exe".to_string(),
        args: vec![
            "-NoProfile".to_string(),
            "-NonInteractive".to_string(),
            "-Command".to_string(),
            "Start-Sleep -Seconds 120".to_string(),
        ],
        working_dir: None,
    }
}

#[cfg(not(windows))]
fn sleeper_spec() -> ProcessSpec {
    ProcessSpec {
        program: "sh".to_string(),
        args: vec!["-c".to_string(), "sleep 120".to_string()],
        working_dir: None,
    }
}

/// App-server stand-in for the gateway open/cancel test, per platform.
#[cfg(windows)]
fn helper_command() -> (String, Vec<String>) {
    (
        "powershell.exe".to_string(),
        vec![
            "-NoProfile".to_string(),
            "-NonInteractive".to_string(),
            "-Command".to_string(),
            "Start-Sleep -Seconds 60".to_string(),
        ],
    )
}

#[cfg(not(windows))]
fn helper_command() -> (String, Vec<String>) {
    (
        "sh".to_string(),
        vec!["-c".to_string(), "sleep 60".to_string()],
    )
}

/// Deterministic nested-job-style refusal for the assign hook.
#[cfg(windows)]
fn fail_assign(_job: &JobObject, _pid: u32) -> Result<(), JobError> {
    Err(JobError::Win32(5))
}

/// Deterministic kernel-clock failure for the creation-time hook.
fn fail_creation_time(pid: u32) -> Result<i64, JobError> {
    Err(JobError::NotFound(format!(
        "test hook: cannot read creation time of pid {pid}"
    )))
}

fn personal_profile() -> ResolvedProfile {
    ResolvedProfile {
        id: ProfileId::from_u128(0xA1),
        provider: "codex".to_string(),
        model: "gpt-6-luna".to_string(),
        effort: "low".to_string(),
        skills: vec![],
        subagents: vec![],
        permissions: vec![],
        fallbacks: vec![],
        hash: ContentHash::from_hex("0".repeat(64)),
    }
}

#[tokio::test]
async fn owned_spawn_inspect_terminate() {
    let supervisor = OwnedProcessSupervisor::new();
    let identity = supervisor
        .spawn(&sleeper_spec())
        .await
        .expect("spawn helper");
    assert_eq!(
        supervisor.inspect(&identity).await.expect("inspect"),
        ProcessState::Running
    );
    let report = supervisor
        .terminate_detailed(&identity)
        .await
        .expect("terminate");
    assert_eq!(report.kill_count, 1);
    assert!(report.killed);
    match supervisor
        .inspect(&identity)
        .await
        .expect("inspect after kill")
    {
        ProcessState::Exited(_) => {}
        other => panic!("expected exited, got {other:?}"),
    }
}

#[tokio::test]
async fn never_kill_reused_pid() {
    let supervisor = OwnedProcessSupervisor::new();
    let identity = supervisor
        .spawn(&sleeper_spec())
        .await
        .expect("spawn helper");

    // A stale record for the same PID but a different creation time must
    // kill nothing: the PID may have been recycled.
    let wrong_creation_time = ProcessIdentity {
        pid: identity.pid,
        created_at: Timestamp::from_millis(1),
        ownership_id: identity.ownership_id.clone(),
    };
    // Same ownership, wrong creation time: this is only possible if the
    // test fabricates it, which is exactly the recycled-PID shape.
    if wrong_creation_time.created_at == identity.created_at {
        panic!("test needs a creation time that differs from the real one");
    }
    let refused = supervisor
        .terminate_detailed(&wrong_creation_time)
        .await
        .expect("refusal is not an error");
    assert_eq!(refused.kill_count, 0);
    assert!(!refused.killed);

    // The victim is still alive.
    assert_eq!(
        supervisor.inspect(&identity).await.expect("victim inspect"),
        ProcessState::Running
    );

    // The true identity kills exactly once; a repeat kills nothing.
    let real = supervisor
        .terminate_detailed(&identity)
        .await
        .expect("real terminate");
    assert_eq!(real.kill_count, 1);
    let again = supervisor
        .terminate_detailed(&identity)
        .await
        .expect("repeat terminate");
    assert_eq!(again.kill_count, 0);
    assert!(!again.killed);
}

#[tokio::test]
async fn unknown_identity_kills_nothing() {
    let supervisor = OwnedProcessSupervisor::new();
    let ghost = ProcessIdentity {
        pid: 1,
        created_at: Timestamp::from_millis(1),
        ownership_id: "no-such-ownership".to_string(),
    };
    match supervisor.terminate_detailed(&ghost).await {
        Err(AppError::NotFound(_)) => {}
        other => panic!("expected NotFound, got {other:?}"),
    }
    match supervisor.inspect(&ghost).await {
        Err(AppError::NotFound(_)) => {}
        other => panic!("expected NotFound, got {other:?}"),
    }
}

#[tokio::test]
async fn missing_binary_fails_without_side_effects() {
    let supervisor = OwnedProcessSupervisor::new();
    let spec = ProcessSpec {
        program: "central-definitely-missing-binary-xyz.exe".to_string(),
        args: vec![],
        working_dir: None,
    };
    match supervisor.spawn(&spec).await {
        Err(AppError::Transport(_)) => {}
        other => panic!("expected Transport, got {other:?}"),
    }
}

#[test]
fn job_placement_is_published_never_silent() {
    let supervisor = OwnedProcessSupervisor::new();
    match supervisor.placement() {
        // Enforced is only reported with query-backed proof inside the job
        // object; otherwise the supervisor reports InstructionOnly.
        JobPlacement::Enforced => {}
        JobPlacement::InstructionOnly(reason) => assert!(!reason.is_empty()),
    }
}

/// A refused job assignment must be published per process instead of
/// swallowed: the supervisor-level `placement()` probe (taken once in
/// `new()`) can still say `Enforced` while THIS process was never assigned.
/// Windows-only: the deterministic refusal needs the real kernel clock plus
/// a real job object, and the assertion pins `Running` via the OS check.
#[cfg(windows)]
#[tokio::test]
async fn assign_failure_is_published_per_process() {
    let supervisor = OwnedProcessSupervisor::with_hooks(
        JobObject::new_kill_on_close,
        fail_assign,
        process_creation_time_ms,
    );
    let identity = supervisor
        .spawn(&sleeper_spec())
        .await
        .expect("refused assignment still supervises by handle");
    match supervisor
        .placement_of(&identity)
        .expect("per-process placement")
    {
        JobPlacement::InstructionOnly(reason) => {
            assert!(!reason.is_empty(), "reason published, never silent")
        }
        JobPlacement::Enforced => {
            panic!("a failing assign hook must never report Enforced")
        }
    }
    // The process stays supervised: liveness and exactly-once kill work
    // through the owned handle.
    assert_eq!(
        supervisor.inspect(&identity).await.expect("inspect"),
        ProcessState::Running
    );
    let report = supervisor
        .terminate_detailed(&identity)
        .await
        .expect("terminate");
    assert_eq!(report.kill_count, 1);
}

/// Portable twin: off Windows every job creation fails, so the default
/// supervisor must publish that per process too — supervised, but honestly
/// contained by handle only.
#[cfg(not(windows))]
#[tokio::test]
async fn unenforceable_containment_is_published_per_process() {
    let supervisor = OwnedProcessSupervisor::new();
    let identity = supervisor.spawn(&sleeper_spec()).await.expect("spawn");
    match supervisor
        .placement_of(&identity)
        .expect("per-process placement")
    {
        JobPlacement::InstructionOnly(reason) => {
            assert!(!reason.is_empty(), "reason published, never silent")
        }
        JobPlacement::Enforced => {
            panic!("no job objects off Windows; must never report Enforced")
        }
    }
    assert_eq!(
        supervisor.inspect(&identity).await.expect("inspect"),
        ProcessState::Running
    );
    let report = supervisor
        .terminate_detailed(&identity)
        .await
        .expect("terminate");
    assert_eq!(report.kill_count, 1);
}

/// A failed kernel creation-time query must fail the spawn loudly — never
/// substitute the wall clock (that stamp would later mismatch the terminate
/// check, leaking an unterminable entry). Portable: the hook fails on every
/// platform and the failure path kills through the owned handle, so no
/// identity is recorded and no orphan is left behind.
#[tokio::test]
async fn creation_time_failure_fails_spawn_loudly() {
    let supervisor = OwnedProcessSupervisor::with_hooks(
        JobObject::new_kill_on_close,
        JobObject::assign,
        fail_creation_time,
    );
    let before = supervisor.supervised_count().expect("count");
    match supervisor.spawn(&sleeper_spec()).await {
        Err(AppError::Transport(message)) => {
            assert!(
                message.contains("creation time"),
                "names the cause: {message}"
            );
        }
        other => panic!("expected a loud Transport failure, got {other:?}"),
    }
    assert_eq!(
        supervisor.supervised_count().expect("count"),
        before,
        "a failed spawn records no identity"
    );
}

#[cfg(windows)]
#[tokio::test]
async fn daemon_is_not_in_cli_tied_job() {
    let dir = std::env::temp_dir().join(format!("central-t2-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).expect("scratch dir");
    let log = dir.join("daemon.log");
    // NOTE: the sleeper child is cmd.exe + ping, not powershell: in this
    // sandbox a detached powershell never executes (no output, empty stdio),
    // while a detached cmd runs fine — an environment/binary quirk, not a
    // Central defect. The spawn path under test (DETACHED + breakaway
    // fallback) is identical either way, and quoting a scratch path through
    // cmd is fragile, so execution is proven by liveness instead of a file.
    let mut detached = spawn_detached(
        Path::new("cmd.exe"),
        &["/C".to_string(), "ping -n 30 127.0.0.1".to_string()],
        &log,
    )
    .expect("detached spawn succeeds (escaping or honestly inheriting)");
    std::thread::sleep(Duration::from_secs(2));
    // Still alive after a delay: the child genuinely executes (a stillborn
    // child would already be reaped as exited here).
    assert!(
        detached.child.try_wait().expect("reap check").is_none(),
        "detached child is running, not stillborn"
    );
    match &detached.placement {
        // The goal: no job ties the daemon to the CLI lifetime, so closing
        // the CLI can never take it down.
        DetachPlacement::EscapedJob => assert!(
            !process_in_job(detached.child.id()).expect("query child job membership"),
            "escaped daemon child must not be in a job"
        ),
        // Honest fallback: only acceptable with a published reason while the
        // parent itself is locked in a job that forbids breakaway.
        DetachPlacement::JoinedParentJob(reason) => {
            assert!(!reason.is_empty(), "inherited placement must say why");
            assert!(
                current_process_in_job().expect("query own job membership"),
                "inherited placement is only honest inside a restrictive parent job"
            );
        }
    }
    detached.child.kill().ok();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Portable twin: off Windows there are no job objects, so a detached spawn
/// trivially escapes CLI-tied lifetime — the test pins that the spawn,
/// liveness check, and placement report work there too.
#[cfg(not(windows))]
#[tokio::test]
async fn daemon_spawns_detached_portable() {
    let dir = std::env::temp_dir().join(format!("central-t2-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).expect("scratch dir");
    let log = dir.join("daemon.log");
    let mut detached = spawn_detached(
        Path::new("sh"),
        &["-c".to_string(), "sleep 30".to_string()],
        &log,
    )
    .expect("detached spawn succeeds");
    std::thread::sleep(Duration::from_secs(2));
    assert!(
        detached.child.try_wait().expect("reap check").is_none(),
        "detached child is running, not stillborn"
    );
    assert_eq!(
        detached.placement,
        DetachPlacement::EscapedJob,
        "no job objects off Windows: always escaped"
    );
    detached.child.kill().ok();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn gateway_open_spawns_owned_process_and_cancel_kills_it() {
    let supervisor: Arc<dyn ProcessSupervisor> = Arc::new(OwnedProcessSupervisor::new());
    let (binary, app_server_args) = helper_command();
    let config = CodexConfig {
        binary,
        app_server_args,
        ..CodexConfig::default()
    };
    let gateway = CodexGateway::new(Arc::clone(&supervisor), config);
    let attempt_id = AttemptId::from_u128(0xC10);
    let session = gateway
        .open(OpenSession {
            attempt_id,
            profile: personal_profile(),
            workspace: "fixture-scratch".to_string(),
            resume_from: None,
        })
        .await
        .expect("open spawns the owned helper process");
    let observation = gateway
        .cancel(&ExecutionRef {
            attempt_id,
            session,
            turn: None,
        })
        .await
        .expect("cancel");
    assert!(observation.cancelled, "cancel kills the owned process");
}

/// Opt-in live smoke: installed `codex` CLI stdio `initialize` handshake
/// WITHOUT inference (no turn is ever started). Read-only: it never touches
/// personal auth/config and never sends a prompt.
///
/// Run with: `CENTRAL_CODEX_SMOKE=1 cargo test -p central-infrastructure
/// --test process_ownership installed_cli_stdio_smoke -- --ignored --nocapture`
/// Without a reachable CLI + auth this stays unverified, never faked.
#[tokio::test]
#[ignore = "opt-in live CLI smoke; needs installed codex CLI plus auth"]
async fn installed_cli_stdio_smoke() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::time::timeout;

    if std::env::var("CENTRAL_CODEX_SMOKE").as_deref() != Ok("1") {
        panic!("set CENTRAL_CODEX_SMOKE=1 to opt in (needs installed codex CLI plus auth)");
    }
    let binary = std::env::var("CENTRAL_CODEX_BINARY").unwrap_or_else(|_| "codex".to_string());
    let version_out = tokio::process::Command::new(&binary)
        .arg("--version")
        .output()
        .await
        .expect("codex binary must run for the smoke");
    assert!(
        version_out.status.success(),
        "codex --version must succeed read-only"
    );
    let version = String::from_utf8_lossy(&version_out.stdout).into_owned();
    println!("codex version: {}", version.trim());

    let mut child = tokio::process::Command::new(&binary)
        .arg("app-server")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn codex app-server");
    let mut stdin = child.stdin.take().expect("piped stdin");
    let stdout = child.stdout.take().expect("piped stdout");
    let mut lines = BufReader::new(stdout).lines();

    let init = serde_json::json!({
        "id": 1,
        "method": "initialize",
        "params": {
            "clientInfo": {"name": "central-smoke", "title": "Central smoke", "version": "0.1.0"},
            "capabilities": {"experimentalApi": true}
        }
    });
    stdin
        .write_all(format!("{init}\n").as_bytes())
        .await
        .expect("write initialize");
    let response = timeout(Duration::from_secs(30), lines.next_line())
        .await
        .expect("initialize response within budget")
        .expect("read initialize response")
        .expect("initialize response line present");
    assert!(
        response.contains("\"result\""),
        "initialize must return a result, got: {response}"
    );
    println!("initialize ok (no inference performed)");
    child.kill().await.ok();
}
