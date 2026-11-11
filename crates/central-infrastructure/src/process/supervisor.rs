//! Owned-process supervisor (P2 Task 2): the [`ProcessSupervisor`]
//! implementation for Windows.
//!
//! Every spawned agent process is assigned to a Job Object with
//! KILL_ON_JOB_CLOSE (query-backed at creation), so the whole tree dies with
//! Central. Termination is identity-checked: the presented [`ProcessIdentity`]
//! must match the supervised record's PID *and* creation time *and*
//! ownership id, otherwise nothing is killed and `kill_count` stays 0. A
//! recycled PID therefore can never be hit through a stale record. The actual
//! kill goes through the owned [`Child`](std::process::Child) handle, which
//! refers to the process object rather than the numeric PID.
//!
//! When containment is unenforceable (no Windows host, or a nested job the
//! parent forbids), [`OwnedProcessSupervisor::placement`] reports
//! [`JobPlacement::InstructionOnly`] with the reason instead of pretending.

use std::collections::HashMap;
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use uuid::Uuid;

use central_application::contracts::{AppError, ProcessIdentity, ProcessSpec, ProcessState};
use central_application::ports::ProcessSupervisor;
use central_domain::Timestamp;

use super::windows_job::{self, JobObject};

pub use super::piped::{PipedSpawner, PipedStdio};
pub use super::windows_job::{current_process_in_job, JobError, JobPlacement};

/// Outcome of an identity-checked termination. `kill_count` is 1 only when
/// this call actually ended the supervised process; refusals (stale identity,
/// already exited) and unknown ownerships kill nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TerminateReport {
    pub killed: bool,
    pub kill_count: usize,
    pub detail: String,
}

struct SupervisedChild {
    pid: u32,
    created_at_ms: i64,
    child: Child,
    job: Option<JobObject>,
    /// How THIS process was contained. The supervisor-level [`placement`]
    /// probe is taken once in [`new`]; a later per-spawn assignment can still
    /// fail (nested job), so each record carries its own outcome instead of
    /// inheriting the probe.
    ///
    /// [`placement`]: OwnedProcessSupervisor::placement
    /// [`new`]: OwnedProcessSupervisor::new
    placement: JobPlacement,
    exited: Option<i32>,
}

struct SupervisorState {
    children: HashMap<String, SupervisedChild>,
}

/// Builds one kill-on-close job object.
type CreateJobFn = fn() -> Result<JobObject, JobError>;
/// Assigns a live PID to a job object.
type AssignFn = fn(&JobObject, u32) -> Result<(), JobError>;
/// Reads the kernel creation time of a live PID.
type CreationTimeFn = fn(u32) -> Result<i64, JobError>;

/// Supervises owned agent processes with job-object containment where the
/// host allows it.
pub struct OwnedProcessSupervisor {
    state: Mutex<SupervisorState>,
    placement: JobPlacement,
    create_job: CreateJobFn,
    assign: AssignFn,
    creation_time: CreationTimeFn,
}

impl Default for OwnedProcessSupervisor {
    fn default() -> Self {
        Self::new()
    }
}

impl OwnedProcessSupervisor {
    pub fn new() -> Self {
        Self::with_hooks(
            JobObject::new_kill_on_close,
            JobObject::assign,
            default_creation_time,
        )
    }

    /// Build a supervisor with injectable process hooks. Production code
    /// always uses [`new`]; this seam exists so tests can deterministically
    /// exercise the refused-assignment and failed-clock paths instead of
    /// depending on the host's job nesting.
    ///
    /// [`new`]: OwnedProcessSupervisor::new
    pub fn with_hooks(
        create_job: CreateJobFn,
        assign: AssignFn,
        creation_time: CreationTimeFn,
    ) -> Self {
        let placement = match create_job() {
            Ok(_) => JobPlacement::Enforced,
            Err(error) => JobPlacement::InstructionOnly(format!(
                "kill-on-close job unavailable ({error}); processes are supervised by \
                 handle only and trees may outlive the supervisor"
            )),
        };
        Self {
            state: Mutex::new(SupervisorState {
                children: HashMap::new(),
            }),
            placement,
            create_job,
            assign,
            creation_time,
        }
    }

    /// How containment was established. `Enforced` is only reported with
    /// query-backed proof from the job object; otherwise the reason for
    /// `InstructionOnly` is published.
    ///
    /// This is the construction-time probe. For the outcome of a single
    /// spawn, see [`placement_of`], which is the per-process truth.
    ///
    /// [`placement_of`]: OwnedProcessSupervisor::placement_of
    pub fn placement(&self) -> JobPlacement {
        self.placement.clone()
    }

    /// Per-process containment truth: how the process behind `identity` was
    /// actually contained. A refused assignment is published here as
    /// `InstructionOnly` with the reason instead of being swallowed, even
    /// when [`placement`] still reports `Enforced` from its earlier probe.
    ///
    /// [`placement`]: OwnedProcessSupervisor::placement
    pub fn placement_of(&self, identity: &ProcessIdentity) -> Result<JobPlacement, AppError> {
        let state = self
            .state
            .lock()
            .map_err(|_| AppError::Storage("supervisor lock poisoned".to_string()))?;
        let entry = state.children.get(&identity.ownership_id).ok_or_else(|| {
            AppError::NotFound(format!(
                "unknown supervised process '{}'",
                identity.ownership_id
            ))
        })?;
        if entry.pid != identity.pid || entry.created_at_ms != identity.created_at.0 {
            return Err(AppError::NotFound(format!(
                "identity does not match the supervised record for '{}'; refusing \
                 (possible recycled PID)",
                identity.ownership_id
            )));
        }
        Ok(entry.placement.clone())
    }

    /// How many supervised identities are currently recorded. A spawn that
    /// fails loudly records nothing, so the count is unchanged by it.
    pub fn supervised_count(&self) -> Result<usize, AppError> {
        Ok(self
            .state
            .lock()
            .map_err(|_| AppError::Storage("supervisor lock poisoned".to_string()))?
            .children
            .len())
    }

    /// Shared spawn core: `piped` selects null stdio (fire-and-forget
    /// helpers) or owned stdin/stdout pipes (RPC attach). Everything else —
    /// kernel creation time, job containment, the supervised record — is
    /// identical, so a piped child is killed exactly like a null-stdio one.
    async fn spawn_with_stdio(
        &self,
        spec: &ProcessSpec,
        piped: bool,
    ) -> Result<
        (
            ProcessIdentity,
            Option<(std::process::ChildStdin, std::process::ChildStdout)>,
        ),
        AppError,
    > {
        let mut command = Command::new(&spec.program);
        command.args(&spec.args);
        if piped {
            // Owned pipes for RPC: the gateway consumes both ends, so a
            // full buffer can never park the child. Stderr stays null.
            command
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::null());
        } else {
            command
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
        }
        if let Some(dir) = spec.working_dir.as_ref() {
            command.current_dir(dir);
        }
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            // No console window for owned agent processes.
            const CREATE_NO_WINDOW: u32 = 0x0800_0000;
            command.creation_flags(CREATE_NO_WINDOW);
        }
        let mut child = command.spawn().map_err(|error| {
            AppError::Transport(format!("spawn '{}' failed: {error}", spec.program))
        })?;
        let pipes = if piped {
            let stdin = child.stdin.take().ok_or_else(|| {
                AppError::Transport(format!(
                    "spawn '{}' did not yield a piped stdin",
                    spec.program
                ))
            })?;
            let stdout = child.stdout.take().ok_or_else(|| {
                AppError::Transport(format!(
                    "spawn '{}' did not yield a piped stdout",
                    spec.program
                ))
            })?;
            Some((stdin, stdout))
        } else {
            None
        };
        let pid = child.id();
        // The kernel creation time, never the wall clock on platforms where
        // the stamp is load-bearing: a later liveness check compares it
        // against live kernel truth, and a substituted clock would mismatch
        // there — failing closed on every later terminate and leaking an
        // unterminable record. Retry once for a transient query failure; a
        // second failure fails the spawn loudly and reaps the child through
        // its own handle, so no unidentifiable process is ever supervised or
        // orphaned, and nothing is recorded.
        let created_at_ms = match (self.creation_time)(pid) {
            Ok(stamp) => stamp,
            Err(first) => match (self.creation_time)(pid) {
                Ok(stamp) => stamp,
                Err(second) => {
                    child.kill().ok();
                    for _ in 0..100 {
                        match child.try_wait() {
                            Ok(None) => std::thread::sleep(Duration::from_millis(10)),
                            _ => break,
                        }
                    }
                    return Err(AppError::Transport(format!(
                        "cannot read kernel creation time of spawned pid {pid} \
                         ({first}; retry: {second}); refusing to supervise an \
                         unidentifiable process"
                    )));
                }
            },
        };
        let ownership_id = Uuid::new_v4().to_string();
        // Contain first, before the child can fork away from the job. When
        // assignment fails (nested job, platform without jobs) the child
        // stays supervised by handle only, and the per-process outcome is
        // stored on the record — never swallowed — so `placement_of` reports
        // it honestly even when the construction-time `placement()` probe
        // still says `Enforced`.
        let (job, per_process) = match (self.create_job)() {
            Ok(job) => match (self.assign)(&job, pid) {
                Ok(()) => (Some(job), JobPlacement::Enforced),
                Err(error) => (
                    None,
                    JobPlacement::InstructionOnly(format!(
                        "job assignment failed for pid {pid} ({error}); process is \
                         supervised by handle only and its tree may outlive the supervisor"
                    )),
                ),
            },
            Err(error) => (
                None,
                JobPlacement::InstructionOnly(format!(
                    "kill-on-close job unavailable ({error}); process is supervised by \
                     handle only and its tree may outlive the supervisor"
                )),
            ),
        };
        let identity = ProcessIdentity {
            pid,
            created_at: Timestamp::from_millis(created_at_ms),
            ownership_id: ownership_id.clone(),
        };
        self.state
            .lock()
            .map_err(|_| AppError::Storage("supervisor lock poisoned".to_string()))?
            .children
            .insert(
                ownership_id,
                SupervisedChild {
                    pid,
                    created_at_ms,
                    child,
                    job,
                    placement: per_process,
                    exited: None,
                },
            );
        Ok((identity, pipes))
    }

    /// Identity-checked termination with an observable kill count.
    pub async fn terminate_detailed(
        &self,
        identity: &ProcessIdentity,
    ) -> Result<TerminateReport, AppError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| AppError::Storage("supervisor lock poisoned".to_string()))?;
        let entry = state
            .children
            .get_mut(&identity.ownership_id)
            .ok_or_else(|| {
                AppError::NotFound(format!(
                    "unknown supervised process '{}'",
                    identity.ownership_id
                ))
            })?;
        if entry.pid != identity.pid || entry.created_at_ms != identity.created_at.0 {
            return Ok(TerminateReport {
                killed: false,
                kill_count: 0,
                detail: format!(
                    "identity does not match the supervised record for '{}'; refusing \
                     (possible recycled PID)",
                    identity.ownership_id
                ),
            });
        }
        if let Some(code) = entry.exited {
            return Ok(TerminateReport {
                killed: false,
                kill_count: 0,
                detail: format!("owned process {} already exited ({code})", entry.pid),
            });
        }
        // Liveness through our own handle: try_wait reaps without a PID
        // lookup, so it cannot observe a recycled PID.
        if let Some(status) = entry.child.try_wait().map_err(|error| {
            AppError::Transport(format!("wait on owned process failed: {error}"))
        })? {
            let code = status.code().unwrap_or(-1);
            entry.exited = Some(code);
            entry.job = None;
            return Ok(TerminateReport {
                killed: false,
                kill_count: 0,
                detail: format!("owned process {} already exited ({code})", entry.pid),
            });
        }
        // Belt and braces on Windows: the live OS creation time must still
        // match the supervised record.
        #[cfg(windows)]
        match windows_job::process_creation_time_ms(entry.pid) {
            Ok(live) if live != entry.created_at_ms => {
                return Ok(TerminateReport {
                    killed: false,
                    kill_count: 0,
                    detail: format!(
                        "live creation time of pid {} differs from the supervised record; \
                         refusing (possible recycled PID)",
                        entry.pid
                    ),
                });
            }
            Ok(_) => {}
            Err(error) => {
                return Ok(TerminateReport {
                    killed: false,
                    kill_count: 0,
                    detail: format!(
                        "cannot verify live pid {} ({error}); failing closed, killing nothing",
                        entry.pid
                    ),
                });
            }
        }
        // The kill goes through the owned handle (process object), never a
        // bare PID, so a recycled PID cannot be hit here either.
        entry
            .child
            .kill()
            .map_err(|error| AppError::Transport(format!("kill owned process failed: {error}")))?;
        let mut code = -1;
        for _ in 0..100 {
            match entry.child.try_wait().map_err(|error| {
                AppError::Transport(format!("reap owned process failed: {error}"))
            })? {
                Some(status) => {
                    code = status.code().unwrap_or(-1);
                    break;
                }
                None => std::thread::sleep(Duration::from_millis(10)),
            }
        }
        entry.exited = Some(code);
        // Dropping the last job handle fires KILL_ON_JOB_CLOSE for the rest
        // of the tree.
        entry.job = None;
        Ok(TerminateReport {
            killed: true,
            kill_count: 1,
            detail: format!("terminated owned process {}", entry.pid),
        })
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}

/// Default kernel-clock query. On Windows this is the real process creation
/// time, and it is load-bearing there: terminate/inspect cross-check it
/// against the live OS value. Off Windows there is no kernel clock and those
/// cross-checks are compiled out, so the wall clock is only an opaque
/// correlator — and job containment there is always `InstructionOnly`.
fn default_creation_time(pid: u32) -> Result<i64, JobError> {
    if cfg!(windows) {
        windows_job::process_creation_time_ms(pid)
    } else {
        Ok(now_millis())
    }
}

#[async_trait]
impl ProcessSupervisor for OwnedProcessSupervisor {
    async fn spawn(&self, spec: &ProcessSpec) -> Result<ProcessIdentity, AppError> {
        // Null stdio: fire-and-forget helpers nobody reads from.
        Ok(self.spawn_with_stdio(spec, false).await?.0)
    }

    async fn inspect(&self, identity: &ProcessIdentity) -> Result<ProcessState, AppError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| AppError::Storage("supervisor lock poisoned".to_string()))?;
        let entry = state
            .children
            .get_mut(&identity.ownership_id)
            .ok_or_else(|| {
                AppError::NotFound(format!(
                    "unknown supervised process '{}'",
                    identity.ownership_id
                ))
            })?;
        if entry.pid != identity.pid || entry.created_at_ms != identity.created_at.0 {
            // A recycled PID must never be reported as the supervised child.
            return Ok(ProcessState::Unknown);
        }
        if let Some(code) = entry.exited {
            return Ok(ProcessState::Exited(code));
        }
        match entry.child.try_wait().map_err(|error| {
            AppError::Transport(format!("wait on owned process failed: {error}"))
        })? {
            Some(status) => {
                let code = status.code().unwrap_or(-1);
                entry.exited = Some(code);
                entry.job = None;
                Ok(ProcessState::Exited(code))
            }
            None => {
                #[cfg(windows)]
                match windows_job::process_creation_time_ms(entry.pid) {
                    Ok(live) if live != entry.created_at_ms => Ok(ProcessState::Unknown),
                    Ok(_) => Ok(ProcessState::Running),
                    Err(_) => Ok(ProcessState::Unknown),
                }
                #[cfg(not(windows))]
                Ok(ProcessState::Running)
            }
        }
    }

    async fn terminate(&self, identity: &ProcessIdentity) -> Result<(), AppError> {
        // Refusals and already-exited are honest non-errors: nothing was
        // killed, and that is the safe outcome.
        self.terminate_detailed(identity).await.map(|_| ())
    }
}

#[async_trait]
impl PipedSpawner for OwnedProcessSupervisor {
    async fn spawn_piped(
        &self,
        spec: &ProcessSpec,
    ) -> Result<(ProcessIdentity, PipedStdio), AppError> {
        let (identity, pipes) = self.spawn_with_stdio(spec, true).await?;
        let (stdin, stdout) = pipes.expect("piped spawn always yields pipes");
        let stdin = match tokio::process::ChildStdin::from_std(stdin) {
            Ok(pipe) => pipe,
            Err(error) => {
                // The child is already supervised; never orphan it on a
                // conversion failure — terminate through the owned handle.
                let _ = self.terminate_detailed(&identity).await;
                return Err(AppError::Transport(format!(
                    "cannot adopt piped stdin: {error}"
                )));
            }
        };
        let stdout = match tokio::process::ChildStdout::from_std(stdout) {
            Ok(pipe) => pipe,
            Err(error) => {
                let _ = self.terminate_detailed(&identity).await;
                return Err(AppError::Transport(format!(
                    "cannot adopt piped stdout: {error}"
                )));
            }
        };
        Ok((
            identity,
            PipedStdio {
                stdin: Box::new(stdin),
                stdout: Box::new(stdout),
            },
        ))
    }
}
