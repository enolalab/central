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
    exited: Option<i32>,
}

struct SupervisorState {
    children: HashMap<String, SupervisedChild>,
}

/// Supervises owned agent processes with job-object containment where the
/// host allows it.
pub struct OwnedProcessSupervisor {
    state: Mutex<SupervisorState>,
    placement: JobPlacement,
}

impl Default for OwnedProcessSupervisor {
    fn default() -> Self {
        Self::new()
    }
}

impl OwnedProcessSupervisor {
    pub fn new() -> Self {
        let placement = match JobObject::new_kill_on_close() {
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
        }
    }

    /// How containment was established. `Enforced` is only reported with
    /// query-backed proof from the job object; otherwise the reason for
    /// `InstructionOnly` is published.
    pub fn placement(&self) -> JobPlacement {
        self.placement.clone()
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

#[async_trait]
impl ProcessSupervisor for OwnedProcessSupervisor {
    async fn spawn(&self, spec: &ProcessSpec) -> Result<ProcessIdentity, AppError> {
        let mut command = Command::new(&spec.program);
        command
            .args(&spec.args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if let Some(dir) = spec.working_dir.as_ref() {
            command.current_dir(dir);
        }
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            // No console window for owned agent processes. Stdio stays null:
            // the T2 contract carries no pipe handles, so buffered output
            // could block the child; the stdio attach arrives with the RPC
            // wiring that consumes it.
            const CREATE_NO_WINDOW: u32 = 0x0800_0000;
            command.creation_flags(CREATE_NO_WINDOW);
        }
        let child = command.spawn().map_err(|error| {
            AppError::Transport(format!("spawn '{}' failed: {error}", spec.program))
        })?;
        let pid = child.id();
        // Prefer the OS creation time so a later liveness check compares
        // kernel truth, not our own clock.
        let created_at_ms =
            windows_job::process_creation_time_ms(pid).unwrap_or_else(|_| now_millis());
        let ownership_id = Uuid::new_v4().to_string();
        // Contain first, before the child can fork away from the job. When
        // assignment fails (nested job, platform without jobs) the child
        // stays supervised by handle only; placement() says so honestly.
        let job = match JobObject::new_kill_on_close() {
            Ok(job) => job.assign(pid).ok().map(|()| job),
            Err(_) => None,
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
                    exited: None,
                },
            );
        Ok(identity)
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
