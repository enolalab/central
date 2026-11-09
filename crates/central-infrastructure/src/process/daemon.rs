//! Daemon ownership and detached spawning (Windows adapter).
//!
//! A single daemon owns the storage in a state directory. Ownership is
//! proven by an OS file lock on `daemon.lock` inside that directory,
//! acquired before the database is opened; a second daemon fails with
//! [`DaemonError::AlreadyRunning`] and the stable exit code
//! [`ALREADY_RUNNING_EXIT_CODE`] instead of touching the database.

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use fs2::FileExt;

/// Process exit code used when a second daemon (or `daemon start`)
/// finds the state directory already owned.
pub const ALREADY_RUNNING_EXIT_CODE: i32 = 3;

#[derive(Debug)]
pub enum DaemonError {
    AlreadyRunning,
    Io(std::io::Error),
}

impl std::fmt::Display for DaemonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyRunning => {
                write!(f, "a daemon already owns this state directory")
            }
            Self::Io(error) => write!(f, "daemon io error: {error}"),
        }
    }
}

impl std::error::Error for DaemonError {}

impl From<std::io::Error> for DaemonError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// Held for the daemon's whole lifetime. Dropping it releases the lock,
/// which is what lets a later daemon own the directory after a crash
/// (the OS releases the lock when the process dies).
pub struct OwnerLock {
    _file: File,
    #[allow(dead_code)]
    path: PathBuf,
}

impl OwnerLock {
    /// Acquire the exclusive owner lock for `state_dir`. The directory
    /// is created if needed. This must happen before opening any
    /// database in the directory.
    pub fn acquire(state_dir: &Path) -> Result<Self, DaemonError> {
        std::fs::create_dir_all(state_dir)?;
        let path = state_dir.join("daemon.lock");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)?;
        match file.try_lock_exclusive() {
            Ok(()) => Ok(Self { _file: file, path }),
            Err(_) => Err(DaemonError::AlreadyRunning),
        }
    }
}

/// Spawn a daemon detached and hidden: no console window, no inherited
/// stdio, and no parent/child lifetime coupling, so closing the CLI
/// never kills the daemon. Arguments are passed as an argv array —
/// never through a shell string.
///
/// Where the detached daemon ended up relative to jobs. `EscapedJob` is
/// the goal: no job ties the daemon to the CLI lifetime. `JoinedParentJob`
/// is the honest fallback when the parent job forbids breakaway: the daemon
/// runs, but it inherits the parent job and may die with it. The reason is
/// published so callers can warn instead of assuming independence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DetachPlacement {
    EscapedJob,
    JoinedParentJob(String),
}

/// A detached daemon child plus where it landed relative to jobs.
pub struct DetachedChild {
    pub child: Child,
    pub placement: DetachPlacement,
}

/// Spawn a daemon detached and hidden: no console window, no inherited
/// stdio, and no parent/child lifetime coupling, so closing the CLI
/// never kills the daemon. Arguments are passed as an argv array —
/// never through a shell string.
///
/// On Windows the child first tries to break away from any job tied to the
/// CLI lifetime (`CREATE_BREAKAWAY_FROM_JOB`): the daemon must not live in
/// a job that dies with the CLI. When the parent job forbids breakaway
/// (access denied), the spawn retries inside the inherited job and reports
/// [`DetachPlacement::JoinedParentJob`] with the reason — running contained
/// but never silently. Any other failure is a loud error.
pub fn spawn_detached(
    program: &Path,
    args: &[String],
    log_path: &Path,
) -> Result<DetachedChild, DaemonError> {
    let spawn_with = |breakaway: bool| -> Result<Child, DaemonError> {
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)?;
        let log_err = log.try_clone()?;
        let mut command = Command::new(program);
        command
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(log_err));
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            // DETACHED_PROCESS | CREATE_NO_WINDOW: the daemon survives the
            // CLI and never flashes a console.
            const DETACHED_PROCESS: u32 = 0x0000_0008;
            const CREATE_NO_WINDOW: u32 = 0x0800_0000;
            const CREATE_BREAKAWAY_FROM_JOB: u32 = 0x0100_0000;
            let mut flags = DETACHED_PROCESS | CREATE_NO_WINDOW;
            if breakaway {
                // Keeps the daemon out of any job tied to the CLI lifetime.
                flags |= CREATE_BREAKAWAY_FROM_JOB;
            }
            command.creation_flags(flags);
        }
        #[cfg(not(windows))]
        let _ = breakaway;
        Ok(command.spawn()?)
    };
    match spawn_with(true) {
        Ok(child) => Ok(DetachedChild {
            child,
            placement: DetachPlacement::EscapedJob,
        }),
        Err(DaemonError::Io(error)) if error.raw_os_error() == Some(5) && cfg!(windows) => {
            // ERROR_ACCESS_DENIED on the breakaway attempt: the parent job
            // forbids leaving. Retry inside the inherited job and say so.
            let child = spawn_with(false)?;
            Ok(DetachedChild {
                child,
                placement: DetachPlacement::JoinedParentJob(
                    "parent job forbids breakaway (access denied); daemon runs in the \
                     inherited job and may die with the CLI lifetime"
                        .to_string(),
                ),
            })
        }
        Err(other) => Err(other),
    }
}
