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
pub fn spawn_detached(
    program: &Path,
    args: &[String],
    log_path: &Path,
) -> Result<Child, DaemonError> {
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
        command.creation_flags(DETACHED_PROCESS | CREATE_NO_WINDOW);
    }
    Ok(command.spawn()?)
}
