//! Approved-check runner (P2 Task 5): runs only approved program+argv.
//!
//! The runner is constructed with the approved plan's hash and the exact
//! commands that plan allows. A request must name that plan and match one
//! entry exactly — same program, same argv — or it is refused without
//! spawning anything. The working directory
//! and every allowed write root must stay inside the runner's work root, and
//! the child is killed on timeout. Stdout/stderr are captured, hashed and
//! returned as evidence; evidence is never a verdict.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use central_application::artifacts::content_hash;
use central_application::contracts::AppError;
use central_application::ports::checks::{CheckEvidence, CheckRequest, CheckRunner};
use central_domain::PlanHash;

/// One approved command: an exact program+argv pair. No globbing, no shell
/// expansion, no model prose — the entry matches byte for byte or it refuses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApprovedCommand {
    pub program: String,
    pub args: Vec<String>,
}

/// [`CheckRunner`] over one work root. `work_root` is canonicalized at
/// construction so junctions/symlinks cannot smuggle paths out later.
/// `plan_hash` is the approved plan the allowlist came from: a request
/// naming any other plan is refused even when its program+argv match.
pub struct SupervisedCheckRunner {
    plan_hash: PlanHash,
    approved: Vec<ApprovedCommand>,
    work_root: PathBuf,
}

/// Bytes of stdout/stderr kept verbatim in the tail.
const TAIL_BYTES: usize = 4_096;

impl SupervisedCheckRunner {
    pub fn new(
        plan_hash: PlanHash,
        approved: Vec<ApprovedCommand>,
        work_root: PathBuf,
    ) -> Result<Self, AppError> {
        let work_root = work_root
            .canonicalize()
            .map_err(|error| AppError::Storage(format!("check work root is unusable: {error}")))?;
        Ok(Self {
            plan_hash,
            approved,
            work_root,
        })
    }

    fn contained(&self, raw: &str) -> Result<PathBuf, AppError> {
        reject_raw_path(raw)?;
        let unified = raw.replace('\\', "/");
        let absolute = is_unix_absolute(&unified) || is_drive_absolute(&unified);
        let joined;
        let candidate: &Path = if absolute {
            joined = normalize(raw);
            &joined
        } else {
            joined = normalize_join(&self.work_root, raw);
            &joined
        };
        // Prefer a canonical comparison when the path exists so a junction
        // inside the root cannot escape it; otherwise fall back to lexical.
        let resolved = candidate
            .canonicalize()
            .unwrap_or_else(|_| candidate.to_path_buf());
        let root = self
            .work_root
            .canonicalize()
            .unwrap_or_else(|_| self.work_root.clone());
        if resolved == root || resolved.starts_with(&root) {
            Ok(candidate.to_path_buf())
        } else {
            Err(AppError::Forbidden(format!(
                "check path escapes the work root: {raw}"
            )))
        }
    }
}

fn normalize(raw: &str) -> PathBuf {
    normalize_join(&PathBuf::new(), raw)
}

/// Reject Windows-unsafe syntax before any resolution: NUL, device
/// namespaces (`\\?\`, `\\.\`) and NTFS alternate streams (`:`) fail
/// closed. A single leading drive prefix (`C:/...`) is allowed so an
/// absolute worktree on Windows can still prove containment; any other
/// colon is a stream and is refused.
fn reject_raw_path(raw: &str) -> Result<(), AppError> {
    if raw.contains('\0') {
        return Err(AppError::Forbidden("check path contains NUL".to_string()));
    }
    let unified = raw.replace('\\', "/");
    if unified.starts_with("//?/") || unified.starts_with("//./") {
        return Err(AppError::Forbidden(format!(
            "check path uses a device namespace: {raw}"
        )));
    }
    if let Some(colon_pos) = unified.find(':') {
        let drive_ok = colon_pos == 1
            && unified.len() >= 2
            && unified.as_bytes()[0].is_ascii_alphabetic()
            && unified.as_bytes()[1] == b':'
            && (unified.len() == 2 || unified.as_bytes()[2] == b'/');
        if !drive_ok {
            return Err(AppError::Forbidden(format!(
                "check path escapes the work root: {raw}"
            )));
        }
        if unified[2..].contains(':') {
            return Err(AppError::Forbidden(format!(
                "check path escapes the work root: {raw}"
            )));
        }
    }
    Ok(())
}

fn is_unix_absolute(unified: &str) -> bool {
    unified.starts_with('/')
}

fn is_drive_absolute(unified: &str) -> bool {
    let Some(first) = unified.split('/').next() else {
        return false;
    };
    first.len() == 2 && first.ends_with(':') && first.as_bytes()[0].is_ascii_alphabetic()
}

/// Lexically join `raw` under `base`, resolving `.`/`..` without touching
/// the filesystem. A `..` that climbs past the base is kept as `..` so the
/// containment check below rejects it. Absolute inputs keep their root
/// (`/` on Unix, `C:/` for a drive prefix) so an absolute worktree inside
/// the work root still proves containment; anything outside still fails.
fn normalize_join(base: &Path, raw: &str) -> PathBuf {
    let unified = raw.replace('\\', "/");
    let unix_abs = is_unix_absolute(&unified);
    let drive_abs = is_drive_absolute(&unified);
    let mut out = if unix_abs {
        PathBuf::from("/")
    } else if drive_abs {
        let drive = unified.split('/').next().expect("drive prefix");
        PathBuf::from(format!("{drive}/"))
    } else {
        base.to_path_buf()
    };
    for (index, part) in unified.split('/').enumerate() {
        if drive_abs && !unix_abs && index == 0 {
            continue;
        }
        match part {
            "" | "." => {}
            ".." => {
                if !out.pop() {
                    out.push("..");
                }
            }
            _ => out.push(part),
        }
    }
    out
}

#[async_trait]
impl CheckRunner for SupervisedCheckRunner {
    async fn run(&self, request: CheckRequest) -> Result<CheckEvidence, AppError> {
        if request.approved_plan_hash != self.plan_hash {
            return Err(AppError::Forbidden(format!(
                "check plan {} is not the approved plan {}",
                request.approved_plan_hash, self.plan_hash,
            )));
        }
        if !self
            .approved
            .iter()
            .any(|entry| entry.program == request.program && entry.args == request.args)
        {
            return Err(AppError::Forbidden(format!(
                "check program+argv is not approved: {} {}",
                request.program,
                request.args.join(" "),
            )));
        }
        let cwd = self.contained(&request.cwd)?;
        for root in &request.allowed_write_roots {
            self.contained(root)?;
        }
        let mut child = tokio::process::Command::new(&request.program)
            .args(&request.args)
            .current_dir(&cwd)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| {
                AppError::Transport(format!(
                    "cannot spawn approved check {}: {error}",
                    request.program
                ))
            })?;
        let timeout = std::time::Duration::from_millis(request.timeout_ms.max(1));
        let mut stdout = child.stdout.take();
        let mut stderr = child.stderr.take();
        let status = match tokio::time::timeout(timeout, child.wait()).await {
            Ok(Ok(status)) => status,
            Ok(Err(error)) => {
                return Err(AppError::Transport(format!(
                    "approved check {} failed to report: {error}",
                    request.program
                )));
            }
            Err(_) => {
                let _ = child.kill().await;
                return Err(AppError::Transport(format!(
                    "approved check {} timed out after {} ms; child killed",
                    request.program, request.timeout_ms,
                )));
            }
        };
        let stdout = drain(&mut stdout).await?;
        let stderr = drain(&mut stderr).await?;
        Ok(CheckEvidence {
            program: request.program,
            args: request.args,
            exit_code: status.code().unwrap_or(-1),
            timed_out: false,
            stdout_hash: content_hash(&stdout),
            stderr_hash: content_hash(&stderr),
            stdout_tail: tail(&stdout),
            stderr_tail: tail(&stderr),
        })
    }
}

async fn drain(pipe: &mut Option<impl tokio::io::AsyncRead + Unpin>) -> Result<Vec<u8>, AppError> {
    use tokio::io::AsyncReadExt as _;
    let mut bytes = Vec::new();
    if let Some(handle) = pipe.as_mut() {
        handle.read_to_end(&mut bytes).await.map_err(|error| {
            AppError::Transport(format!("approved check output is unreadable: {error}"))
        })?;
    }
    Ok(bytes)
}

fn tail(bytes: &[u8]) -> String {
    let start = bytes.len().saturating_sub(TAIL_BYTES);
    String::from_utf8_lossy(&bytes[start..]).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absolute_unix_path_keeps_leading_root() {
        assert_eq!(
            normalize("/tmp/state/worktrees/w1"),
            PathBuf::from("/tmp/state/worktrees/w1")
        );
        assert_eq!(
            normalize_join(&PathBuf::from("/tmp/state"), "/tmp/state/worktrees/w1"),
            PathBuf::from("/tmp/state/worktrees/w1")
        );
    }

    #[test]
    fn lexical_dot_segments_resolve_without_escaping() {
        assert_eq!(
            normalize_join(&PathBuf::from("/tmp/state"), "a/./b/../c"),
            PathBuf::from("/tmp/state/a/c")
        );
    }

    #[test]
    fn drive_prefix_is_preserved_lexically() {
        assert_eq!(
            normalize("C:/state/worktrees/w1"),
            PathBuf::from("C:/state/worktrees/w1")
        );
        assert_eq!(
            normalize_join(&PathBuf::from("C:/state"), "C:/state/worktrees/w1"),
            PathBuf::from("C:/state/worktrees/w1")
        );
    }

    #[test]
    fn device_namespace_and_stream_syntax_fail_closed() {
        assert!(reject_raw_path("//?/C:/state").is_err());
        assert!(reject_raw_path("\\\\?\\C:\\state").is_err());
        assert!(reject_raw_path("file:stream").is_err());
        assert!(reject_raw_path("C:/state/file:stream").is_err());
        assert!(reject_raw_path("has\0nul").is_err());
        assert!(reject_raw_path("C:/state/worktrees/w1").is_ok());
        assert!(reject_raw_path("/tmp/state/worktrees/w1").is_ok());
    }
}
