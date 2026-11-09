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
        let joined;
        let candidate: &Path = if Path::new(raw).is_absolute() {
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

/// Lexically join `raw` under `base`, resolving `.`/`..` without touching
/// the filesystem. A `..` that climbs past the base is kept as `..` so the
/// containment check below rejects it.
fn normalize_join(base: &Path, raw: &str) -> PathBuf {
    let unified = raw.replace('\\', "/");
    let absolute = unified.starts_with('/')
        || unified
            .split('/')
            .next()
            .is_some_and(|first| first.ends_with(':'));
    let mut out = if absolute {
        PathBuf::new()
    } else {
        base.to_path_buf()
    };
    for part in unified.split('/') {
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
    // An absolute `raw` normalizes from empty, never silently under `base`:
    // only paths that lexically stay inside the work root pass the check in
    // `contained`. Callers pass run-relative paths; absolute paths are
    // rejected fail-closed even when they name the root itself.
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
