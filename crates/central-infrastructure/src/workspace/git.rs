//! Argv-only Git access for workspaces (P3 Task 5).
//!
//! Every git invocation goes through [`Git::run`] with an argument vector —
//! never a shell string — and carries a per-command owned empty hooks dir
//! (`-c core.hooksPath=<dir>`), so Central's mechanical operations never
//! fire user hooks and never mutate global git config. Timeouts kill the
//! child; cleanup is explicit and ownership-checked at the manager layer.

use std::path::{Path, PathBuf};
use std::time::Duration;

use central_application::contracts::AppError;

/// Cap for one mechanical git operation. Fixture repos are tiny; production
/// repos deserve an explicit failure over an unbounded hang.
const GIT_TIMEOUT: Duration = Duration::from_secs(60);

/// Git bound to one owned empty hooks directory. The directory is created by
/// the caller (the manager owns it under its state root); it must exist,
/// be a directory, and stay empty so no hook ever runs.
#[derive(Clone, Debug)]
pub struct Git {
    hooks_dir: PathBuf,
}

impl Git {
    /// Bind git to `hooks_dir`, creating it when missing. An existing
    /// non-empty hooks dir is refused: hooks must never run here.
    pub fn new(hooks_dir: PathBuf) -> Result<Self, AppError> {
        std::fs::create_dir_all(&hooks_dir).map_err(|error| {
            AppError::Storage(format!(
                "cannot create owned git hooks dir {}: {error}",
                hooks_dir.display()
            ))
        })?;
        let entries = std::fs::read_dir(&hooks_dir).map_err(|error| {
            AppError::Storage(format!(
                "cannot list owned git hooks dir {}: {error}",
                hooks_dir.display()
            ))
        })?;
        let count = entries.count();
        if count != 0 {
            return Err(AppError::Forbidden(format!(
                "owned git hooks dir {} is not empty; hooks must never run here",
                hooks_dir.display()
            )));
        }
        Ok(Self { hooks_dir })
    }

    /// Run `git -c core.hooksPath=<owned-empty> <args>` in `repo`, argv-only.
    /// Stdout is returned trimmed; failure and timeout report stderr without
    /// spawning anything else. No shell, no global config mutation.
    pub async fn run(&self, repo: &Path, args: &[&str]) -> Result<String, AppError> {
        let hooks = self.hooks_dir.to_string_lossy().into_owned();
        let hooks_flag = format!("core.hooksPath={hooks}");
        let mut command = tokio::process::Command::new("git");
        command
            .arg("-c")
            .arg(&hooks_flag)
            .args(args)
            .current_dir(repo)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_TERMINAL_PROMPT", "0")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(|error| {
            AppError::Transport(format!("cannot spawn git {}: {error}", args.join(" ")))
        })?;
        let mut stdout = child.stdout.take();
        let mut stderr = child.stderr.take();
        let status = tokio::time::timeout(GIT_TIMEOUT, child.wait())
            .await
            .map_err(|_| {
                AppError::Transport(format!(
                    "git {} timed out after {}s; child killed",
                    args.join(" "),
                    GIT_TIMEOUT.as_secs()
                ))
            })?
            .map_err(|error| {
                AppError::Transport(format!("git {} failed to report: {error}", args.join(" ")))
            })?;
        let out_bytes = drain(&mut stdout).await?;
        let err_bytes = drain(&mut stderr).await?;
        if !status.success() {
            return Err(AppError::Storage(format!(
                "git {} failed: {}",
                args.join(" "),
                String::from_utf8_lossy(&err_bytes).trim()
            )));
        }
        Ok(String::from_utf8_lossy(&out_bytes).trim().to_string())
    }

    /// Resolve `rev` to a commit SHA (`rev^{commit}`), refusing non-commits.
    pub async fn rev_parse_commit(&self, repo: &Path, rev: &str) -> Result<String, AppError> {
        let candidate = format!("{rev}^{{commit}}");
        let sha = self
            .run(repo, &["rev-parse", "--verify", &candidate])
            .await?;
        if sha.len() != 40 || !sha.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(AppError::Storage(format!(
                "git resolved {rev} to a non-commit id: {sha}"
            )));
        }
        Ok(sha)
    }

    /// The tree a commit points at.
    pub async fn commit_tree(&self, repo: &Path, commit: &str) -> Result<String, AppError> {
        let candidate = format!("{commit}^{{tree}}");
        self.run(repo, &["rev-parse", "--verify", &candidate]).await
    }

    /// The current index/worktree tree of a checked-out worktree.
    pub async fn worktree_tree(&self, worktree: &Path) -> Result<String, AppError> {
        self.run(worktree, &["write-tree"]).await
    }

    /// Porcelain status of a worktree (empty when clean).
    pub async fn porcelain_status(&self, repo: &Path) -> Result<String, AppError> {
        self.run(repo, &["status", "--porcelain"]).await
    }

    /// One detached worktree for one attempt. Never checks out a branch, so
    /// old workers on branches are never rebased or moved.
    pub async fn add_worktree_detached(
        &self,
        repo: &Path,
        path: &Path,
        commit: &str,
    ) -> Result<(), AppError> {
        let target = path.to_string_lossy().into_owned();
        self.run(repo, &["worktree", "add", "--detach", &target, commit])
            .await
            .map(|_| ())
    }

    /// A branch-bound worktree for integration merges.
    pub async fn add_worktree_branch(
        &self,
        repo: &Path,
        path: &Path,
        branch: &str,
    ) -> Result<(), AppError> {
        let target = path.to_string_lossy().into_owned();
        self.run(repo, &["worktree", "add", &target, branch])
            .await
            .map(|_| ())
    }

    /// Explicit worktree removal. The manager checks ownership first; this
    /// only performs the git-side removal.
    pub async fn remove_worktree(&self, repo: &Path, path: &Path) -> Result<(), AppError> {
        let target = path.to_string_lossy().into_owned();
        self.run(repo, &["worktree", "remove", "--force", &target])
            .await
            .map(|_| ())
    }

    /// Create `branch` at `base`, refusing when it already exists.
    pub async fn create_branch(
        &self,
        repo: &Path,
        branch: &str,
        base: &str,
    ) -> Result<(), AppError> {
        self.run(repo, &["branch", branch, base]).await.map(|_| ())
    }

    /// Delete an integration branch after promotion or abandonment.
    pub async fn delete_branch(&self, repo: &Path, branch: &str) -> Result<(), AppError> {
        self.run(repo, &["branch", "-D", branch]).await.map(|_| ())
    }

    /// Merge one candidate commit with a deterministic message. On conflict
    /// the caller must [`Git::merge_abort`] — never resolve here. Identity
    /// travels per command so a non-fast-forward merge never depends on
    /// ambient git config.
    pub async fn merge_commit(
        &self,
        worktree: &Path,
        sha: &str,
        message: &str,
    ) -> Result<(), AppError> {
        self.run(
            worktree,
            &[
                "-c",
                "user.name=central",
                "-c",
                "user.email=central@local",
                "merge",
                "--no-edit",
                "-m",
                message,
                sha,
            ],
        )
        .await
        .map(|_| ())
    }

    /// Abort an in-progress merge, restoring the pre-merge state.
    pub async fn merge_abort(&self, worktree: &Path) -> Result<(), AppError> {
        self.run(worktree, &["merge", "--abort"]).await.map(|_| ())
    }

    /// Atomic compare-and-swap of a ref: moves only when the ref still
    /// points at `old`. Returns the ref head after the attempt.
    pub async fn update_ref_cas(
        &self,
        repo: &Path,
        ref_name: &str,
        new: &str,
        old: &str,
    ) -> Result<String, AppError> {
        // An empty old-value is a creation CAS: the all-zero id means
        // "must not exist", so a first promotion creates the ref while a
        // concurrent creation still refuses instead of force-moving.
        let old = if old.is_empty() { Self::ZERO_OID } else { old };
        self.run(repo, &["update-ref", ref_name, new, old]).await?;
        self.read_ref(repo, ref_name).await
    }

    /// The commit a ref points at right now.
    pub async fn read_ref(&self, repo: &Path, ref_name: &str) -> Result<String, AppError> {
        self.run(repo, &["rev-parse", "--verify", ref_name]).await
    }

    /// The all-zero object id: the CAS old-value that means "must not
    /// exist" (`git update-ref <ref> <new> <zero>` creates only when the
    /// ref is absent, and refuses when anything appeared concurrently).
    pub const ZERO_OID: &'static str = "0000000000000000000000000000000000000000";

    /// The commit a ref points at, or `None` when the ref is absent. Only
    /// the missing-ref diagnostic maps to `None` (git has emitted
    /// `fatal: Needed a single revision` for `rev-parse --verify` on a
    /// missing ref for over a decade); every other failure propagates so
    /// a broken repo never looks like a fresh ref.
    pub async fn try_read_ref(
        &self,
        repo: &Path,
        ref_name: &str,
    ) -> Result<Option<String>, AppError> {
        match self.run(repo, &["rev-parse", "--verify", ref_name]).await {
            Ok(head) => Ok(Some(head)),
            Err(AppError::Storage(message)) if message.contains("Needed a single revision") => {
                Ok(None)
            }
            Err(other) => Err(other),
        }
    }

    /// True when `rev` names an existing object (read-only probe for sync).
    pub async fn object_exists(&self, repo: &Path, rev: &str) -> Result<bool, AppError> {
        let candidate = format!("{rev}^{{commit}}");
        let output = tokio::process::Command::new("git")
            .arg("-c")
            .arg(format!(
                "core.hooksPath={}",
                self.hooks_dir.to_string_lossy()
            ))
            .args(["cat-file", "-e", &candidate])
            .current_dir(repo)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_TERMINAL_PROMPT", "0")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| AppError::Transport(format!("cannot spawn git cat-file: {error}")))?;
        let status = tokio::time::timeout(GIT_TIMEOUT, output.wait_with_output())
            .await
            .map_err(|_| {
                AppError::Transport(format!(
                    "git cat-file timed out after {}s",
                    GIT_TIMEOUT.as_secs()
                ))
            })?
            .map_err(|error| {
                AppError::Transport(format!("git cat-file failed to report: {error}"))
            })?;
        Ok(status.status.success())
    }
}

async fn drain(pipe: &mut Option<impl tokio::io::AsyncRead + Unpin>) -> Result<Vec<u8>, AppError> {
    use tokio::io::AsyncReadExt as _;
    let mut bytes = Vec::new();
    if let Some(handle) = pipe.as_mut() {
        handle
            .read_to_end(&mut bytes)
            .await
            .map_err(|error| AppError::Transport(format!("git output is unreadable: {error}")))?;
    }
    Ok(bytes)
}
