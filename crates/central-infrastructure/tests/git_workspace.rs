//! P3 Task 5 (RED): Git workspaces — snapshot, isolated worktrees, sealed
//! candidates, and the user-commit sync queue.
//!
//! Required: `space_unicode_and_junction_paths`,
//! `candidate_seal_detects_mutation`, `sync_does_not_stash_user_work`.
//! Fixture repos are generated under the OS temp dir (never a real repo).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use central_application::contracts::AppError;
use central_application::ports::workspace::{SnapshotRequest, WorkspaceManager, WorkspaceRequest};
use central_domain::{AttemptId, RunId};
use central_infrastructure::workspace::{queue_user_commit, GitWorkspaceManager};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn scratch_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "central-{}-{}-{}",
        name,
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("clear stale scratch");
    }
    std::fs::create_dir_all(&root).expect("create scratch");
    root
}

/// Argv-only git for fixtures: no shell strings, hermetic config, no prompts.
fn git(repo: &Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(repo)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_AUTHOR_NAME", "Central Test")
        .env("GIT_AUTHOR_EMAIL", "central-test@local")
        .env("GIT_COMMITTER_NAME", "Central Test")
        .env("GIT_COMMITTER_EMAIL", "central-test@local")
        .output()
        .expect("spawn git");
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

fn porcelain(repo: &Path) -> String {
    git(repo, &["status", "--porcelain"])
}

fn stash_list(repo: &Path) -> String {
    git(repo, &["stash", "list"])
}

/// `git init` plus one commit of `files`; returns HEAD.
fn repo_with_commit(dir: &Path, files: &[(&str, &str)], message: &str) -> String {
    git(dir, &["init"]);
    for (name, body) in files {
        let path = dir.join(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("fixture dirs");
        }
        std::fs::write(&path, body).expect("fixture file");
    }
    git(dir, &["add", "-A"]);
    git(dir, &["commit", "-m", message]);
    git(dir, &["rev-parse", "HEAD"])
}

fn snapshot_request(repo: &Path, commit: &str) -> SnapshotRequest {
    SnapshotRequest {
        repo_root: repo.to_string_lossy().into_owned(),
        commit: commit.to_string(),
        selected_dirty_paths: Vec::new(),
    }
}

#[tokio::test]
async fn space_unicode_and_junction_paths() {
    // Repos live under a path with spaces and non-ASCII names: every git
    // invocation must be argv-only so nothing is re-parsed by a shell.
    let root = scratch_root("central ws caf\u{00e9} \u{5b50}");
    let repo = root.join("repo dir \u{00e9}");
    std::fs::create_dir_all(&repo).expect("unicode repo dir");
    let head = repo_with_commit(
        &repo,
        &[("calc.py", "def add(a, b):\n    return a + b\n")],
        "base",
    );

    let state = root.join("state");
    let manager = GitWorkspaceManager::open(&state).expect("open manager");

    let snapshot = manager
        .snapshot(snapshot_request(&repo, &head))
        .await
        .expect("snapshot under spaced unicode path");
    assert_eq!(snapshot.commit, head);
    let workspace = manager
        .create(WorkspaceRequest {
            attempt_id: AttemptId::from_u128(5_001),
            input: snapshot,
        })
        .await
        .expect("worktree under spaced unicode path");
    let candidate = manager
        .seal(&workspace)
        .await
        .expect("seal unicode worktree");
    assert_eq!(candidate.parent, head);
    manager
        .verify_seal(&candidate)
        .await
        .expect("fresh seal verifies");

    // A junction inside the root pointing outside must never be followed:
    // resolving through it escapes the root, so containment fails closed.
    let outside = scratch_root("central-junction-outside");
    std::fs::write(outside.join("secret.txt"), "outside").expect("outside file");
    let link = root.join("escape_link");
    let junction = std::process::Command::new("cmd")
        .args([
            "/C",
            "mklink",
            "/J",
            link.to_string_lossy().as_ref(),
            outside.to_string_lossy().as_ref(),
        ])
        .output()
        .map(|out| out.status.success())
        .unwrap_or(false);
    let junction_escape = if junction {
        central_infrastructure::workspace::contained_path(&root, "escape_link/secret.txt")
    } else {
        // Junctions unavailable here: cover the canonical-escape branch
        // with the absolute outside path instead (it exists, and its
        // canonical form is outside the root).
        let absolute_outside = outside.join("secret.txt").to_string_lossy().into_owned();
        central_infrastructure::workspace::contained_path(&root, &absolute_outside)
    };
    assert!(junction_escape.is_err(), "junction escape must fail closed");

    manager
        .remove_workspace(&workspace)
        .await
        .expect("explicit ownership-checked cleanup");
}

#[tokio::test]
async fn candidate_seal_detects_mutation() {
    let root = scratch_root("seal-mutation");
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).expect("repo dir");
    let head = repo_with_commit(
        &repo,
        &[("calc.py", "def add(a, b):\n    return a + b\n")],
        "base",
    );

    let manager = GitWorkspaceManager::open(&root.join("state")).expect("open manager");
    let snapshot = manager
        .snapshot(snapshot_request(&repo, &head))
        .await
        .expect("snapshot");
    let workspace = manager
        .create(WorkspaceRequest {
            attempt_id: AttemptId::from_u128(5_002),
            input: snapshot,
        })
        .await
        .expect("create");
    let candidate = manager.seal(&workspace).await.expect("seal");
    // The sealed source commit is immutable: its recorded parent is the base.
    assert_eq!(candidate.parent, head);

    // Verifier scratch traffic outside the worktree never invalidates evidence.
    let scratch = root.join("verifier-scratch");
    std::fs::create_dir_all(&scratch).expect("scratch dir");
    std::fs::write(scratch.join("notes.txt"), "scratch output").expect("scratch write");
    manager
        .verify_seal(&candidate)
        .await
        .expect("scratch writes keep the seal valid");

    // A tracked-source write inside the worktree mutates the candidate under
    // verification: the evidence is invalid, never a pass.
    let worktree = PathBuf::from(&workspace.worktree);
    std::fs::write(
        worktree.join("calc.py"),
        "def add(a, b):\n    return 'mutated'\n",
    )
    .expect("mutate tracked source");
    let verdict = manager.verify_seal(&candidate).await;
    assert!(
        matches!(verdict, Err(AppError::Conflict(_))),
        "tracked-source mutation must invalidate the seal: {verdict:?}"
    );

    manager
        .remove_workspace(&workspace)
        .await
        .expect("explicit ownership-checked cleanup");
}

#[tokio::test]
async fn sync_does_not_stash_user_work() {
    let root = scratch_root("sync-no-stash");
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).expect("repo dir");
    let head = repo_with_commit(
        &repo,
        &[("calc.py", "def add(a, b):\n    return a + b\n")],
        "base",
    );
    // Uncommitted user work: sync must queue the commit without touching it.
    std::fs::write(
        repo.join("calc.py"),
        "def add(a, b):\n    return a + b  # user edit\n",
    )
    .expect("dirty user file");

    let user_dirty_before = porcelain(&repo);
    assert!(!user_dirty_before.is_empty(), "fixture must start dirty");
    let stash_before = stash_list(&repo);

    let run = RunId::from_u128(5_100);
    let intent = queue_user_commit(&root.join("daemon-state"), run, &repo, &head)
        .expect("queue user commit");
    assert_eq!(intent.commit, head);

    let user_dirty_after = porcelain(&repo);
    let stash_after = stash_list(&repo);
    assert_eq!(
        user_dirty_after, user_dirty_before,
        "sync must never touch the user worktree"
    );
    assert_eq!(stash_after, stash_before, "sync must never stash user work");
    assert!(stash_after.is_empty(), "no stash entry may appear");
}

#[tokio::test]
async fn seal_binds_attempt_identity() {
    let root = scratch_root("seal-attempt-bind");
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).expect("repo dir");
    let head = repo_with_commit(
        &repo,
        &[("calc.py", "def add(a, b):\n    return a + b\n")],
        "base",
    );

    let manager = GitWorkspaceManager::open(&root.join("state")).expect("open manager");
    let snapshot = manager
        .snapshot(snapshot_request(&repo, &head))
        .await
        .expect("snapshot");
    let workspace = manager
        .create(WorkspaceRequest {
            attempt_id: AttemptId::from_u128(5_003),
            input: snapshot,
        })
        .await
        .expect("create");
    let candidate = manager.seal(&workspace).await.expect("seal");
    manager
        .verify_seal(&candidate)
        .await
        .expect("fresh seal verifies");

    // A CandidateRef with a swapped attempt_id must not verify: the seal is
    // bound to the attempt that produced it.
    let mut swapped = candidate.clone();
    swapped.attempt_id = AttemptId::from_u128(9_999);
    let verdict = manager.verify_seal(&swapped).await;
    assert!(
        matches!(verdict, Err(AppError::Conflict(_))),
        "swapped attempt_id must fail verify: {verdict:?}"
    );

    manager
        .remove_workspace(&workspace)
        .await
        .expect("explicit ownership-checked cleanup");
}
