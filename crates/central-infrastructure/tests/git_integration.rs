//! P3 Task 5 (RED): incremental integration — serialized nodes, temporary
//! candidate branches, baseline/affected/acceptance checks, and
//! compare-and-swap promotion.
//!
//! Required: `integration_failure_preserves_last_good_ref`,
//! `promotion_uses_compare_and_swap`. Plus receipt recovery from the actual
//! ref and the check-phase wiring. Fixture repos are generated under the OS
//! temp dir (never a real repo).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use central_application::contracts::AppError;
use central_application::integration::{IntegrationPlan, PlannedCheck};
use central_application::ports::workspace::{
    CandidateRef, IntegrationRequest, PromoteRequest, SnapshotRequest, VerificationSummary,
    WorkspaceManager, WorkspaceRequest,
};
use central_domain::{AttemptId, PlanHash, RunId};
use central_infrastructure::checks::{ApprovedCommand, SupervisedCheckRunner};
use central_infrastructure::workspace::{run_integration_checks, GitWorkspaceManager};

static COUNTER: AtomicU64 = AtomicU64::new(10_000);

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

async fn sealed_candidate(
    manager: &GitWorkspaceManager,
    repo: &Path,
    commit: &str,
    attempt: u128,
    edit: Option<(&str, &str)>,
) -> (
    central_application::ports::workspace::WorkspaceRef,
    CandidateRef,
) {
    let snapshot = manager
        .snapshot(snapshot_request(repo, commit))
        .await
        .expect("snapshot");
    let workspace = manager
        .create(WorkspaceRequest {
            attempt_id: AttemptId::from_u128(attempt),
            input: snapshot,
        })
        .await
        .expect("create");
    if let Some((name, body)) = edit {
        std::fs::write(PathBuf::from(&workspace.worktree).join(name), body)
            .expect("worker edit in worktree");
    }
    let candidate = manager.seal(&workspace).await.expect("seal");
    (workspace, candidate)
}

const GOOD_REF: &str = "refs/heads/central-good";

fn read_ref(repo: &Path, name: &str) -> String {
    git(repo, &["rev-parse", "--verify", name])
}

fn plan_hash(name: &str) -> PlanHash {
    PlanHash::from_hex(format!("{name:0<64}"))
}

fn pass_summary() -> VerificationSummary {
    VerificationSummary {
        passed: true,
        baseline_ok: true,
        affected_ok: true,
        acceptance_ok: true,
        evidence: vec!["baseline:ok".to_string()],
    }
}

fn fail_summary() -> VerificationSummary {
    VerificationSummary {
        passed: false,
        baseline_ok: false,
        affected_ok: false,
        acceptance_ok: false,
        evidence: Vec::new(),
    }
}

#[tokio::test]
async fn integration_failure_preserves_last_good_ref() {
    let root = scratch_root("integration-failure");
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).expect("repo dir");
    let base = repo_with_commit(&repo, &[("calc.py", "VALUE = 1\n")], "base");
    git(&repo, &["update-ref", GOOD_REF, &base]);
    let previous_good = read_ref(&repo, GOOD_REF);

    let manager = GitWorkspaceManager::open(&root.join("state")).expect("open manager");
    let run = RunId::from_u128(6_100);
    let (_ws, candidate) = sealed_candidate(
        &manager,
        &repo,
        &base,
        6_101,
        Some(("calc.py", "VALUE = 2\n")),
    )
    .await;

    // Failed checks never move the good ref: promotion without a pass refuses.
    let prepared = manager
        .prepare_integration(IntegrationRequest {
            run,
            base: base.clone(),
            candidates: vec![candidate.clone()],
            approved_plan: plan_hash("aa"),
            ref_name: GOOD_REF.to_string(),
        })
        .await
        .expect("prepare");
    let refused = manager
        .promote(PromoteRequest {
            candidate: prepared,
            expected_parent: previous_good.clone(),
            verification: fail_summary(),
        })
        .await;
    assert!(
        refused.is_err(),
        "promotion on failed checks must refuse, never move the good ref"
    );
    let main_ref_after_failure = read_ref(&repo, GOOD_REF);
    assert_eq!(
        main_ref_after_failure, previous_good,
        "failed checks must not update the good ref"
    );

    // A conflicting merge never resolves itself and never touches the ref:
    // no out-of-plan conflict-resolution task is created; the leader owns it.
    let (_ws_b, candidate_b) = sealed_candidate(
        &manager,
        &repo,
        &base,
        6_102,
        Some(("calc.py", "VALUE = 3\n")),
    )
    .await;
    let conflict = manager
        .prepare_integration(IntegrationRequest {
            run,
            base: base.clone(),
            candidates: vec![candidate, candidate_b],
            approved_plan: plan_hash("aa"),
            ref_name: GOOD_REF.to_string(),
        })
        .await;
    assert!(
        matches!(conflict, Err(AppError::Conflict(_))),
        "conflicting candidates must surface as conflict for the leader: {conflict:?}"
    );
    assert_eq!(
        read_ref(&repo, GOOD_REF),
        previous_good,
        "conflict must not move the good ref"
    );
}

#[tokio::test]
async fn promotion_uses_compare_and_swap() {
    let root = scratch_root("promotion-cas");
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).expect("repo dir");
    let base = repo_with_commit(&repo, &[("calc.py", "VALUE = 1\n")], "base");
    git(&repo, &["update-ref", GOOD_REF, &base]);

    let manager = GitWorkspaceManager::open(&root.join("state")).expect("open manager");
    let run = RunId::from_u128(6_200);
    let (_ws, candidate) = sealed_candidate(
        &manager,
        &repo,
        &base,
        6_201,
        Some(("calc.py", "VALUE = 2\n")),
    )
    .await;

    // The happy path moves the ref exactly once, from the expected parent.
    let prepared = manager
        .prepare_integration(IntegrationRequest {
            run,
            base: base.clone(),
            candidates: vec![candidate],
            approved_plan: plan_hash("bb"),
            ref_name: GOOD_REF.to_string(),
        })
        .await
        .expect("prepare");
    let receipt = manager
        .promote(PromoteRequest {
            candidate: prepared,
            expected_parent: base.clone(),
            verification: pass_summary(),
        })
        .await
        .expect("promote with current parent");
    assert_eq!(receipt.previous_head, base);
    assert_eq!(read_ref(&repo, GOOD_REF), receipt.new_head);

    // A stale parent is a conflict, never a force-move: the good ref stays.
    let (_ws2, candidate2) = sealed_candidate(
        &manager,
        &repo,
        &receipt.new_head,
        6_202,
        Some(("calc.py", "VALUE = 3\n")),
    )
    .await;
    let prepared2 = manager
        .prepare_integration(IntegrationRequest {
            run,
            base: receipt.new_head.clone(),
            candidates: vec![candidate2],
            approved_plan: plan_hash("bb"),
            ref_name: GOOD_REF.to_string(),
        })
        .await
        .expect("prepare again");
    let promote_with_old_parent = manager
        .promote(PromoteRequest {
            candidate: prepared2,
            expected_parent: base.clone(),
            verification: pass_summary(),
        })
        .await;
    assert!(
        matches!(promote_with_old_parent, Err(AppError::Conflict(_))),
        "a stale expected parent must conflict: {promote_with_old_parent:?}"
    );
    assert_eq!(
        read_ref(&repo, GOOD_REF),
        receipt.new_head,
        "CAS failure must not move the good ref; a new candidate and checks are required"
    );
}

#[tokio::test]
async fn promotion_receipt_recovered_from_actual_ref() {
    let root = scratch_root("promotion-recover");
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).expect("repo dir");
    let base = repo_with_commit(&repo, &[("calc.py", "VALUE = 1\n")], "base");
    git(&repo, &["update-ref", GOOD_REF, &base]);

    let manager = GitWorkspaceManager::open(&root.join("state")).expect("open manager");
    let run = RunId::from_u128(6_300);
    let (_ws, candidate) = sealed_candidate(
        &manager,
        &repo,
        &base,
        6_301,
        Some(("calc.py", "VALUE = 9\n")),
    )
    .await;
    let prepared = manager
        .prepare_integration(IntegrationRequest {
            run,
            base: base.clone(),
            candidates: vec![candidate],
            approved_plan: plan_hash("cc"),
            ref_name: GOOD_REF.to_string(),
        })
        .await
        .expect("prepare");
    let receipt = manager
        .promote(PromoteRequest {
            candidate: prepared,
            expected_parent: base.clone(),
            verification: pass_summary(),
        })
        .await
        .expect("promote");

    // The durable receipt may be lost after the Git mutation committed:
    // recovery inspects the actual ref/object IDs and reports the truth.
    let recovered = manager
        .recover_promotion(run, GOOD_REF)
        .await
        .expect("recover");
    assert_eq!(recovered.new_head, receipt.new_head);
    assert_eq!(recovered.new_head, read_ref(&repo, GOOD_REF));
}

/// Restart persistence: the live-worktree and sealed-candidate registry
/// survives a manager reopen, so a daemon restart does not orphan a
/// worktree binding or force a re-seal. A corrupt registry fails closed
/// instead of running with amnesia.
#[tokio::test]
async fn workspace_registry_survives_reopen() {
    let root = scratch_root("registry-reopen");
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).expect("repo dir");
    let base = repo_with_commit(&repo, &[("calc.py", "VALUE = 1\n")], "base");

    let attempt = AttemptId::from_u128(9_001);
    let sealed_tree = {
        let manager = GitWorkspaceManager::open(&root.join("state")).expect("open manager");
        let snapshot = manager
            .snapshot(snapshot_request(&repo, &base))
            .await
            .expect("snapshot");
        let workspace = manager
            .create(WorkspaceRequest {
                attempt_id: attempt,
                input: snapshot,
            })
            .await
            .expect("create");
        std::fs::write(
            PathBuf::from(&workspace.worktree).join("calc.py"),
            "VALUE = 2\n",
        )
        .expect("worker edit");
        let candidate = manager.seal(&workspace).await.expect("seal");
        assert!(root.join("state").join("workspaces.json").is_file());
        candidate.tree.clone()
    };
    // Same state dir, fresh manager: the binding is back without a
    // re-create, and the sealed candidate verifies + prepares. Sealing
    // always mints a fresh commit object by design, so identity is the
    // tree (same bytes), not the commit id.
    let reopened = GitWorkspaceManager::open(&root.join("state")).expect("reopen manager");
    let resealed = reopened
        .seal_live(attempt)
        .await
        .expect("seal after reopen");
    assert_eq!(resealed.tree, sealed_tree);
    let run = RunId::from_u128(9_002);
    let prepared = reopened
        .prepare_integration(IntegrationRequest {
            run,
            base: base.clone(),
            candidates: vec![resealed],
            approved_plan: plan_hash("rr"),
            ref_name: GOOD_REF.to_string(),
        })
        .await
        .expect("prepare after reopen");
    assert_eq!(prepared.base, base);

    // A corrupt registry refuses to open with amnesia instead of silently
    // dropping every binding.
    std::fs::write(root.join("state").join("workspaces.json"), "{oops").expect("corrupt");
    assert!(GitWorkspaceManager::open(&root.join("state")).is_err());
}

#[tokio::test]
async fn integration_checks_run_baseline_affected_acceptance() {
    let root = scratch_root("integration-checks");
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).expect("repo dir");
    let base = repo_with_commit(&repo, &[("calc.py", "VALUE = 1\n")], "base");
    git(&repo, &["update-ref", GOOD_REF, &base]);

    let state = root.join("state");
    let manager = GitWorkspaceManager::open(&state).expect("open manager");
    let run = RunId::from_u128(6_400);
    let (_ws, candidate) = sealed_candidate(
        &manager,
        &repo,
        &base,
        6_401,
        Some(("calc.py", "VALUE = 4\n")),
    )
    .await;
    let prepared = manager
        .prepare_integration(IntegrationRequest {
            run,
            base: base.clone(),
            candidates: vec![candidate],
            approved_plan: plan_hash("dd"),
            ref_name: GOOD_REF.to_string(),
        })
        .await
        .expect("prepare");

    // The only approved check is `git rev-parse HEAD` over the candidate
    // worktree: baseline, affected and acceptance all bind the exact commit.
    let approved_plan = plan_hash("dd");
    let runner = SupervisedCheckRunner::new(
        approved_plan.clone(),
        vec![ApprovedCommand {
            program: "git".to_string(),
            args: vec!["rev-parse".to_string(), "HEAD".to_string()],
        }],
        state.clone(),
    )
    .expect("check runner");
    let plan = IntegrationPlan {
        baseline: vec![PlannedCheck {
            program: "git".to_string(),
            args: vec!["rev-parse".to_string(), "HEAD".to_string()],
        }],
        affected: vec![PlannedCheck {
            program: "git".to_string(),
            args: vec!["rev-parse".to_string(), "HEAD".to_string()],
        }],
        acceptance: vec![PlannedCheck {
            program: "git".to_string(),
            args: vec!["rev-parse".to_string(), "HEAD".to_string()],
        }],
    };
    let (evidence, summary) =
        run_integration_checks(&runner, &approved_plan, &prepared.worktree, &plan)
            .await
            .expect("run checks");
    assert_eq!(
        evidence.len(),
        3,
        "baseline + affected + acceptance, in order"
    );
    assert!(summary.passed && summary.baseline_ok && summary.affected_ok && summary.acceptance_ok);

    // The harness (not this helper) judges: unapproved programs never run.
    let bad = IntegrationPlan {
        baseline: vec![PlannedCheck {
            program: "definitely-not-approved".to_string(),
            args: Vec::new(),
        }],
        affected: Vec::new(),
        acceptance: Vec::new(),
    };
    let refused = run_integration_checks(&runner, &approved_plan, &prepared.worktree, &bad).await;
    assert!(
        matches!(refused, Err(AppError::Forbidden(_))),
        "unapproved checks must refuse without spawning: {refused:?}"
    );
}
