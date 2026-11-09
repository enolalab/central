//! P3 Task 1 review round 1: pin the expected-output hash paths of
//! [`dependency_ready`](central_domain::dependency_ready).
//!
//! `dependencies_of` always emits `expected_output: None` today; wiring a
//! real pin through `PlanTaskV1` belongs to T2's verified-outputs work, so
//! these tests hand-build `Some(hash)` edges instead, proving the
//! matching (ready) and mismatching (blocked) paths of the mechanism
//! itself.

mod support;

use central_domain::{dependency_ready, DependencyKind, TaskDependency, TaskStatus};
use support::{approved_run, task_a};

fn hash(hex: &str) -> central_domain::ContentHash {
    central_domain::ContentHash::from_hex(hex)
}

fn state_with_verified_a() -> (central_domain::RunState, central_domain::ContentHash) {
    let (mut state, _) = approved_run();
    let output = hash(&"aa".repeat(32));
    state.tasks.get_mut(&task_a()).expect("task a").status = TaskStatus::Succeeded;
    state.verified_outputs.insert(task_a(), output.clone());
    (state, output)
}

#[test]
fn pinned_hash_matching_output_is_ready() {
    let (state, output) = state_with_verified_a();
    let edge = TaskDependency {
        predecessor: task_a(),
        kind: DependencyKind::Data,
        expected_output: Some(output),
    };
    assert!(dependency_ready(&edge, &state));
}

#[test]
fn pinned_hash_mismatch_stays_blocked() {
    let (state, _) = state_with_verified_a();
    let edge = TaskDependency {
        predecessor: task_a(),
        kind: DependencyKind::Data,
        expected_output: Some(hash(&"bb".repeat(32))),
    };
    assert!(!dependency_ready(&edge, &state));
}

#[test]
fn unpinned_data_edge_accepts_any_verified_output() {
    // Control for the current wiring: `dependencies_of` emits `None`
    // pins, which any verified output satisfies (and nothing does while
    // the output is still unverified).
    let (state, _) = state_with_verified_a();
    let edge = TaskDependency {
        predecessor: task_a(),
        kind: DependencyKind::Data,
        expected_output: None,
    };
    assert!(dependency_ready(&edge, &state));

    let (unverified, _) = approved_run();
    assert!(!dependency_ready(&edge, &unverified));
}
