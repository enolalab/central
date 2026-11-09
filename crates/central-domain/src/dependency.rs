//! Typed task dependencies (P3 Task 1).
//!
//! A plan task names two dependency lists: `depends_on` (order — the
//! predecessor must have succeeded first) and `data_depends_on` (data —
//! the predecessor must have succeeded *with a verified output* the
//! dependent's input pins). A data edge carries the expected output hash
//! when the dependent pins one; `None` means any verified output of the
//! predecessor satisfies the edge.
//!
//! Pure: no IO, no mutation, deterministic for a given [`RunState`].

use crate::{ContentHash, RunState, Task, TaskId, TaskStatus};

/// Whether an edge is order-only or data-bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DependencyKind {
    Order,
    Data,
}

/// One typed dependency edge with its optional expected output hash.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskDependency {
    pub predecessor: TaskId,
    pub kind: DependencyKind,
    pub expected_output: Option<ContentHash>,
}

/// Every edge a task waits on: order edges from `depends_on`, data edges
/// from `data_depends_on`. Order is stable (plan order, deduped).
pub fn dependencies_of(task: &Task) -> Vec<TaskDependency> {
    let mut edges = Vec::new();
    for predecessor in &task.depends_on {
        if !edges.iter().any(|edge: &TaskDependency| {
            edge.predecessor == *predecessor && edge.kind == DependencyKind::Order
        }) {
            edges.push(TaskDependency {
                predecessor: *predecessor,
                kind: DependencyKind::Order,
                expected_output: None,
            });
        }
    }
    for predecessor in &task.data_depends_on {
        if !edges.iter().any(|edge: &TaskDependency| {
            edge.predecessor == *predecessor && edge.kind == DependencyKind::Data
        }) {
            edges.push(TaskDependency {
                predecessor: *predecessor,
                kind: DependencyKind::Data,
                expected_output: None,
            });
        }
    }
    edges
}

/// The verified output hash recorded for a task, if any. Written by the
/// quality loop when a verifier accepts a sealed candidate (P3/T2); read
/// here by the scheduling gate.
pub fn verified_output(state: &RunState, task: TaskId) -> Option<&ContentHash> {
    state.verified_outputs.get(&task)
}

/// Whether one edge is satisfied: order needs the predecessor
/// `Succeeded`; data additionally needs a verified output, matching the
/// pinned hash when the edge names one.
pub fn dependency_ready(edge: &TaskDependency, state: &RunState) -> bool {
    let predecessor = match state.tasks.get(&edge.predecessor) {
        Some(task) => task,
        None => return false,
    };
    if predecessor.status != TaskStatus::Succeeded {
        return false;
    }
    match edge.kind {
        DependencyKind::Order => true,
        DependencyKind::Data => match verified_output(state, edge.predecessor) {
            Some(actual) => match &edge.expected_output {
                Some(expected) => actual == expected,
                None => true,
            },
            None => false,
        },
    }
}

/// The edges still blocking a task, in plan order.
pub fn blocking_dependencies(task: &Task, state: &RunState) -> Vec<TaskDependency> {
    dependencies_of(task)
        .into_iter()
        .filter(|edge| !dependency_ready(edge, state))
        .collect()
}
