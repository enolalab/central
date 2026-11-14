use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{DomainError, PlanHash, ProfileId, TaskId};

pub const PLAN_SCHEMA_VERSION: u32 = 1;

/// Maximum quality-repair rounds after the first verification (spec section 8).
pub const MAX_REPAIR_ROUNDS: u8 = 2;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TaskKind {
    Planning,
    Implementation,
    Verification,
    Integration,
    Assessment,
    /// A leader analysis step owned by a revision proposal.
    Proposal,
    /// A user approval gate owned by a revision workflow.
    Approval,
}

impl TaskKind {
    /// Whether the kind is a review step rather than build work:
    /// proposal, approval, verification or assessment. Revision
    /// follow-ups surface these first when listing reverify work.
    pub fn is_review(&self) -> bool {
        matches!(
            self,
            TaskKind::Proposal | TaskKind::Approval | TaskKind::Verification | TaskKind::Assessment
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct VerifySpec {
    pub method: String,
    pub criteria: Vec<String>,
}

/// One approved program+argv pair for an integration check phase.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct IntegrationCheck {
    pub program: String,
    #[serde(default)]
    pub args: Vec<String>,
}

/// Machine-readable integration contract for an `integration` task.
/// Baseline, affected and acceptance checks run in that order against
/// the temporary candidate worktree; the good ref moves only on pass.
/// `base_ref` pins the base commit (default: the latest synced commit);
/// `good_ref` names the ref to move and has no default — Central never
/// invents which ref an integration may move.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct IntegrationSpec {
    pub good_ref: String,
    #[serde(default)]
    pub base_ref: Option<String>,
    #[serde(default)]
    pub baseline: Vec<IntegrationCheck>,
    #[serde(default)]
    pub affected: Vec<IntegrationCheck>,
    #[serde(default)]
    pub acceptance: Vec<IntegrationCheck>,
}

impl IntegrationSpec {
    /// Total approved checks across all three phases.
    pub fn check_count(&self) -> usize {
        self.baseline.len() + self.affected.len() + self.acceptance.len()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PlanTaskV1 {
    pub id: TaskId,
    pub kind: TaskKind,
    pub objective: String,
    pub inputs: Vec<String>,
    pub scope: Vec<String>,
    pub out_of_scope: Vec<String>,
    pub depends_on: Vec<TaskId>,
    pub data_depends_on: Vec<TaskId>,
    pub outputs: Vec<String>,
    pub verify: Option<VerifySpec>,
    pub evidence: Vec<String>,
    pub profile_id: Option<ProfileId>,
    pub permissions: Vec<String>,
    pub max_repair_rounds: u8,
    pub requires_approval: Vec<String>,
    pub handover: String,
    /// Machine-readable integration contract. Only meaningful for
    /// `integration` tasks; ignored for every other kind. `None` means
    /// the task has no approved checks, so it can never dispatch — it
    /// waits for an approved revision instead of running unapproved
    /// commands.
    #[serde(default)]
    pub integration: Option<IntegrationSpec>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PlanDocumentV1 {
    pub version: u32,
    pub goal: String,
    pub tasks: Vec<PlanTaskV1>,
}

/// Canonical JSON: serde_json maps are BTree-ordered, struct fields and task
/// order are part of the approved content, and no timestamps are included.
pub fn plan_hash(plan: &PlanDocumentV1) -> PlanHash {
    let value = serde_json::to_value(plan).expect("plan serialization is infallible");
    let canonical = serde_json::to_string(&value).expect("plan serialization is infallible");
    let digest = Sha256::digest(canonical.as_bytes());
    PlanHash::from_hex(hex_encode(&digest))
}

pub fn validate_plan(plan: &PlanDocumentV1) -> Result<PlanHash, DomainError> {
    if plan.version != PLAN_SCHEMA_VERSION {
        return Err(DomainError::UnsupportedPlanVersion(plan.version));
    }
    if plan.goal.trim().is_empty() {
        return Err(DomainError::InvalidPlan(
            "goal must not be empty".to_string(),
        ));
    }
    if plan.tasks.is_empty() {
        return Err(DomainError::EmptyPlan);
    }

    let mut ids = BTreeSet::new();
    for task in &plan.tasks {
        if !ids.insert(task.id) {
            return Err(DomainError::DuplicateTaskId(task.id));
        }
        if task.objective.trim().is_empty() {
            return Err(DomainError::InvalidPlan(format!(
                "task {} objective must not be empty",
                task.id
            )));
        }
        match &task.verify {
            Some(verify)
                if !verify.method.trim().is_empty()
                    && !verify.criteria.is_empty()
                    && verify
                        .criteria
                        .iter()
                        .all(|criterion| !criterion.trim().is_empty()) => {}
            _ if task.kind == TaskKind::Integration => {
                // An integration task is verified by its approved checks,
                // not by a verifier: the integration spec carries the
                // contract instead of a verify block.
                match &task.integration {
                    Some(spec)
                        if !spec.good_ref.trim().is_empty()
                            && spec.check_count() > 0
                            && spec
                                .baseline
                                .iter()
                                .chain(spec.affected.iter())
                                .chain(spec.acceptance.iter())
                                .all(|check| !check.program.trim().is_empty()) => {}
                    _ => return Err(DomainError::MissingVerification(task.id)),
                }
            }
            _ => return Err(DomainError::MissingVerification(task.id)),
        }
        if task.max_repair_rounds > MAX_REPAIR_ROUNDS {
            return Err(DomainError::InvalidPlan(format!(
                "task {} exceeds the maximum of {MAX_REPAIR_ROUNDS} repair rounds",
                task.id
            )));
        }
    }

    for task in &plan.tasks {
        for dependency in task.depends_on.iter().chain(&task.data_depends_on) {
            if !ids.contains(dependency) {
                return Err(DomainError::MissingDependency {
                    task: task.id,
                    dependency: *dependency,
                });
            }
        }
    }

    reject_cycles(plan)?;
    Ok(plan_hash(plan))
}

fn reject_cycles(plan: &PlanDocumentV1) -> Result<(), DomainError> {
    let edges: BTreeMap<TaskId, Vec<TaskId>> = plan
        .tasks
        .iter()
        .map(|task| {
            let mut deps = task.depends_on.clone();
            deps.extend(task.data_depends_on.iter().copied());
            (task.id, deps)
        })
        .collect();

    #[derive(Clone, Copy, PartialEq)]
    enum Mark {
        Visiting,
        Done,
    }

    fn visit(
        node: TaskId,
        edges: &BTreeMap<TaskId, Vec<TaskId>>,
        marks: &mut BTreeMap<TaskId, Mark>,
    ) -> Result<(), DomainError> {
        match marks.get(&node) {
            Some(Mark::Visiting) => return Err(DomainError::DependencyCycle),
            Some(Mark::Done) => return Ok(()),
            None => {}
        }
        marks.insert(node, Mark::Visiting);
        if let Some(deps) = edges.get(&node) {
            for dep in deps {
                visit(*dep, edges, marks)?;
            }
        }
        marks.insert(node, Mark::Done);
        Ok(())
    }

    let mut marks = BTreeMap::new();
    for task in &plan.tasks {
        visit(task.id, &edges, &mut marks)?;
    }
    Ok(())
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}
