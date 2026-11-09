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
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct VerifySpec {
    pub method: String,
    pub criteria: Vec<String>,
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
