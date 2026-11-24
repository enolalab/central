//! Read-model projection: curated public views mapped field by
//! field from domain state. The full `RunState` is never serialized to
//! a client: internal maps (attempt inputs, sessions, resource claims,
//! retry bookkeeping, repair bases, revision holds) stay server-side,
//! inbox previews are redacted, and every string passes through the
//! secret scrubber.

use central_application::ports::store::{ChatMessage, UserProfileRecord};
use central_domain::{
    Artifact, Attempt, Event, InboxEntry, PlanDocumentV1, PlanRevision, RunState, Task, UsageLedger,
};

/// Scrub bearer tokens and `secret`-shaped values from free text.
/// Credential answers are never stored in durable state, so this is
/// defense in depth, not the primary control.
pub fn redact_text(text: &str) -> String {
    let mut out = text.to_string();
    for marker in ["Bearer ", "bearer ", "BEARER "] {
        out = scrub_after_marker(&out, marker);
    }
    for key in ["secret", "token", "password", "api_key", "apikey"] {
        out = scrub_key_value(&out, key);
    }
    out
}

fn scrub_after_marker(text: &str, marker: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(pos) = rest.find(marker) {
        out.push_str(&rest[..pos + marker.len()]);
        rest = &rest[pos + marker.len()..];
        let end = rest.find(|c: char| c.is_whitespace()).unwrap_or(rest.len());
        if end > 0 {
            out.push_str("[redacted]");
        }
        out.push_str(&rest[end..]);
        rest = "";
    }
    out.push_str(rest);
    out
}

fn scrub_key_value(text: &str, key: &str) -> String {
    // Case-insensitive `key = value` / `key: value` scrubbing without a
    // regex engine: walk the lowercase view, redact in the original.
    let lower = text.to_lowercase();
    let mut out = String::new();
    let mut cursor = 0;
    let mut search_from = 0;
    while let Some(found) = lower[search_from..].find(key) {
        let start = search_from + found;
        let mut pos = start + key.len();
        let bytes = text.as_bytes();
        while pos < bytes.len() && matches!(bytes[pos], b' ' | b'\t' | b'"' | b'\'') {
            pos += 1;
        }
        if pos >= bytes.len() || !matches!(bytes[pos], b'=' | b':') {
            search_from = start + key.len();
            continue;
        }
        pos += 1;
        while pos < bytes.len() && matches!(bytes[pos], b' ' | b'\t' | b'"' | b'\'') {
            pos += 1;
        }
        let mut end = pos;
        while end < bytes.len()
            && !bytes[end].is_ascii_whitespace()
            && !matches!(bytes[end], b'"' | b'\'' | b',' | b'}')
        {
            end += 1;
        }
        if end == pos {
            search_from = start + key.len();
            continue;
        }
        out.push_str(&text[cursor..pos]);
        out.push_str("[redacted]");
        cursor = end;
        search_from = end;
    }
    out.push_str(&text[cursor..]);
    out
}

/// Recursively redact every string inside a JSON payload.
pub fn redact_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::String(text) => serde_json::Value::String(redact_text(&text)),
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.into_iter().map(redact_value).collect())
        }
        serde_json::Value::Object(map) => serde_json::Value::Object(
            map.into_iter()
                .map(|(key, item)| (key, redact_value(item)))
                .collect(),
        ),
        other => other,
    }
}

/// The curated run view: scheduling, approval, budget and verification
/// state with provenance, without internal scheduling maps.
pub fn project_state(state: &RunState) -> serde_json::Value {
    let tasks: Vec<serde_json::Value> = state.tasks.values().map(project_task_inner).collect();
    let attempts: Vec<serde_json::Value> = state
        .attempts
        .values()
        .map(|attempt| project_attempt(state, attempt))
        .collect();
    let mut inbox: Vec<serde_json::Value> = state.inbox.values().map(project_inbox_entry).collect();
    inbox.sort_by(|left, right| left["key"].as_str().cmp(&right["key"].as_str()));
    let mut plans: Vec<serde_json::Value> = state.plans.values().map(project_plan).collect();
    plans.sort_by(|left, right| left["hash"].as_str().cmp(&right["hash"].as_str()));
    let mut artifacts: Vec<serde_json::Value> =
        state.artifacts.values().map(project_artifact).collect();
    artifacts.sort_by(|left, right| left["id"].as_str().cmp(&right["id"].as_str()));
    serde_json::json!({
        "run_id": state.id.to_string(),
        "revision": state.revision.0,
        "created": state.created,
        "goal": state.goal,
        "repository": state.repository,
        "control": format!("{:?}", state.control),
        "holds": state.holds.iter().map(|hold| format!("{hold:?}")).collect::<Vec<_>>(),
        "active_plan": state.active_plan.as_ref().map(|hash| hash.to_string()),
        "pending_plan": state.pending_plan.as_ref().map(|hash| hash.to_string()),
        "worker_profile": state.worker_profile.as_ref().map(|id| id.to_string()),
        "leader_profile": state.leader_profile.as_ref().map(|id| id.to_string()),
        "plans": plans,
        "tasks": tasks,
        "attempts": attempts,
        "inbox": inbox,
        "artifacts": artifacts,
        "usage": project_usage(&state.usage),
        "verified_outputs": state.verified_outputs.iter().map(|(task, hash)| serde_json::json!({
            "task_id": task.to_string(),
            "candidate_hash": hash.to_string(),
        })).collect::<Vec<_>>(),
        "verification_disputes": state.verification_disputes.iter().map(|(task, count)| serde_json::json!({
            "task_id": task.to_string(),
            "disputes": count,
        })).collect::<Vec<_>>(),
        "stale_candidates": state.stale_candidates.values().map(|candidate| serde_json::json!({
            "attempt_id": candidate.attempt_id.to_string(),
            "task_id": candidate.task_id.to_string(),
            "bound_plan": candidate.bound_plan.to_string(),
            "candidate_hash": candidate.candidate_hash.to_string(),
            "reuse": format!("{:?}", candidate.reuse),
        })).collect::<Vec<_>>(),
    })
}

fn project_task_inner(task: &Task) -> serde_json::Value {
    serde_json::json!({
        "task_id": task.id.to_string(),
        "plan_hash": task.plan_hash.to_string(),
        "kind": format!("{:?}", task.kind),
        "objective": task.objective,
        "status": format!("{:?}", task.status),
        "depends_on": task.depends_on.iter().map(|id| id.to_string()).collect::<Vec<_>>(),
        "data_depends_on": task.data_depends_on.iter().map(|id| id.to_string()).collect::<Vec<_>>(),
        "verify": task.verify,
        // The exact criteria hash a verifier verdict must cite: criteria
        // change only through an approved plan revision, and a verdict
        // naming any other hash is stale.
        "criteria_hash": central_domain::criteria_hash_for(&task.verify).to_string(),
        "profile_id": task.profile_id.as_ref().map(|id| id.to_string()),
        "max_repair_rounds": task.max_repair_rounds,
        "repair_rounds_used": task.repair_rounds_used,
    })
}

/// One task with its wait reason: why it is not running right now, in
/// words a client can show without replaying the scheduler.
pub fn project_task(state: &RunState, task: &Task) -> serde_json::Value {
    let mut view = project_task_inner(task);
    let reason = wait_reason(state, task);
    view["wait_reason"] = serde_json::Value::String(reason);
    if let Some(hash) = state.verified_outputs.get(&task.id) {
        view["verified_output"] = serde_json::Value::String(hash.to_string());
    }
    view["disputes"] = state
        .verification_disputes
        .get(&task.id)
        .copied()
        .unwrap_or(0)
        .into();
    view["repair_pending"] = state.repair_base.contains_key(&task.id).into();
    view
}

fn wait_reason(state: &RunState, task: &Task) -> String {
    use central_domain::{RunControl, TaskStatus};
    if !matches!(task.status, TaskStatus::Pending | TaskStatus::Ready) {
        return format!("task is {:?}", task.status);
    }
    if state.control != RunControl::Running {
        return format!("run is {:?}; resume to schedule", state.control);
    }
    if !state.holds.is_empty() {
        let holds: Vec<String> = state.holds.iter().map(|hold| format!("{hold:?}")).collect();
        return format!("held: {}", holds.join(", "));
    }
    let unmet: Vec<String> = task
        .depends_on
        .iter()
        .filter(|dep| {
            state
                .tasks
                .get(dep)
                .is_none_or(|task| task.status != TaskStatus::Succeeded)
        })
        .map(|dep| dep.to_string())
        .collect();
    if !unmet.is_empty() {
        return format!("waiting on {}", unmet.join(", "));
    }
    "ready".to_string()
}

/// One attempt with its frozen bindings: profile, session evidence and
/// verification linkage. History is never rewritten here.
pub fn project_attempt(state: &RunState, attempt: &Attempt) -> serde_json::Value {
    let session = state.attempt_sessions.get(&attempt.id).map(|binding| {
        serde_json::json!({
            "provider": binding.provider,
            "instance_epoch": binding.instance_epoch,
            "thread_id": binding.thread_id,
        })
    });
    serde_json::json!({
        "attempt_id": attempt.id.to_string(),
        "task_id": attempt.task_id.to_string(),
        "plan_hash": attempt.plan_hash.to_string(),
        "status": format!("{:?}", attempt.status),
        "profile_id": attempt.profile_id.as_ref().map(|id| id.to_string()),
        "created_at": attempt.created_at.0,
        "session": session,
        "predecessor": state.attempt_predecessor.get(&attempt.id).map(|id| id.to_string()),
    })
}

pub fn project_plan(plan: &PlanRevision) -> serde_json::Value {
    serde_json::json!({
        "hash": plan.hash.to_string(),
        "status": format!("{:?}", plan.status),
        "proposed_by": plan.proposed_by.to_string(),
        "proposed_at": plan.proposed_at.0,
        "approved_by": plan.approved_by.as_ref().map(|id| id.to_string()),
        "approved_at": plan.approved_at.as_ref().map(|at| at.0),
        "document": plan.document,
    })
}

/// Diff two plan documents: added/removed/changed tasks plus whether
/// verification criteria moved (criteria moves need an explanation and
/// user approval before they take effect).
pub fn plan_diff(from: &PlanDocumentV1, to: &PlanDocumentV1) -> serde_json::Value {
    use std::collections::BTreeMap;
    let before: BTreeMap<String, _> = from
        .tasks
        .iter()
        .map(|task| (task.id.to_string(), task))
        .collect();
    let after: BTreeMap<String, _> = to
        .tasks
        .iter()
        .map(|task| (task.id.to_string(), task))
        .collect();
    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut changed = Vec::new();
    let mut criteria_moved = false;
    for (id, task) in &before {
        match after.get(id) {
            None => removed.push(id.clone()),
            Some(next) => {
                if task.objective != next.objective
                    || task.kind != next.kind
                    || task.depends_on != next.depends_on
                    || task.data_depends_on != next.data_depends_on
                {
                    changed.push(id.clone());
                }
                if task.verify != next.verify {
                    changed.push(id.clone());
                    criteria_moved = true;
                }
            }
        }
    }
    for id in after.keys() {
        if !before.contains_key(id) {
            added.push(id.clone());
        }
    }
    serde_json::json!({
        "added": added,
        "removed": removed,
        "changed": changed,
        "criteria_moved": criteria_moved,
    })
}

/// One inbox entry, redacted: secret requests carry a placeholder
/// preview (set at the source), and every string is scrubbed.
pub fn project_inbox_entry(entry: &InboxEntry) -> serde_json::Value {
    let preview = if entry.is_secret {
        "[secret request: preview withheld]".to_string()
    } else {
        redact_text(&entry.preview)
    };
    serde_json::json!({
        "key": entry.key,
        "attempt_id": entry.attempt_id.as_ref().map(|id| id.to_string()),
        "kind": format!("{:?}", entry.kind),
        "status": format!("{:?}", entry.status),
        "preview_hash": entry.preview_hash.to_string(),
        "preview": preview,
        "requested_at": entry.requested_at.0,
        "provider": entry.provider,
        "epoch": entry.epoch,
        "thread_id": entry.thread_id,
        "turn_id": entry.turn_id,
        "item_id": entry.item_id,
        "request_id": entry.request_id,
        "is_secret": entry.is_secret,
    })
}

pub fn project_artifact(artifact: &Artifact) -> serde_json::Value {
    serde_json::json!({
        "id": artifact.id.to_string(),
        "hash": artifact.hash.to_string(),
        "kind": artifact.kind,
        "path": artifact.path,
        "task_id": artifact.task_id.as_ref().map(|id| id.to_string()),
        "attempt_id": artifact.attempt_id.as_ref().map(|id| id.to_string()),
    })
}

/// Usage ledger with its observed/estimated/unknown split explicit:
/// estimates and missing finals are never presented as observed.
pub fn project_usage(ledger: &UsageLedger) -> serde_json::Value {
    serde_json::json!({
        "total_tokens": ledger.total_tokens,
        "total_cost_minor": ledger.total_cost_minor,
        "unknown": ledger.unknown,
        "observed_tokens": ledger.observed_tokens,
        "estimated_tokens": ledger.estimated_tokens,
        "observed_cost_minor": ledger.observed_cost_minor,
        "in_flight": ledger.in_flight.values().map(|missing| serde_json::json!({
            "key": missing.key,
            "provider": missing.provider,
            "epoch": missing.epoch,
            "metric": missing.metric,
            "source": missing.source,
            "noted_at": missing.noted_at.0,
        })).collect::<Vec<_>>(),
        "missing_final": ledger.missing_final.values().map(|missing| serde_json::json!({
            "key": missing.key,
            "provider": missing.provider,
            "epoch": missing.epoch,
            "metric": missing.metric,
            "source": missing.source,
            "noted_at": missing.noted_at.0,
        })).collect::<Vec<_>>(),
        "limits": ledger.limits,
        "audit": ledger.audit.iter().map(|entry| serde_json::json!({
            "missing_key": entry.missing_key,
            "note": entry.note,
            "by": entry.by.to_string(),
            "at": entry.at.0,
            "resolved_observed": entry.resolved_observed,
        })).collect::<Vec<_>>(),
    })
}

/// One event as `EventViewV1`: durable seq, run, kind, UTC-ms time and
/// the redacted payload. The kind comes from the serialized variant
/// name, so new variants project without handler changes; an unknown
/// kind can never appear because the store only holds `Event`.
pub fn project_event(seq: u64, event: &Event) -> serde_json::Value {
    let value = serde_json::to_value(event).unwrap_or(serde_json::Value::Null);
    let (kind, payload) = match value {
        serde_json::Value::Object(map) if map.len() == 1 => {
            let (kind, payload) = map.into_iter().next().expect("one entry");
            (kind, payload)
        }
        other => ("Unknown".to_string(), other),
    };
    serde_json::json!({
        "seq": seq,
        "run_id": event.run_id().to_string(),
        "kind": kind,
        "at": event.at().0,
        "payload": redact_value(payload),
    })
}

pub fn project_chat_message(message: &ChatMessage) -> serde_json::Value {
    serde_json::json!({
        "seq": message.seq,
        "run_id": message.run_id.to_string(),
        "author": message.author,
        "body": redact_text(&message.body),
        "at": message.at.0,
    })
}

pub fn project_profile(
    record: &UserProfileRecord,
    capability: serde_json::Value,
) -> serde_json::Value {
    let config: serde_json::Value =
        serde_json::from_str(&record.config_json).unwrap_or(serde_json::Value::Null);
    serde_json::json!({
        "profile_id": record.profile_id,
        "name": record.name,
        "provider": record.provider,
        "model": record.model,
        "effort": record.effort,
        "config": config,
        "updated_at": record.updated_at.0,
        "capability": capability,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secrets_are_scrubbed() {
        assert_eq!(
            redact_text("Authorization: Bearer abc123 ok"),
            "Authorization: Bearer [redacted] ok"
        );
        assert_eq!(redact_text("token=abc123 done"), "token=[redacted] done");
        assert_eq!(redact_text("nothing here"), "nothing here");
        let value = redact_value(serde_json::json!({"a": "Bearer xyz", "b": ["ok"]}));
        assert_eq!(value["a"], "Bearer [redacted]");
        assert_eq!(value["b"][0], "ok");
    }
}
