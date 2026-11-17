//! MSP harness request normalization for the inbox.
//!
//! Server requests (`approval/request`, `userInput/request`) become Codex
//! [`NormalizedRequest`](super::super::codex::NormalizedRequest) values —
//! the pending-request table, preview hashing and Once-answer scope are
//! shared harness plumbing — bound to an epoch-scoped
//! [`ExternalRequestKey`](central_application::contracts::ExternalRequestKey)
//! with `provider: "muse"`. Request ids are only unique inside one server
//! lifetime, so the instance epoch is part of every key.
//!
//! Previews are redacted and truncated at this boundary. The stored preview
//! hash is computed over the stored (redacted) text, so a durable intent can
//! never carry a secret payload or a content-derived hash of one.

use central_application::contracts::SessionRef;
use central_domain::{preview_hash_for, truncate_preview, InboxKind};
use serde_json::Value;

use crate::codex::stdio::BufferedEvent;
use crate::codex::wire::{is_secret_key_in, sanitize_text};
use crate::codex::{NormalizedRequest, SECRET_PREVIEW_PLACEHOLDER};

/// How much preview text survives normalization; the domain truncates again.
const NORMALIZE_PREVIEW_CHARS: usize = 4000;

/// Classify a server-request method. Unknown methods are not inbox traffic:
/// returning `None` keeps them as plain observations, never as answers.
pub fn method_kind(method: &str) -> Option<InboxKind> {
    match method {
        "approval/request" => Some(InboxKind::Approval),
        "userInput/request" => Some(InboxKind::Question),
        _ => None,
    }
}

fn text_field(params: &Value, names: &[&str]) -> Option<String> {
    let object = params.as_object()?;
    for name in names {
        if let Some(text) = object.get(*name).and_then(Value::as_str) {
            return Some(text.to_string());
        }
    }
    None
}

/// MSP session id: `sessionId` first, then the Codex spellings so shared
/// tooling keeps working.
pub fn session_field(params: &Value) -> String {
    text_field(params, &["sessionId", "threadId", "thread_id"]).unwrap_or_default()
}

fn turn_field(params: &Value) -> String {
    text_field(params, &["turnId", "turn_id"]).unwrap_or_default()
}

fn item_field(params: &Value) -> String {
    text_field(params, &["approvalId", "userInputId", "itemId", "item_id"]).unwrap_or_default()
}

/// Normalize one buffered server request. Returns `None` for notifications
/// and for methods that are not inbox traffic.
pub fn normalize_request(epoch: &str, event: &BufferedEvent) -> Option<NormalizedRequest> {
    if !event.is_request {
        return None;
    }
    let kind = method_kind(&event.method)?;
    let params = &event.params;

    let secret_declared = params
        .get("questions")
        .and_then(Value::as_array)
        .is_some_and(|questions| {
            questions.iter().any(|question| {
                question
                    .get("isSecret")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
            })
        });
    let scrubbed = is_secret_key_in(&event.params);
    let is_secret = secret_declared || scrubbed;

    let raw_preview = match kind {
        InboxKind::Approval => text_field(params, &["title", "command", "text", "prompt"])
            .unwrap_or_else(|| params.to_string()),
        InboxKind::Question => question_preview(params)
            .unwrap_or_else(|| text_field(params, &["prompt", "text"]).unwrap_or_default()),
    };
    let preview = if raw_preview.trim().is_empty() {
        SECRET_PREVIEW_PLACEHOLDER.to_string()
    } else {
        sanitize_text(&raw_preview).into_owned()
    };
    let preview = truncate_preview(&truncate_chars(&preview, NORMALIZE_PREVIEW_CHARS));
    let preview_hash = preview_hash_for(&preview);
    Some(NormalizedRequest {
        key: central_application::contracts::ExternalRequestKey {
            session: SessionRef {
                provider: "muse".to_string(),
                instance_epoch: epoch.to_string(),
                thread_id: session_field(params),
            },
            turn_id: turn_field(params),
            item_id: item_field(params),
            request_id: event
                .id
                .as_ref()
                .map(|id| id.to_string())
                .unwrap_or_default(),
        },
        kind,
        preview,
        preview_hash,
        is_secret,
    })
}

/// Human-readable rendering of a `userInput/request` question list: the
/// question text plus its option labels, so the approval UI shows what the
/// harness actually asked.
fn question_preview(params: &Value) -> Option<String> {
    let questions = params.get("questions")?.as_array()?;
    let mut parts = Vec::new();
    for question in questions {
        let text = question
            .get("question")
            .or_else(|| question.get("header"))
            .and_then(Value::as_str)
            .unwrap_or("(question)");
        let mut options = Vec::new();
        for option in question
            .get("options")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
        {
            let label = option.get("label").and_then(Value::as_str).unwrap_or("?");
            options.push(label.to_string());
        }
        if options.is_empty() {
            parts.push(text.to_string());
        } else {
            parts.push(format!("{} [{}]", text, options.join(" | ")));
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("; "))
    }
}

fn truncate_chars(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        text.to_string()
    } else {
        text.chars().take(max).collect()
    }
}

/// Build one drainable observation for a buffered event, bound to the
/// attempt that owns the session. Never fabricates answers: the outcome is
/// always `None`; resolution only confirms entries the user already replied
/// to.
pub fn observation_for(
    epoch: &str,
    attempt_id: central_domain::AttemptId,
    event: &BufferedEvent,
) -> central_domain::Observation {
    let session = session_field(&event.params);
    let item = item_field(&event.params);
    let id = event
        .id
        .as_ref()
        .map(|id| id.to_string())
        .unwrap_or_else(|| event.params.to_string().len().to_string());
    central_domain::Observation {
        attempt_id,
        provider_epoch: epoch.to_string(),
        dedup_key: format!("{}:{session}:{item}:{id}", event.method),
        outcome: None,
        detail: String::new(),
    }
}
