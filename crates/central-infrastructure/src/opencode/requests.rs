//! opencode permission-request normalization for the inbox.
//!
//! Open permission requests (`GET /session/{id}/permission`, items shaped
//! like `Permission.Request`) become Codex
//! [`NormalizedRequest`](super::super::codex::NormalizedRequest) values —
//! the pending-request table, preview hashing and Once-answer scope are
//! shared harness plumbing — bound to an epoch-scoped
//! [`ExternalRequestKey`](central_application::contracts::ExternalRequestKey)
//! with `provider: "opencode"`. opencode has no turn concept on the
//! permission itself, so the turn member stays empty and the permission id
//! is both the item and the durable anchor.
//!
//! Previews are redacted and truncated at this boundary. The stored preview
//! hash is computed over the stored (redacted) text, so a durable intent can
//! never carry a secret payload or a content-derived hash of one.

use central_application::contracts::SessionRef;
use central_domain::{preview_hash_for, truncate_preview, InboxKind};
use serde_json::Value;

use crate::codex::wire::sanitize_text;
use crate::codex::{NormalizedRequest, SECRET_PREVIEW_PLACEHOLDER};

/// How much preview text survives normalization; the domain truncates again.
const NORMALIZE_PREVIEW_CHARS: usize = 4000;

/// opencode permissions are the only inbox traffic: every open request is
/// an approval (allow-once or reject), never a free-text question.
pub fn method_kind(method: &str) -> Option<InboxKind> {
    match method {
        "session.permission" => Some(InboxKind::Approval),
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

/// Normalize one open permission request. Returns `None` for shapes that
/// are not inbox traffic.
pub fn normalize_request(
    epoch: &str,
    session_id: &str,
    request: &Value,
) -> Option<NormalizedRequest> {
    let kind = method_kind("session.permission")?;
    let id = text_field(request, &["id"])?;
    let action = text_field(request, &["action"]).unwrap_or_default();
    let resources: Vec<String> = request
        .get("resources")
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    let message = text_field(request, &["message"]).unwrap_or_default();

    let mut raw_preview = action.clone();
    if !resources.is_empty() {
        raw_preview.push_str(&format!(" [{}]", resources.join(", ")));
    }
    if !message.is_empty() {
        raw_preview.push_str(&format!(" — {message}"));
    }
    let preview = if raw_preview.trim().is_empty() {
        SECRET_PREVIEW_PLACEHOLDER.to_string()
    } else {
        sanitize_text(&raw_preview).into_owned()
    };
    let preview = truncate_preview(&truncate_chars(&preview, NORMALIZE_PREVIEW_CHARS));
    let preview_hash = preview_hash_for(&preview);
    let scrubbed = crate::codex::wire::is_secret_key_in(request);
    Some(NormalizedRequest {
        key: central_application::contracts::ExternalRequestKey {
            session: SessionRef {
                provider: "opencode".to_string(),
                instance_epoch: epoch.to_string(),
                thread_id: session_id.to_string(),
            },
            turn_id: String::new(),
            item_id: id,
            request_id: "permission".to_string(),
        },
        kind,
        preview,
        preview_hash,
        is_secret: scrubbed,
    })
}

/// Build one drainable observation for a normalized permission request.
/// Never fabricates answers: the outcome is always `None`.
pub fn observation_for(
    epoch: &str,
    attempt_id: central_domain::AttemptId,
    session_id: &str,
    item_id: &str,
) -> central_domain::Observation {
    central_domain::Observation {
        attempt_id,
        provider_epoch: epoch.to_string(),
        dedup_key: format!("session.permission:{session_id}:{item_id}"),
        outcome: None,
        detail: String::new(),
    }
}

fn truncate_chars(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        text.to_string()
    } else {
        text.chars().take(max).collect()
    }
}
