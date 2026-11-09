//! Harness request normalization for the inbox (P2 Task 3).
//!
//! Server requests (`item/commandExecution/requestApproval`,
//! `item/tool/requestUserInput`) become [`NormalizedRequest`] values bound to
//! an epoch-scoped [`ExternalRequestKey`](central_application::contracts::ExternalRequestKey).
//! Request ids are only unique inside one server lifetime, so the instance
//! epoch is part of every key: a restart that reuses request id `0`
//! normalizes to a *different* key, and the old entry goes stale instead of
//! accepting.
//!
//! Previews are redacted and truncated at this boundary. The stored preview
//! hash is computed over the stored (redacted) text, so a durable intent can
//! never carry a secret payload or a content-derived hash of one.

use std::collections::HashMap;

use central_application::contracts::{AppError, ExternalRequest, ExternalRequestKey, SessionRef};
use central_domain::{canonical_key, preview_hash_for, truncate_preview, ContentHash, InboxKind};
use serde_json::Value;

use super::stdio::BufferedEvent;
use super::wire::sanitize_text;

/// Placeholder stored when the harness request itself carries secrets.
/// Never the credential; the hash of this placeholder is what persists.
pub const SECRET_PREVIEW_PLACEHOLDER: &str = "[redacted-secret]";

/// How much preview text survives normalization; the domain truncates again.
const NORMALIZE_PREVIEW_CHARS: usize = 4000;

/// One normalized harness request, still gateway-side: [`crate::CodexGateway`]
/// holds these in a [`PendingRequestTable`] until the application drains and
/// persists them through `Command::RecordExternalRequest`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NormalizedRequest {
    pub key: ExternalRequestKey,
    pub kind: InboxKind,
    pub preview: String,
    pub preview_hash: ContentHash,
    pub is_secret: bool,
}

/// Classify a server-request method. Unknown methods are not inbox traffic:
/// returning `None` keeps them as plain observations, never as answers.
pub fn method_kind(method: &str) -> Option<InboxKind> {
    match method {
        "item/commandExecution/requestApproval" => Some(InboxKind::Approval),
        "item/tool/requestUserInput" => Some(InboxKind::Question),
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

/// Normalize one buffered server request. Returns `None` for notifications
/// and for methods that are not inbox traffic.
pub fn normalize_request(epoch: &str, event: &BufferedEvent) -> Option<NormalizedRequest> {
    if !event.is_request {
        return None;
    }
    let kind = method_kind(&event.method)?;
    let params = &event.params;
    let thread_id = text_field(params, &["threadId", "thread_id"]).unwrap_or_default();
    let turn_id = text_field(params, &["turnId", "turn_id"]).unwrap_or_default();
    let item_id = text_field(params, &["itemId", "item_id"]).unwrap_or_default();
    let request_id = event
        .id
        .as_ref()
        .map(|id| id.to_string())
        .unwrap_or_default();

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
    let scrubbed = super::wire::is_secret_key_in(&event.params);
    let is_secret = secret_declared || scrubbed;

    let raw_preview = match kind {
        InboxKind::Approval => {
            text_field(params, &["command", "text", "prompt"]).unwrap_or_else(|| params.to_string())
        }
        InboxKind::Question => question_preview(params)
            .unwrap_or_else(|| text_field(params, &["prompt", "text"]).unwrap_or_default()),
    };
    let preview = if raw_preview.trim().is_empty() {
        SECRET_PREVIEW_PLACEHOLDER.to_string()
    } else if is_secret && secret_declared && matches!(kind, InboxKind::Question) {
        // The question text stays visible so the user can answer it; only
        // the *answer* travels the secret path. Scrub embedded secrets from
        // the visible text without hiding the question itself.
        sanitize_text(&raw_preview).into_owned()
    } else {
        sanitize_text(&raw_preview).into_owned()
    };
    let preview = truncate_preview(&truncate_chars(&preview, NORMALIZE_PREVIEW_CHARS));
    let preview_hash = preview_hash_for(&preview);
    Some(NormalizedRequest {
        key: ExternalRequestKey {
            session: SessionRef {
                provider: "codex".to_string(),
                instance_epoch: epoch.to_string(),
                thread_id,
            },
            turn_id,
            item_id,
            request_id,
        },
        kind,
        preview,
        preview_hash,
        is_secret,
    })
}

/// Human-readable rendering of a `requestUserInput` question list: the
/// question text plus its option labels, so the approval UI shows what the
/// harness actually asked.
fn question_preview(params: &Value) -> Option<String> {
    let questions = params.get("questions")?.as_array()?;
    let mut parts = Vec::new();
    for question in questions {
        let text = question
            .get("question")
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
            let description = option
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or("");
            if description.is_empty() {
                options.push(label.to_string());
            } else {
                options.push(format!("{label}: {description}"));
            }
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
    let thread = text_field(&event.params, &["threadId", "thread_id"]).unwrap_or_default();
    let item = text_field(&event.params, &["itemId", "item_id"]).unwrap_or_default();
    let id = event
        .id
        .as_ref()
        .map(|id| id.to_string())
        .unwrap_or_else(|| event.params.to_string().len().to_string());
    central_domain::Observation {
        attempt_id,
        provider_epoch: epoch.to_string(),
        dedup_key: format!("{}:{thread}:{item}:{id}", event.method),
        outcome: None,
    }
}

/// Lifecycle of a gateway-side request: seen on the wire, drained into the
/// durable inbox, then answered exactly once.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RequestState {
    Seen,
    Drained,
    Answered,
}

#[derive(Debug)]
struct TableEntry {
    request: NormalizedRequest,
    state: RequestState,
}

/// Gateway-side pending requests. Recording is idempotent (a replay keeps
/// the first copy), draining binds the owning attempt, and answering enforces
/// the Once scope: an already-answered key is refused, never resent.
#[derive(Debug, Default)]
pub struct PendingRequestTable {
    inner: HashMap<String, TableEntry>,
}

impl PendingRequestTable {
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn canonical(request: &NormalizedRequest) -> String {
        let key = &request.key;
        canonical_key(
            &key.session.provider,
            &key.session.instance_epoch,
            &key.session.thread_id,
            &key.turn_id,
            &key.item_id,
            &key.request_id,
        )
    }

    /// Record a normalized request. A replay (same canonical key) keeps the
    /// first copy: one entry per request, never a duplicate.
    pub fn record(&mut self, request: NormalizedRequest) {
        let canonical = Self::canonical(&request);
        self.inner.entry(canonical).or_insert(TableEntry {
            request,
            state: RequestState::Seen,
        });
    }

    /// Drain `Seen` entries for one thread into application requests bound
    /// to the owning attempt. Drained entries leave `Seen` so an answer can
    /// only route after Central persisted the entry.
    pub fn drain_for_thread(
        &mut self,
        thread_id: &str,
        attempt_id: Option<central_domain::AttemptId>,
    ) -> Vec<ExternalRequest> {
        let mut drained = Vec::new();
        for entry in self.inner.values_mut() {
            if entry.state == RequestState::Seen && entry.request.key.session.thread_id == thread_id
            {
                entry.state = RequestState::Drained;
                drained.push(ExternalRequest {
                    key: entry.request.key.clone(),
                    kind: entry.request.kind,
                    preview: entry.request.preview.clone(),
                    preview_hash: entry.request.preview_hash.clone(),
                    attempt_id,
                    is_secret: entry.request.is_secret,
                });
            }
        }
        drained
    }

    /// Consume the single allowed answer for a persisted request.
    pub fn consume_answer(&mut self, key: &ExternalRequestKey) -> Result<(), AppError> {
        let canonical = canonical_key(
            &key.session.provider,
            &key.session.instance_epoch,
            &key.session.thread_id,
            &key.turn_id,
            &key.item_id,
            &key.request_id,
        );
        match self.inner.get_mut(&canonical) {
            None => Err(AppError::Conflict(format!(
                "unknown external request: {canonical}"
            ))),
            Some(entry) if entry.state == RequestState::Seen => Err(AppError::Conflict(
                "request is not persisted yet; reconcile the inbox before answering".to_string(),
            )),
            Some(entry) if entry.state == RequestState::Answered => Err(AppError::Conflict(
                "request was already answered once; the scope does not allow a second reply"
                    .to_string(),
            )),
            Some(entry) => {
                entry.state = RequestState::Answered;
                Ok(())
            }
        }
    }
}
