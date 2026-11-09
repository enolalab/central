//! Durable approval/question inbox (P2 Task 3).
//!
//! External harness requests (tool approvals, user-input questions) live in
//! [`RunState`](crate::RunState) as [`InboxEntry`] values keyed by a canonical
//! string built from the request's instance epoch, thread, turn, item and
//! request ids. Request ids are only unique inside one server lifetime, so
//! the epoch is part of the key: a restart that reuses request id `0`
//! produces a *different* entry, and the old one goes [`InboxStatus::Stale`].
//!
//! A reply is scoped twice: it names the canonical key (epoch included) and
//! the immutable [`InboxEntry::preview_hash`] of the exact text the user
//! saw. An accept for one request never applies to another, even with
//! identical text. Only [`InboxStatus::Pending`] entries answer; resolved or
//! stale entries refuse with [`DomainError::StaleRequest`].
//!
//! Secret answers are memory-only: [`VolatileAnswerRef`] carries a random id
//! that resolves through an application-side vault. The durable command,
//! event, intent and receipt hold the reference only — never the payload and
//! never a content-derived hash — so a restart forces re-entry instead of
//! replaying a credential.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{ActorId, AttemptId, ContentHash, Observation, Timestamp};

/// Lifecycle of one inbox entry. Pending answers, Answered awaits harness
/// confirmation, Resolved is done, Stale demands a fresh verified request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum InboxStatus {
    Pending,
    Answered,
    Resolved,
    Stale,
}

/// What the harness is waiting for: an action it must not take without
/// approval, or a question it cannot answer itself. Plan approval is NOT an
/// inbox kind: plans approve through `Command::ApprovePlan`, and a plan hash
/// never matches an inbox key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum InboxKind {
    Approval,
    Question,
}

/// Outcome recorded when a pending entry is answered.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum InboxOutcome {
    Approved,
    Declined,
    Answered,
}

/// Scope of an external answer. Answers are single-use by construction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AnswerScope {
    Once,
}

/// Memory-only reference to a secret answer. The id is random per answer and
/// carries no content: it resolves through an application-side vault that is
/// never persisted, so a restart drops it and the user must re-enter.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct VolatileAnswerRef(pub String);

impl VolatileAnswerRef {
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Payload of an external answer. `Secret` holds a random
/// [`VolatileAnswerRef`], never the credential and never a hash of it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AnswerPayload {
    Plain(String),
    Secret(VolatileAnswerRef),
}

/// Epoch-scoped identity of one harness request. The canonical string is the
/// inbox map key; the epoch keeps a restarted harness from colliding with
/// the requests of its previous lifetime.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InboxKey {
    pub provider: String,
    pub epoch: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub request_id: String,
}

impl InboxKey {
    pub fn canonical(&self) -> String {
        canonical_key(
            &self.provider,
            &self.epoch,
            &self.thread_id,
            &self.turn_id,
            &self.item_id,
            &self.request_id,
        )
    }
}

pub fn canonical_key(
    provider: &str,
    epoch: &str,
    thread_id: &str,
    turn_id: &str,
    item_id: &str,
    request_id: &str,
) -> String {
    format!("{provider}:{epoch}:{thread_id}:{turn_id}:{item_id}:{request_id}")
}

/// One normalized harness request, as carried by
/// `Command::RecordExternalRequest`. The preview is already truncated and
/// redacted by the caller; for secret-bearing requests it is a placeholder.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InboxRequest {
    pub key: InboxKey,
    pub kind: InboxKind,
    pub preview: String,
    pub preview_hash: ContentHash,
    pub attempt_id: Option<AttemptId>,
    pub is_secret: bool,
}

/// One durable inbox entry. `preview_hash`, `preview`, `requested_by` and
/// `requested_at` are immutable after insert: only `status` moves, through
/// Pending -> Answered -> Resolved, or to Stale on epoch rotation/stop.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InboxEntry {
    pub key: String,
    pub attempt_id: Option<AttemptId>,
    pub kind: InboxKind,
    pub status: InboxStatus,
    pub preview_hash: ContentHash,
    pub preview: String,
    pub requested_by: ActorId,
    pub requested_at: Timestamp,
    pub provider: String,
    pub epoch: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub request_id: String,
    pub is_secret: bool,
}

/// Whether one harness observation is evidence for one inbox entry.
/// Correlation is attempt plus epoch plus item: the entry must belong to the
/// observation's attempt and epoch, and the observation's dedup key must name
/// the entry's item (the `requestApproval:{item_id}` keys in the fixtures, or
/// the `method:thread:item:id` keys built by `observation_for`). An unrelated
/// notification for the same attempt never matches, so a progress ping can
/// never resolve a safety-critical approval without harness evidence for it.
pub fn observation_matches_entry(observation: &Observation, entry: &InboxEntry) -> bool {
    if entry.attempt_id != Some(observation.attempt_id) {
        return false;
    }
    if entry.epoch != observation.provider_epoch {
        return false;
    }
    !entry.item_id.is_empty() && observation.dedup_key.contains(&entry.item_id)
}

/// SHA-256 hex of the exact preview text the user saw. Callers hash the
/// stored (already redacted) preview, so the hash of a secret-bearing
/// request is derived from its placeholder, never from the credential.
pub fn preview_hash_for(preview: &str) -> ContentHash {
    let digest = Sha256::digest(preview.as_bytes());
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        hex.push_str(&format!("{byte:02x}"));
    }
    ContentHash::from_hex(hex)
}

/// Maximum preview text kept on an entry; the wire copy stays with the
/// harness, Central keeps what the user saw.
pub const MAX_PREVIEW_CHARS: usize = 2000;

pub fn truncate_preview(preview: &str) -> String {
    if preview.chars().count() <= MAX_PREVIEW_CHARS {
        preview.to_string()
    } else {
        preview.chars().take(MAX_PREVIEW_CHARS).collect()
    }
}
