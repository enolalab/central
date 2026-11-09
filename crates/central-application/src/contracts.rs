use std::fmt;

use central_domain::{
    ActorId, AnswerPayload, AnswerScope, AttemptId, AttemptStatus, CommandEnvelope, CommandId,
    ContentHash, DomainError, Effect, Event, InboxKind, IntentId, ResolvedProfile, Revision, RunId,
    RunState, Timestamp,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Application-level error. Domain failures are mapped onto these variants;
/// storage and transport details never leak domain/SQL types upward.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AppError {
    Validation(String),
    Conflict(String),
    Forbidden(String),
    Storage(String),
    Capability(String),
    Transport(String),
    NotFound(String),
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Validation(msg) => write!(f, "validation error: {msg}"),
            Self::Conflict(msg) => write!(f, "conflict: {msg}"),
            Self::Forbidden(msg) => write!(f, "forbidden: {msg}"),
            Self::Storage(msg) => write!(f, "storage error: {msg}"),
            Self::Capability(msg) => write!(f, "capability error: {msg}"),
            Self::Transport(msg) => write!(f, "transport error: {msg}"),
            Self::NotFound(msg) => write!(f, "not found: {msg}"),
        }
    }
}

impl std::error::Error for AppError {}

impl From<DomainError> for AppError {
    fn from(error: DomainError) -> Self {
        match error {
            DomainError::Forbidden => Self::Forbidden(error.to_string()),
            DomainError::StaleRevision
            | DomainError::PlanHashMismatch
            | DomainError::StaleRequest
            | DomainError::RequestConflict(_) => Self::Conflict(error.to_string()),
            DomainError::UnknownPlan(_) | DomainError::UnknownRequest(_) => {
                Self::NotFound(error.to_string())
            }
            other => Self::Validation(other.to_string()),
        }
    }
}

/// Durable proof that a command was committed. Contains no credentials.
/// `payload_hash` lets a replay distinguish the same payload (return this
/// receipt) from a different payload reusing the command id (conflict).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Receipt {
    pub command_id: CommandId,
    pub run_id: RunId,
    pub revision: Revision,
    pub result: String,
    pub payload_hash: ContentHash,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutboxStatus {
    Pending,
    Dispatching,
    Completed,
    UnknownOutcome,
    Failed,
}

impl OutboxStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "Pending",
            Self::Dispatching => "Dispatching",
            Self::Completed => "Completed",
            Self::UnknownOutcome => "UnknownOutcome",
            Self::Failed => "Failed",
        }
    }

    pub fn from_status_str(value: &str) -> Result<Self, AppError> {
        match value {
            "Pending" => Ok(Self::Pending),
            "Dispatching" => Ok(Self::Dispatching),
            "Completed" => Ok(Self::Completed),
            "UnknownOutcome" => Ok(Self::UnknownOutcome),
            "Failed" => Ok(Self::Failed),
            other => Err(AppError::Storage(format!("unknown outbox status: {other}"))),
        }
    }
}

/// A persisted intention to perform an external effect. An intent is not
/// proof that the external effect happened; only dispatch evidence can move
/// it to `Completed`, and an ambiguous send becomes `UnknownOutcome`, never
/// `Completed`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboxIntent {
    pub id: IntentId,
    pub run_id: RunId,
    pub effect: Effect,
    pub status: OutboxStatus,
}

/// One atomic unit of persistence: snapshot, events, outbox intents and the
/// command receipt are written in a single transaction, or none of them are.
#[derive(Clone, Debug, PartialEq)]
pub struct CommitBatch {
    pub actor: ActorId,
    pub command_id: CommandId,
    pub payload_hash: ContentHash,
    pub expected_revision: Revision,
    pub next_state: RunState,
    pub events: Vec<Event>,
    pub intents: Vec<OutboxIntent>,
    pub receipt: Receipt,
}

/// Why a dispatch did not produce a usable external reference. A transport
/// loss after the send is `OutcomeUnknown` by default: Central has no
/// exactly-once guarantee from the outbox.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DispatchError {
    DefinitelyNotSent(String),
    OutcomeUnknown(String),
    Rejected(String),
}

impl fmt::Display for DispatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DefinitelyNotSent(msg) => write!(f, "definitely not sent: {msg}"),
            Self::OutcomeUnknown(msg) => write!(f, "dispatch outcome unknown: {msg}"),
            Self::Rejected(msg) => write!(f, "dispatch rejected: {msg}"),
        }
    }
}

impl std::error::Error for DispatchError {}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    pub provider: String,
    pub supports_resume: bool,
    pub features: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRef {
    pub provider: String,
    pub instance_epoch: String,
    pub thread_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnRef {
    pub session: SessionRef,
    pub turn_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalRequestKey {
    pub session: SessionRef,
    pub turn_id: String,
    pub item_id: String,
    pub request_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenSession {
    pub attempt_id: AttemptId,
    pub profile: ResolvedProfile,
    pub workspace: String,
    pub resume_from: Option<SessionRef>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SendTurn {
    pub session: SessionRef,
    pub attempt_id: AttemptId,
    pub input: String,
    pub output_schema: Option<String>,
    pub command_id: CommandId,
}

/// One answer to an external harness request. The scope is always
/// [`AnswerScope::Once`]: a reply is consumed by a single send and never
/// replayed. A `Secret` payload here is already resolved to plain text by the
/// caller — the durable intent is what carries the memory-only reference.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalAnswer {
    pub key: ExternalRequestKey,
    pub payload: AnswerPayload,
    pub scope: AnswerScope,
}

/// One normalized harness request waiting for a user reply. The preview is
/// redacted and truncated at the source; secret-bearing requests carry a
/// placeholder preview and `is_secret`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalRequest {
    pub key: ExternalRequestKey,
    pub kind: InboxKind,
    pub preview: String,
    pub preview_hash: ContentHash,
    pub attempt_id: Option<AttemptId>,
    pub is_secret: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionRef {
    pub attempt_id: AttemptId,
    pub session: SessionRef,
    pub turn: Option<TurnRef>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelObservation {
    pub cancelled: bool,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconcileEvidence {
    pub attempt_id: AttemptId,
    pub status: AttemptStatus,
    pub detail: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DispatchSummary {
    pub dispatched: usize,
    pub unknown: usize,
    pub still_pending: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoverySummary {
    pub runs_seen: usize,
    pub awaiting_resume: usize,
    pub unknown_outcome: usize,
}

/// Identity of a supervised OS process. PID alone is not enough on Windows:
/// the creation time and ownership/job id disambiguate a recycled PID. No OS
/// handle appears in application contracts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessIdentity {
    pub pid: u32,
    pub created_at: Timestamp,
    pub ownership_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessSpec {
    pub program: String,
    pub args: Vec<String>,
    pub working_dir: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProcessState {
    Running,
    Exited(i32),
    Unknown,
}

/// Canonical payload hash for command-id deduplication: SHA-256 of the
/// canonical JSON of the envelope's expected revision and command (the
/// command id itself is the dedup key, not part of the payload).
pub fn command_payload_hash(envelope: &CommandEnvelope) -> Result<ContentHash, AppError> {
    let payload = serde_json::json!({
        "expectedRevision": envelope.expected_revision,
        "command": envelope.command,
    });
    let canonical = serde_json::to_string(&payload)
        .map_err(|e| AppError::Validation(format!("cannot hash command payload: {e}")))?;
    let digest = Sha256::digest(canonical.as_bytes());
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        hex.push_str(&format!("{byte:02x}"));
    }
    Ok(ContentHash::from_hex(hex))
}
