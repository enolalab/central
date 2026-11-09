//! Structured leader bridge (P2 Task 4): chat stays chat, proposals take
//! the schema-validated channel.
//!
//! [`LeaderService::message`] is the common gate for leader text about one
//! run. Plain chat — including markdown-fenced JSON — is returned as
//! [`ExecutionSegment::Chat`] and mutates nothing: Central never scrapes
//! markdown JSON to change state. Only a strict [`LeaderReplyV1`] document
//! (correct version, no unknown fields, embedded plan passing domain
//! validation) commits a `ProposePlan` command as the leader actor, and even
//! then the plan is only *pending*: it becomes active solely through the
//! user's hash approval (`ApprovePlan` with the exact hash).
//!
//! Agent bridge tokens ([`AgentBridgeToken`]) scope one harness call to
//! propose-or-report for exactly one run (and, for reports, one attempt).
//! Tokens never approve: [`LeaderService::approve_with_token`] always
//! answers 403 and mutates nothing. Scope-changing leader answers still need
//! a user-approved revision; the service never invents one.

use central_domain::{Actor, Command, CommandEnvelope, PlanDocumentV1, PlanHash, RunId};
use serde::{Deserialize, Serialize};

use crate::contracts::AppError;
use crate::handover::{accept_worker_result, parse_worker_result};
use crate::Runtime;

/// Schema identity for leader replies (see
/// `contracts/leader-reply-v1.schema.json`).
pub const LEADER_REPLY_SCHEMA: &str = "central/leader-reply-v1";
/// The only accepted leader-reply version. Anything else is unknown schema.
pub const LEADER_REPLY_VERSION: u32 = 1;

/// Structured leader reply: a message plus, optionally, a plan proposal.
/// The proposal is validated by domain `validate_plan` on commit; this type
/// only gates the envelope (version, unknown fields, empty message).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LeaderReplyV1 {
    pub version: u32,
    pub message: String,
    pub proposal: Option<PlanDocumentV1>,
}

/// What one leader message turned into: inert chat, or a pending proposal.
/// There is no `Approved` segment: approval is always the user's hash.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutionSegment {
    Chat { text: String },
    Proposal { hash: PlanHash },
}

/// What one agent harness call may do. Constructed only through the scoped
/// constructors below: proposal-only or report-only, bound to exactly one
/// run and (for reports) one attempt. There is no approve capability to set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentBridgeToken {
    pub run_id: RunId,
    pub attempt_id: Option<central_domain::AttemptId>,
    pub allow_propose: bool,
    pub allow_report: bool,
}

impl AgentBridgeToken {
    /// A token that may propose plans for exactly this run.
    pub fn proposal_only(run_id: RunId, attempt_id: central_domain::AttemptId) -> Self {
        Self {
            run_id,
            attempt_id: Some(attempt_id),
            allow_propose: true,
            allow_report: false,
        }
    }

    /// A token that may report results for exactly this run and attempt.
    pub fn report_only(run_id: RunId, attempt_id: central_domain::AttemptId) -> Self {
        Self {
            run_id,
            attempt_id: Some(attempt_id),
            allow_propose: false,
            allow_report: true,
        }
    }
}

/// The answer to one bridge call, with the HTTP-style status the daemon
/// surfaces: 200 applied, 403 the token may not do that, 422 the payload is
/// schema-valid but not acceptable for this run/attempt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BridgeResponse {
    pub status: u16,
    pub detail: String,
}

/// The leader bridge for one run. The runtime already owns the run id; every
/// method re-checks the caller-supplied run against it so a token minted for
/// another run cannot act here.
pub struct LeaderService {
    runtime: Runtime,
    leader: Actor,
}

impl LeaderService {
    pub fn new(runtime: Runtime, leader: Actor) -> Self {
        Self { runtime, leader }
    }

    pub fn runtime(&self) -> &Runtime {
        &self.runtime
    }

    fn check_run(&self, run: RunId) -> Result<(), AppError> {
        if run != self.runtime.run_id() {
            return Err(AppError::Forbidden(format!(
                "bridge call targets run {run} but this service serves run {}",
                self.runtime.run_id(),
            )));
        }
        Ok(())
    }

    /// Route one leader text through the common gate. Returns chat without
    /// touching state unless the whole text is a strict, schema-valid
    /// [`LeaderReplyV1`] carrying a proposal — markdown-fenced or otherwise
    /// embedded JSON is never extracted. A committed proposal stays pending;
    /// `active_plan` is untouched until the user approves the exact hash.
    pub async fn message(&self, run: RunId, text: String) -> Result<ExecutionSegment, AppError> {
        self.check_run(run)?;
        let trimmed = text.trim();
        // Not a JSON document at all: chat, no mutation.
        if !trimmed.starts_with('{') {
            return Ok(ExecutionSegment::Chat { text });
        }
        let reply: LeaderReplyV1 = serde_json::from_str(trimmed)
            .map_err(|error| AppError::Validation(format!("malformed leader reply: {error}")))?;
        if reply.version != LEADER_REPLY_VERSION {
            return Err(AppError::Validation(format!(
                "unknown {LEADER_REPLY_SCHEMA} version {}; Central supports version {LEADER_REPLY_VERSION}",
                reply.version,
            )));
        }
        if reply.message.trim().is_empty() {
            return Err(AppError::Validation(
                "leader reply message must not be empty".to_string(),
            ));
        }
        let Some(plan) = reply.proposal else {
            return Ok(ExecutionSegment::Chat {
                text: reply.message,
            });
        };
        let envelope = CommandEnvelope {
            id: self.runtime_ids_next(),
            expected_revision: None,
            command: Command::ProposePlan { plan },
        };
        self.runtime.execute(self.leader.clone(), envelope).await?;
        let state = self
            .runtime
            .store()
            .load(run)
            .await?
            .ok_or_else(|| AppError::Storage(format!("run {run} vanished after propose")))?;
        let hash = state.pending_plan.clone().ok_or_else(|| {
            AppError::Storage("proposal committed but no pending plan is stored".to_string())
        })?;
        Ok(ExecutionSegment::Proposal { hash })
    }

    /// Propose through an agent token: the token must be proposal-scoped for
    /// exactly this run, and the payload must be a strict leader reply with
    /// a proposal. Anything else is refused without mutation.
    pub async fn propose_with_token(
        &self,
        token: &AgentBridgeToken,
        run: RunId,
        text: String,
    ) -> Result<BridgeResponse, AppError> {
        self.check_run(run)?;
        if token.run_id != run || !token.allow_propose {
            return Err(AppError::Forbidden(
                "agent token is not scoped to propose for this run".to_string(),
            ));
        }
        match self.message(run, text).await? {
            ExecutionSegment::Proposal { hash } => Ok(BridgeResponse {
                status: 200,
                detail: format!("proposal pending under hash {hash}"),
            }),
            ExecutionSegment::Chat { .. } => Err(AppError::Validation(
                "agent proposal needs a schema-valid leader reply carrying a proposal".to_string(),
            )),
        }
    }

    /// Report through an agent token: the token must be report-scoped for
    /// exactly this run and attempt. The report is schema-validated and
    /// checked against the loaded run (same membership gate as the CLI
    /// import path); committing it belongs to P2/T5.
    pub async fn report_with_token(
        &self,
        token: &AgentBridgeToken,
        run: RunId,
        text: &str,
    ) -> Result<BridgeResponse, AppError> {
        self.check_run(run)?;
        if token.run_id != run || !token.allow_report {
            return Err(AppError::Forbidden(
                "agent token is not scoped to report for this run".to_string(),
            ));
        }
        let result = parse_worker_result(text)?;
        let Some(attempt) = token.attempt_id else {
            return Err(AppError::Forbidden(
                "report token names no attempt".to_string(),
            ));
        };
        if result.attempt_id != attempt {
            return Err(AppError::Forbidden(format!(
                "report token covers attempt {attempt} but the report names {}",
                result.attempt_id,
            )));
        }
        let state = self
            .runtime
            .store()
            .load(run)
            .await?
            .ok_or_else(|| AppError::Storage(format!("run {run} has no loaded state")))?;
        accept_worker_result(&state, &result)?;
        Ok(BridgeResponse {
            status: 200,
            detail: format!(
                "report accepted for attempt {} (candidate {})",
                attempt,
                result.candidate_hash.as_str(),
            ),
        })
    }

    /// Approve through an agent token: always refused. Tokens propose and
    /// report; only the user approves a plan hash. The 403 mutates nothing.
    pub fn approve_with_token(
        &self,
        _token: &AgentBridgeToken,
        _run: RunId,
        _plan_hash: &PlanHash,
    ) -> BridgeResponse {
        BridgeResponse {
            status: 403,
            detail: "agent bridge tokens cannot approve plans; only the user approves a plan hash"
                .to_string(),
        }
    }

    fn runtime_ids_next(&self) -> central_domain::CommandId {
        self.runtime.ids().next_command_id()
    }
}
