//! Structured worker/verifier reports and session handovers (P2 Task 4).
//!
//! Three schema-validated DTOs, no markdown scraping:
//!
//! * [`WorkerResultV1`]: what one attempt produced (hashes and run-relative
//!   paths, never approval). [`accept_worker_result`] checks the report
//!   against the loaded run before anything downstream may use it: the
//!   attempt must belong to this run, and the candidate path must stay
//!   inside the run (relative, no parent escapes).
//! * [`VerificationV1`]: an independent verdict bound to an exact subject
//!   hash and criteria hash. Parsing only validates the envelope; judging
//!   candidates belongs to P2/T5.
//! * [`HandoverV1`]: what a new session needs to continue. [`build_handover`]
//!   inspects evidence *before* writing: a claim lands in `verified_facts`
//!   only when its evidence id matches an observation of the predecessor
//!   attempt or an artifact hash in the same handover; everything else stays
//!   in `unverified_notes`. Self-reported prose without evidence is never
//!   promoted.

use std::collections::BTreeMap;

use central_domain::{AttemptId, ContentHash, Observation, RunState, VerificationOutcome};
use serde::{Deserialize, Serialize};

use crate::contracts::AppError;

/// Schema identity for worker reports (see
/// `contracts/worker-result-v1.schema.json`).
pub const WORKER_RESULT_SCHEMA: &str = "central/worker-result-v1";
/// Schema identity for verifier verdicts (see
/// `contracts/verification-v1.schema.json`).
pub const VERIFICATION_SCHEMA: &str = "central/verification-v1";
/// Schema identity for session handovers (no standalone file: the shape is
/// fixed by [`HandoverV1`] and validated claim-by-claim in [`build_handover`]).
pub const HANDOVER_SCHEMA: &str = "central/handover-v1";

/// The only accepted schema versions. Anything else is an unknown schema,
/// never a best-effort parse.
pub const WORKER_RESULT_VERSION: u32 = 1;
pub const VERIFICATION_VERSION: u32 = 1;
pub const HANDOVER_VERSION: u32 = 1;

/// Structured report of what one attempt produced.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerResultV1 {
    pub version: u32,
    pub attempt_id: AttemptId,
    pub input_hash: ContentHash,
    pub candidate_path: String,
    pub candidate_hash: ContentHash,
    pub evidence_paths: Vec<String>,
    pub summary: String,
}

/// Independent verdict bound to an exact subject and exact criteria.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VerificationV1 {
    pub version: u32,
    pub subject_hash: ContentHash,
    pub criteria_hash: ContentHash,
    pub outcome: VerificationOutcome,
    pub evidence_ids: Vec<String>,
    pub findings: String,
}

/// One artifact carried by a handover: a run-relative path plus its hash.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HandoverArtifact {
    pub path: String,
    pub hash: ContentHash,
}

/// What a new session needs to continue after this attempt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HandoverV1 {
    pub version: u32,
    pub predecessor: AttemptId,
    pub input: String,
    pub artifacts: Vec<HandoverArtifact>,
    pub verified_facts: Vec<String>,
    pub unverified_notes: Vec<String>,
    pub next_step: String,
}

/// One claim the predecessor session hands over, with the evidence id that
/// backs it (an observation `dedup_key` or an artifact hash), if any.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HandoverClaim {
    pub text: String,
    pub evidence_id: Option<String>,
}

/// Evidence inspected *before* a handover is written: the predecessor
/// attempt's observations and the content hashes of its artifacts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HandoverEvidence<'a> {
    pub observations: &'a [Observation],
    pub artifact_hashes: &'a [ContentHash],
}

fn reject_version(schema: &str, version: u32, supported: u32) -> Result<(), AppError> {
    if version != supported {
        return Err(AppError::Validation(format!(
            "unknown {schema} version {version}; Central supports version {supported}"
        )));
    }
    Ok(())
}

/// A candidate path is only accepted when it stays inside the run:
/// relative, non-empty, without parent escapes or drive/root prefixes.
fn check_candidate_path(path: &str) -> Result<(), AppError> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err(AppError::Validation(
            "candidatePath must not be empty".to_string(),
        ));
    }
    let normalized = trimmed.replace('\\', "/");
    if normalized.starts_with('/')
        || normalized.split('/').any(|segment| segment == "..")
        || normalized.contains(':')
    {
        return Err(AppError::Validation(format!(
            "candidatePath escapes the run root: {trimmed}"
        )));
    }
    Ok(())
}

/// Strictly parse a worker report: schema version, unknown fields, empty
/// summary and escaping paths are all rejected. The attempt's membership in
/// this run is checked separately by [`accept_worker_result`] against the
/// loaded state, never from the report itself.
pub fn parse_worker_result(text: &str) -> Result<WorkerResultV1, AppError> {
    let result: WorkerResultV1 = serde_json::from_str(text)
        .map_err(|error| AppError::Validation(format!("malformed worker result: {error}")))?;
    reject_version(WORKER_RESULT_SCHEMA, result.version, WORKER_RESULT_VERSION)?;
    if result.summary.trim().is_empty() {
        return Err(AppError::Validation(
            "worker result summary must not be empty".to_string(),
        ));
    }
    check_candidate_path(&result.candidate_path)?;
    for path in &result.evidence_paths {
        check_candidate_path(path)?;
    }
    Ok(result)
}

/// Strictly parse a verifier verdict envelope. `outcome` must be one of
/// pass/fail/blocked; judging the candidate itself belongs to P2/T5.
pub fn parse_verification(text: &str) -> Result<VerificationV1, AppError> {
    let verdict: VerificationV1 = serde_json::from_str(text)
        .map_err(|error| AppError::Validation(format!("malformed verification: {error}")))?;
    reject_version(VERIFICATION_SCHEMA, verdict.version, VERIFICATION_VERSION)?;
    if verdict.findings.trim().is_empty() {
        return Err(AppError::Validation(
            "verification findings must not be empty".to_string(),
        ));
    }
    Ok(verdict)
}

/// Check a parsed worker report against the loaded run before anything
/// downstream uses it: the attempt must belong to this run (a foreign
/// candidate is refused), and the paths must still be contained.
pub fn accept_worker_result(state: &RunState, result: &WorkerResultV1) -> Result<(), AppError> {
    if !state.attempts.contains_key(&result.attempt_id) {
        return Err(AppError::Validation(format!(
            "foreign candidate: attempt {} does not belong to run {}",
            result.attempt_id, state.id,
        )));
    }
    check_candidate_path(&result.candidate_path)?;
    Ok(())
}

/// Build a handover from inspected evidence. A claim is verified only when
/// its evidence id matches an observation of the predecessor attempt or an
/// artifact hash carried by this handover; every other claim — including a
/// bare claim with no evidence id at all — stays in `unverified_notes`.
/// Nothing here writes to the store: the caller persists the returned value.
pub fn build_handover(
    predecessor: AttemptId,
    input: String,
    claims: Vec<HandoverClaim>,
    artifacts: Vec<HandoverArtifact>,
    evidence: &HandoverEvidence<'_>,
    next_step: String,
) -> HandoverV1 {
    let mut verified_facts = Vec::new();
    let mut unverified_notes = Vec::new();
    for claim in claims {
        let backed = claim.evidence_id.as_deref().is_some_and(|id| {
            evidence.observations.iter().any(|observation| {
                observation.attempt_id == predecessor && observation.dedup_key == id
            }) || artifacts
                .iter()
                .any(|artifact| artifact.hash.as_str() == id)
        });
        if backed {
            verified_facts.push(claim.text);
        } else {
            unverified_notes.push(claim.text);
        }
    }
    HandoverV1 {
        version: HANDOVER_VERSION,
        predecessor,
        input,
        artifacts,
        verified_facts,
        unverified_notes,
        next_step,
    }
}

/// Strictly parse a handover file (the validated import route when the live
/// structured channel is unavailable). Separation of verified and unverified
/// claims is preserved as written; promotion requires [`build_handover`].
pub fn parse_handover(text: &str) -> Result<HandoverV1, AppError> {
    let handover: HandoverV1 = serde_json::from_str(text)
        .map_err(|error| AppError::Validation(format!("malformed handover: {error}")))?;
    reject_version(HANDOVER_SCHEMA, handover.version, HANDOVER_VERSION)?;
    if handover.next_step.trim().is_empty() {
        return Err(AppError::Validation(
            "handover next_step must not be empty".to_string(),
        ));
    }
    for artifact in &handover.artifacts {
        check_candidate_path(&artifact.path)?;
    }
    Ok(handover)
}

/// Canonical hash of a handover file: SHA-256 over the canonical JSON, the
/// same content addressing the file import CLI prints next to the schema id.
pub fn handover_hash(handover: &HandoverV1) -> ContentHash {
    let value = serde_json::to_value(handover).expect("handover serialization is infallible");
    let canonical = serde_json::to_string(&value).expect("handover serialization is infallible");
    ContentHash::from_hex(sha256_hex(canonical.as_bytes()))
}

/// Canonical hash of a worker-result file for the import route.
pub fn worker_result_hash(result: &WorkerResultV1) -> ContentHash {
    let value = serde_json::to_value(result).expect("worker result serialization is infallible");
    let canonical =
        serde_json::to_string(&value).expect("worker result serialization is infallible");
    ContentHash::from_hex(sha256_hex(canonical.as_bytes()))
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(bytes);
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        hex.push_str(&format!("{byte:02x}"));
    }
    hex
}

/// Evidence view over a loaded run for one attempt: its observations plus
/// the hashes of artifacts the run already stores. A new session inspects
/// this before writing its handover.
pub fn evidence_for(state: &RunState, attempt: AttemptId) -> (Vec<Observation>, Vec<ContentHash>) {
    let observations = state
        .observations
        .iter()
        .filter(|observation| observation.attempt_id == attempt)
        .cloned()
        .collect();
    let mut hashes: BTreeMap<String, ContentHash> = BTreeMap::new();
    for artifact in state.artifacts.values() {
        if artifact.attempt_id == Some(attempt) {
            hashes.insert(artifact.id.to_string(), artifact.hash.clone());
        }
    }
    (observations, hashes.into_values().collect())
}
