//! Result and handover import: `POST /v1/runs/{id}/attempts/{attempt}/results`
//! and `POST /v1/runs/{id}/handover`.
//!
//! Validated import with binding and candidate checks that records for
//! real — never a print-only validation. A worker report is sealed
//! (membership, containment, exact input/candidate/criteria hashes)
//! and committed through the durable `RecordWorkerResult` command,
//! which parks the task in `AwaitingVerification` and never marks it
//! passed. A handover is parsed, evidence-checked and recorded as a
//! durable observation for the new session to inspect — prose without
//! evidence stays unverified. Owner imports, or the attempt-bound
//! bridge filing for its own attempt; every other caller is 403.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use central_application::ports::{Clock, IdSource};
use central_application::{handover, verification};
use central_domain::{AttemptId, Command, CommandEnvelope, Observation, RunId};

use super::auth::{app_error_response, authenticate, AuthScope};
use super::AppState;
use crate::api::errors::{error_response, ApiError};

fn runtime_for(state: &AppState, run_id: RunId) -> central_application::Runtime {
    central_application::Runtime::new(
        Arc::clone(&state.store),
        Arc::clone(&state.clock) as Arc<dyn Clock>,
        Arc::clone(&state.ids) as Arc<dyn IdSource>,
        Arc::clone(&state.agent),
        run_id,
    )
    .with_secrets(Arc::clone(&state.secrets))
}

/// Import rights: the owner, a control client, or the bridge bound to
/// exactly this attempt. Anything else (readers, other attempts'
/// bridges) is 403.
fn authorize_import(auth: &super::auth::Auth, attempt: AttemptId) -> Result<(), ApiError> {
    match &auth.scope {
        AuthScope::Owner | AuthScope::Control => Ok(()),
        AuthScope::Bridge { attempt: bound, .. } if *bound == attempt => Ok(()),
        _ => Err(error_response(
            StatusCode::FORBIDDEN,
            "forbidden",
            "results import needs the owner, a control client, or the attempt's own bridge",
        )),
    }
}

pub async fn post_results(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((raw_id, raw_attempt)): Path<(String, String)>,
    body: String,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = authenticate(&state, &headers).await?;
    let run_id = raw_id.parse::<RunId>().map_err(|_| {
        error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            format!("invalid run id: {raw_id}"),
        )
    })?;
    let attempt_id = raw_attempt.parse::<AttemptId>().map_err(|_| {
        error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            format!("invalid attempt id: {raw_attempt}"),
        )
    })?;
    authorize_import(&auth, attempt_id)?;
    // Strict parse first: schema version, unknown fields, empty
    // summary and escaping paths are all refused before any state.
    let report = handover::parse_worker_result(&body).map_err(app_error_response)?;
    if report.attempt_id != attempt_id {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            format!(
                "report names attempt {} but the path binds {attempt_id}",
                report.attempt_id
            ),
        ));
    }
    let run_state = state
        .store
        .load(run_id)
        .await
        .map_err(app_error_response)?
        .ok_or_else(|| {
            error_response(
                StatusCode::NOT_FOUND,
                "not_found",
                format!("run {run_id} not found"),
            )
        })?;
    // Membership + containment against the loaded run, then the exact
    // seal the verifier must reproduce. A foreign candidate never
    // reaches the commit.
    let seal = verification::seal_candidate(&run_state, &report).map_err(|error| {
        use central_application::contracts::AppError;
        match error {
            AppError::Validation(message) if message.starts_with("foreign candidate") => {
                error_response(StatusCode::NOT_FOUND, "not_found", message)
            }
            other => app_error_response(other),
        }
    })?;
    let envelope = CommandEnvelope {
        id: state.ids.next_command_id(),
        expected_revision: None,
        command: Command::RecordWorkerResult {
            attempt: attempt_id,
            input_hash: seal.input_hash.clone(),
            candidate_hash: seal.candidate_hash.clone(),
            criteria_hash: seal.criteria_hash.clone(),
        },
    };
    let receipt = runtime_for(&state, run_id)
        .execute(auth.actor, envelope)
        .await
        .map_err(app_error_response)?;
    state.scan_notify.notify_one();
    let receipt = serde_json::to_value(&receipt).map_err(|error| {
        error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            format!("cannot encode receipt: {error}"),
        )
    })?;
    Ok(Json(serde_json::json!({
        "receipt": receipt,
        "sealed": {
            "task_id": seal.task_id.to_string(),
            "input_hash": seal.input_hash.to_string(),
            "candidate_hash": seal.candidate_hash.to_string(),
            "criteria_hash": seal.criteria_hash.to_string(),
            "status": "awaiting_verification",
        },
    })))
}

pub async fn post_handover(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(raw_id): Path<String>,
    body: String,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = authenticate(&state, &headers).await?;
    let run_id = raw_id.parse::<RunId>().map_err(|_| {
        error_response(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            format!("invalid run id: {raw_id}"),
        )
    })?;
    // Strict parse: version, contained artifact paths, non-empty step.
    let handover_file = handover::parse_handover(&body).map_err(app_error_response)?;
    authorize_import(&auth, handover_file.predecessor)?;
    let run_state = state
        .store
        .load(run_id)
        .await
        .map_err(app_error_response)?
        .ok_or_else(|| {
            error_response(
                StatusCode::NOT_FOUND,
                "not_found",
                format!("run {run_id} not found"),
            )
        })?;
    if !run_state.attempts.contains_key(&handover_file.predecessor) {
        return Err(error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            format!(
                "foreign handover: predecessor {} does not belong to run {run_id}",
                handover_file.predecessor
            ),
        ));
    }
    // Evidence inspection before recording: each submitted fact must
    // cite the predecessor's observations (dedup key or detail) or a
    // carried artifact hash to stay verified; everything else is
    // stored as unverified — honestly labeled, never promoted.
    let (observations, hashes) = handover::evidence_for(&run_state, handover_file.predecessor);
    let mut verified = 0usize;
    let mut unverified = handover_file.unverified_notes.len();
    for fact in &handover_file.verified_facts {
        let backed = observations
            .iter()
            .any(|observation| observation.dedup_key == *fact || observation.detail.contains(fact))
            || hashes.iter().any(|hash| hash.as_str() == fact);
        if backed {
            verified += 1;
        } else {
            unverified += 1;
        }
    }
    // Durable record as an observation on the predecessor: the new
    // session inspects evidence before writing, and the detail marks
    // what was verified versus carried over unverified.
    let hash = handover::handover_hash(&handover_file);
    let observation = Observation {
        attempt_id: handover_file.predecessor,
        provider_epoch: "handover".to_string(),
        dedup_key: hash.to_string(),
        outcome: None,
        detail: format!(
            "handover: {verified} verified fact(s), {unverified} unverified note(s); next: {}",
            handover_file.next_step,
        ),
    };
    let envelope = CommandEnvelope {
        id: state.ids.next_command_id(),
        expected_revision: None,
        command: Command::RecordObservation { observation },
    };
    // The handover endpoint is the specialized write path for this
    // command: it validated, bound and evidence-checked first, unlike
    // the generic gateway which refuses it as non-operator.
    let receipt = runtime_for(&state, run_id)
        .execute(auth.actor, envelope)
        .await
        .map_err(app_error_response)?;
    state.scan_notify.notify_one();
    let receipt = serde_json::to_value(&receipt).map_err(|error| {
        error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            format!("cannot encode receipt: {error}"),
        )
    })?;
    Ok(Json(serde_json::json!({
        "receipt": receipt,
        "handover_hash": hash.to_string(),
        "verified_facts": verified,
        "unverified_notes": unverified,
    })))
}
