//! SQL executed on the single dedicated writer thread. Every commit runs
//! in one DuckDB transaction on this thread; no transaction is ever held
//! across an await point.

use std::sync::{Arc, Mutex};

use central_application::contracts::{AppError, CommitBatch, OutboxIntent, OutboxStatus, Receipt};
use central_domain::{ActorId, CommandId, Event, Revision, RunId, RunState};
use duckdb::{params, Connection, OptionalExt, Transaction};

use super::codec;

/// Statement at which a test-injected commit failure fires. The failure is
/// one-shot: it is consumed when it fires, and the whole commit rolls back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailPoint {
    Snapshot,
    Event,
    Outbox,
    Receipt,
}

pub type SharedFailPoint = Arc<Mutex<Option<FailPoint>>>;

pub fn storage_error(error: impl std::fmt::Display) -> AppError {
    AppError::Storage(error.to_string())
}

fn maybe_fail(fail: &SharedFailPoint, point: FailPoint) -> Result<(), AppError> {
    let mut guard = fail
        .lock()
        .map_err(|_| AppError::Storage("failure-injection lock poisoned".to_string()))?;
    if *guard == Some(point) {
        *guard = None;
        return Err(AppError::Storage(format!(
            "injected commit failure at {point:?} statement"
        )));
    }
    Ok(())
}

fn validate_batch(batch: &CommitBatch) -> Result<(), AppError> {
    let invalid = |msg: &str| AppError::Validation(msg.to_string());
    if batch.receipt.command_id != batch.command_id {
        return Err(invalid("receipt command id does not match the batch"));
    }
    if batch.receipt.run_id != batch.next_state.id {
        return Err(invalid("receipt run id does not match the next state"));
    }
    if batch.receipt.payload_hash != batch.payload_hash {
        return Err(invalid("receipt payload hash does not match the batch"));
    }
    if batch.receipt.revision != batch.next_state.revision {
        return Err(invalid("receipt revision does not match the next state"));
    }
    if batch.next_state.revision != batch.expected_revision.next() {
        return Err(invalid(
            "next state revision must be exactly expected revision + 1",
        ));
    }
    if batch
        .intents
        .iter()
        .any(|intent| intent.run_id != batch.next_state.id)
    {
        return Err(invalid(
            "outbox intent run id does not match the next state",
        ));
    }
    // New intents are born Pending. Anything else would smuggle a
    // dispatch outcome into the atomic commit without evidence.
    if batch
        .intents
        .iter()
        .any(|intent| intent.status != OutboxStatus::Pending)
    {
        return Err(invalid("new outbox intents must be Pending"));
    }
    Ok(())
}

pub fn load(conn: &Connection, id: RunId) -> Result<Option<RunState>, AppError> {
    let mut stmt = conn
        .prepare("SELECT snapshot_json FROM runs WHERE id = ?")
        .map_err(storage_error)?;
    let json: Option<String> = stmt
        .query_row(params![id.to_string()], |row| row.get(0))
        .optional()
        .map_err(storage_error)?;
    match json {
        None => Ok(None),
        Some(json) => Ok(Some(codec::decode_state(&json)?)),
    }
}

pub fn receipt(
    conn: &Connection,
    actor: ActorId,
    id: CommandId,
) -> Result<Option<Receipt>, AppError> {
    let mut stmt = conn
        .prepare("SELECT receipt_json FROM command_receipts WHERE actor_id = ? AND command_id = ?")
        .map_err(storage_error)?;
    let json: Option<String> = stmt
        .query_row(params![actor.to_string(), id.to_string()], |row| row.get(0))
        .optional()
        .map_err(storage_error)?;
    match json {
        None => Ok(None),
        Some(json) => Ok(Some(codec::decode_receipt(&json)?)),
    }
}

pub fn pending(conn: &Connection, id: RunId) -> Result<Vec<OutboxIntent>, AppError> {
    let mut stmt = conn
        .prepare(
            "SELECT intent_json FROM outbox \
             WHERE run_id = ? AND status = 'Pending' ORDER BY id",
        )
        .map_err(storage_error)?;
    let mut rows = stmt.query(params![id.to_string()]).map_err(storage_error)?;
    let mut intents = Vec::new();
    while let Some(row) = rows.next().map_err(storage_error)? {
        let json: String = row.get(0).map_err(storage_error)?;
        let intent = codec::decode_intent(&json)?;
        if intent.status != OutboxStatus::Pending {
            return Err(AppError::Storage(
                "pending outbox intent has a non-pending status".to_string(),
            ));
        }
        intents.push(intent);
    }
    Ok(intents)
}

pub fn events(conn: &Connection, id: RunId) -> Result<Vec<Event>, AppError> {
    let mut stmt = conn
        .prepare("SELECT event_json FROM events WHERE run_id = ? ORDER BY seq")
        .map_err(storage_error)?;
    let mut rows = stmt.query(params![id.to_string()]).map_err(storage_error)?;
    let mut events = Vec::new();
    while let Some(row) = rows.next().map_err(storage_error)? {
        let json: String = row.get(0).map_err(storage_error)?;
        events.push(codec::decode_event(&json)?);
    }
    Ok(events)
}

pub fn mark_outbox_status(
    conn: &Connection,
    intent_id: central_domain::IntentId,
    status: OutboxStatus,
) -> Result<(), AppError> {
    let mut stmt = conn
        .prepare("SELECT intent_json FROM outbox WHERE id = ?")
        .map_err(storage_error)?;
    let json: Option<String> = stmt
        .query_row(params![intent_id.to_string()], |row| row.get(0))
        .optional()
        .map_err(storage_error)?;
    let Some(json) = json else {
        return Err(AppError::NotFound(format!(
            "outbox intent {intent_id} not found"
        )));
    };
    let mut intent = codec::decode_intent(&json)?;
    intent.status = status;
    let encoded = codec::encode_intent(&intent)?;
    let changed = conn
        .execute(
            "UPDATE outbox SET intent_json = ?, status = ? WHERE id = ?",
            params![encoded, status.as_str(), intent_id.to_string()],
        )
        .map_err(storage_error)?;
    if changed != 1 {
        return Err(AppError::Storage(format!(
            "outbox status update affected {changed} rows"
        )));
    }
    Ok(())
}

pub fn list_run_ids(conn: &Connection) -> Result<Vec<RunId>, AppError> {
    let mut stmt = conn
        .prepare("SELECT id FROM runs ORDER BY id")
        .map_err(storage_error)?;
    let mut rows = stmt.query([]).map_err(storage_error)?;
    let mut ids = Vec::new();
    while let Some(row) = rows.next().map_err(storage_error)? {
        let raw: String = row.get(0).map_err(storage_error)?;
        let id = raw
            .parse::<RunId>()
            .map_err(|_| AppError::Storage(format!("corrupt run id in store: {raw}")))?;
        ids.push(id);
    }
    Ok(ids)
}

pub fn outbox(conn: &Connection, id: RunId) -> Result<Vec<OutboxIntent>, AppError> {
    let mut stmt = conn
        .prepare("SELECT intent_json FROM outbox WHERE run_id = ? ORDER BY id")
        .map_err(storage_error)?;
    let mut rows = stmt.query(params![id.to_string()]).map_err(storage_error)?;
    let mut intents = Vec::new();
    while let Some(row) = rows.next().map_err(storage_error)? {
        let json: String = row.get(0).map_err(storage_error)?;
        intents.push(codec::decode_intent(&json)?);
    }
    Ok(intents)
}

/// The only status changes dispatch/recovery may make. Anything else
/// (for example `Pending -> Completed` without a dispatch in between)
/// is rejected, so an intent can never be completed without evidence.
fn guarded_transition_allowed(expected: OutboxStatus, next: OutboxStatus) -> bool {
    matches!(
        (expected, next),
        (OutboxStatus::Pending, OutboxStatus::Dispatching)
            | (OutboxStatus::Dispatching, OutboxStatus::Completed)
            | (OutboxStatus::Dispatching, OutboxStatus::UnknownOutcome)
            | (OutboxStatus::Dispatching, OutboxStatus::Failed)
    )
}

pub fn transition_outbox(
    conn: &Connection,
    intent_id: central_domain::IntentId,
    expected: OutboxStatus,
    next: OutboxStatus,
) -> Result<bool, AppError> {
    if !guarded_transition_allowed(expected, next) {
        return Err(AppError::Validation(format!(
            "outbox transition {} -> {} is not allowed",
            expected.as_str(),
            next.as_str()
        )));
    }
    let mut stmt = conn
        .prepare("SELECT intent_json FROM outbox WHERE id = ?")
        .map_err(storage_error)?;
    let json: Option<String> = stmt
        .query_row(params![intent_id.to_string()], |row| row.get(0))
        .optional()
        .map_err(storage_error)?;
    let Some(json) = json else {
        return Err(AppError::NotFound(format!(
            "outbox intent {intent_id} not found"
        )));
    };
    let mut intent = codec::decode_intent(&json)?;
    if intent.status != expected {
        return Ok(false);
    }
    intent.status = next;
    let encoded = codec::encode_intent(&intent)?;
    // The status column is part of the guard: the row only changes if
    // it is still in the expected status.
    let changed = conn
        .execute(
            "UPDATE outbox SET intent_json = ?, status = ? WHERE id = ? AND status = ?",
            params![
                encoded,
                next.as_str(),
                intent_id.to_string(),
                expected.as_str()
            ],
        )
        .map_err(storage_error)?;
    Ok(changed == 1)
}

pub fn replace_state(
    conn: &Connection,
    state: &RunState,
    expected_revision: Revision,
) -> Result<(), AppError> {
    if state.revision != expected_revision {
        return Err(AppError::Validation(
            "recovered state revision must equal the expected revision".to_string(),
        ));
    }
    let snapshot_json = codec::encode_state(state)?;
    let expected = i64::try_from(expected_revision.0)
        .map_err(|_| AppError::Validation("expected revision out of range".to_string()))?;
    let changed = conn
        .execute(
            "UPDATE runs SET snapshot_json = ? WHERE id = ? AND revision = ?",
            params![snapshot_json, state.id.to_string(), expected],
        )
        .map_err(storage_error)?;
    if changed != 1 {
        return Err(AppError::Conflict(
            "snapshot compare-and-swap failed during recovery".to_string(),
        ));
    }
    Ok(())
}

pub fn commit(
    conn: &mut Connection,
    batch: &CommitBatch,
    fail: &SharedFailPoint,
) -> Result<Receipt, AppError> {
    validate_batch(batch)?;
    let tx = conn.transaction().map_err(storage_error)?;
    match commit_in_transaction(&tx, batch, fail) {
        Ok(receipt) => {
            tx.commit().map_err(storage_error)?;
            Ok(receipt)
        }
        // Dropping the transaction rolls back every statement of the batch.
        Err(error) => Err(error),
    }
}

fn commit_in_transaction(
    tx: &Transaction,
    batch: &CommitBatch,
    fail: &SharedFailPoint,
) -> Result<Receipt, AppError> {
    let run_id = batch.next_state.id.to_string();

    // Actor+command dedup inside the transaction: the same payload replays
    // the first receipt, a different payload conflicts and changes nothing.
    let existing: Option<(String, String)> = {
        let mut stmt = tx
            .prepare(
                "SELECT payload_hash, receipt_json FROM command_receipts \
                 WHERE actor_id = ? AND command_id = ?",
            )
            .map_err(storage_error)?;
        stmt.query_row(
            params![batch.actor.to_string(), batch.command_id.to_string()],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()
        .map_err(storage_error)?
    };
    if let Some((stored_hash, receipt_json)) = existing {
        if stored_hash != batch.payload_hash.as_str() {
            return Err(AppError::Conflict(
                "command id already used with a different payload".to_string(),
            ));
        }
        return codec::decode_receipt(&receipt_json);
    }

    // CAS on the current revision before writing anything.
    let current_revision: Option<i64> = {
        let mut stmt = tx
            .prepare("SELECT revision FROM runs WHERE id = ?")
            .map_err(storage_error)?;
        stmt.query_row(params![run_id], |row| row.get(0))
            .optional()
            .map_err(storage_error)?
    };
    let current = current_revision.unwrap_or(0);
    let expected = i64::try_from(batch.expected_revision.0)
        .map_err(|_| AppError::Validation("expected revision out of range".to_string()))?;
    if current != expected {
        return Err(AppError::Conflict(format!(
            "expected revision {} but run is at revision {}",
            batch.expected_revision,
            Revision(u64::try_from(current).unwrap_or_default())
        )));
    }

    // Statement 1: snapshot (insert for a new run, CAS update otherwise).
    maybe_fail(fail, FailPoint::Snapshot)?;
    let snapshot_json = codec::encode_state(&batch.next_state)?;
    let new_revision = i64::try_from(batch.next_state.revision.0)
        .map_err(|_| AppError::Validation("next revision out of range".to_string()))?;
    let snapshot_rows = if current_revision.is_none() {
        tx.execute(
            "INSERT INTO runs (id, revision, snapshot_json) VALUES (?, ?, ?)",
            params![run_id, new_revision, snapshot_json],
        )
        .map_err(storage_error)?
    } else {
        tx.execute(
            "UPDATE runs SET revision = ?, snapshot_json = ? WHERE id = ? AND revision = ?",
            params![new_revision, snapshot_json, run_id, expected],
        )
        .map_err(storage_error)?
    };
    if snapshot_rows != 1 {
        return Err(AppError::Conflict(
            "snapshot compare-and-swap failed".to_string(),
        ));
    }

    // Statement 2: events, sequenced after the run's existing events.
    maybe_fail(fail, FailPoint::Event)?;
    let mut seq: i64 = {
        let mut stmt = tx
            .prepare("SELECT COALESCE(MAX(seq), 0) FROM events WHERE run_id = ?")
            .map_err(storage_error)?;
        stmt.query_row(params![run_id], |row| row.get(0))
            .map_err(storage_error)?
    };
    for event in &batch.events {
        seq += 1;
        let event_json = codec::encode_event(event)?;
        tx.execute(
            "INSERT INTO events (run_id, seq, event_json) VALUES (?, ?, ?)",
            params![run_id, seq, event_json],
        )
        .map_err(storage_error)?;
    }

    // Statement 3: outbox intents (pending until dispatch evidence exists).
    maybe_fail(fail, FailPoint::Outbox)?;
    for intent in &batch.intents {
        let intent_json = codec::encode_intent(intent)?;
        tx.execute(
            "INSERT INTO outbox (id, run_id, intent_json, status) VALUES (?, ?, ?, ?)",
            params![
                intent.id.to_string(),
                run_id,
                intent_json,
                intent.status.as_str()
            ],
        )
        .map_err(storage_error)?;
    }

    // Statement 4: the command receipt.
    maybe_fail(fail, FailPoint::Receipt)?;
    let receipt_json = codec::encode_receipt(&batch.receipt)?;
    tx.execute(
        "INSERT INTO command_receipts (actor_id, command_id, payload_hash, receipt_json) \
         VALUES (?, ?, ?, ?)",
        params![
            batch.actor.to_string(),
            batch.command_id.to_string(),
            batch.payload_hash.as_str(),
            receipt_json
        ],
    )
    .map_err(storage_error)?;

    Ok(batch.receipt.clone())
}
