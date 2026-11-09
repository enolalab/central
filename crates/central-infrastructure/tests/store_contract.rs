use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use central_application::contracts::{AppError, CommitBatch, OutboxIntent, OutboxStatus, Receipt};
use central_application::ports::Store;
use central_domain::{
    decide, Actor, ActorId, ActorKind, AttemptId, Command, CommandId, ContentHash, Effect, Event,
    IntentId, Revision, RunId, RunState, Timestamp,
};
use central_infrastructure::storage::{DuckDbStore, FailPoint};

fn temp_db(name: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!(
        "central-store-contract-{}-{name}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir.join("central.duckdb")
}

fn user() -> Actor {
    Actor {
        id: ActorId::from_u128(100),
        kind: ActorKind::User,
    }
}

fn run_id() -> RunId {
    RunId::from_u128(10)
}

fn created_state() -> RunState {
    let state = RunState::new(run_id());
    decide(
        &state,
        &user(),
        &Command::CreateRun {
            goal: "Build the durable runtime".to_string(),
            repository: "D:/Code/enolalab/central".to_string(),
        },
        Timestamp(1_700_000_000_000),
    )
    .expect("create run decision")
    .next
}

fn batch(payload: &str) -> CommitBatch {
    let next_state = created_state();
    let receipt = Receipt {
        command_id: CommandId::from_u128(900),
        run_id: run_id(),
        revision: Revision(1),
        result: "applied".to_string(),
        payload_hash: ContentHash::from_hex(payload),
    };
    CommitBatch {
        actor: user().id,
        command_id: CommandId::from_u128(900),
        payload_hash: ContentHash::from_hex(payload),
        expected_revision: Revision(0),
        next_state,
        events: vec![Event::RunCreated {
            run_id: run_id(),
            goal: "Build the durable runtime".to_string(),
            repository: "D:/Code/enolalab/central".to_string(),
            by: user().id,
            at: Timestamp(1_700_000_000_000),
        }],
        intents: vec![OutboxIntent {
            id: IntentId::from_u128(700),
            run_id: run_id(),
            effect: Effect::CancelAttempt {
                attempt_id: AttemptId::from_u128(42),
            },
            status: OutboxStatus::Pending,
        }],
        receipt,
    }
}

#[tokio::test]
async fn atomic_commit_and_receipt() {
    let store = DuckDbStore::open(&temp_db("atomic")).expect("open store");
    let first_receipt = store.commit(batch("a")).await.expect("commit");

    let state = store
        .load(run_id())
        .await
        .expect("load")
        .expect("run exists");
    assert_eq!(state.revision, Revision(1));
    let events = store.events(run_id()).await.expect("events");
    assert_eq!(events.len(), 1);
    let intents = store.pending(run_id()).await.expect("pending");
    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].status, OutboxStatus::Pending);

    let duplicate_receipt = store.commit(batch("a")).await.expect("duplicate commit");
    assert_eq!(duplicate_receipt, first_receipt);
    // Duplicate replay must not append a second event or intent.
    assert_eq!(store.events(run_id()).await.expect("events").len(), 1);
    assert_eq!(store.pending(run_id()).await.expect("pending").len(), 1);
    let stored = store
        .receipt(user().id, CommandId::from_u128(900))
        .await
        .expect("receipt lookup")
        .expect("receipt exists");
    assert_eq!(stored, first_receipt);
}

#[tokio::test]
async fn same_id_different_payload_conflicts() {
    let store = DuckDbStore::open(&temp_db("conflict-payload")).expect("open store");
    store.commit(batch("a")).await.expect("commit");

    let mut different = batch("b");
    different.next_state = {
        let mut state = created_state();
        state.goal = "A different goal".to_string();
        state
    };
    let err = store.commit(different).await.expect_err("conflict");
    assert!(matches!(err, AppError::Conflict(_)), "got {err:?}");

    let state = store.load(run_id()).await.expect("load").expect("run");
    assert_eq!(state.revision, Revision(1));
    assert_eq!(state.goal, "Build the durable runtime");
    assert_eq!(store.events(run_id()).await.expect("events").len(), 1);
    assert_eq!(store.pending(run_id()).await.expect("pending").len(), 1);
}

#[tokio::test]
async fn expected_revision_conflicts() {
    let store = DuckDbStore::open(&temp_db("conflict-revision")).expect("open store");

    let mut stale = batch("a");
    stale.expected_revision = Revision(5);
    stale.next_state.revision = Revision(6);
    stale.receipt.revision = Revision(6);
    let err = store.commit(stale).await.expect_err("stale expected");
    assert!(matches!(err, AppError::Conflict(_)), "got {err:?}");
    assert_eq!(store.load(run_id()).await.expect("load"), None);

    store.commit(batch("a")).await.expect("commit");

    // A second, distinct command that still expects revision 0 must CAS-fail.
    let mut second = batch("c");
    second.command_id = CommandId::from_u128(901);
    second.receipt.command_id = CommandId::from_u128(901);
    second.receipt.payload_hash = ContentHash::from_hex("c");
    second.payload_hash = ContentHash::from_hex("c");
    let err = store.commit(second).await.expect_err("cas conflict");
    assert!(matches!(err, AppError::Conflict(_)), "got {err:?}");
    let state = store.load(run_id()).await.expect("load").expect("run");
    assert_eq!(state.revision, Revision(1));
    assert_eq!(store.events(run_id()).await.expect("events").len(), 1);
}

#[tokio::test]
async fn injected_failure_at_each_statement_rolls_back() {
    for point in [
        FailPoint::Snapshot,
        FailPoint::Event,
        FailPoint::Outbox,
        FailPoint::Receipt,
    ] {
        let store = DuckDbStore::open(&temp_db("rollback")).expect("open store");
        store.inject_commit_failure(point);
        let err = store
            .commit(batch("a"))
            .await
            .expect_err("injected failure must fail the commit");
        assert!(
            matches!(err, AppError::Storage(_)),
            "point {point:?}: {err:?}"
        );
        assert_eq!(store.load(run_id()).await.expect("load"), None, "{point:?}");
        assert_eq!(
            store.events(run_id()).await.expect("events").len(),
            0,
            "{point:?}"
        );
        assert_eq!(
            store.pending(run_id()).await.expect("pending").len(),
            0,
            "{point:?}"
        );
        assert_eq!(
            store
                .receipt(user().id, CommandId::from_u128(900))
                .await
                .expect("receipt"),
            None,
            "{point:?}"
        );
        // The writer survives the rollback and a clean retry commits whole.
        store.commit(batch("a")).await.expect("retry commit");
        let state = store.load(run_id()).await.expect("load").expect("run");
        assert_eq!(state.revision, Revision(1));
    }
}

#[tokio::test]
async fn reopen_preserves_revision_events_intents_and_receipt() {
    let path = temp_db("reopen");
    let first_receipt = {
        let store = DuckDbStore::open(&path).expect("open store");
        store.commit(batch("a")).await.expect("commit")
    };
    let store = DuckDbStore::open(&path).expect("reopen store");
    let state = store.load(run_id()).await.expect("load").expect("run");
    assert_eq!(state.revision, Revision(1));
    assert_eq!(store.events(run_id()).await.expect("events").len(), 1);
    assert_eq!(store.pending(run_id()).await.expect("pending").len(), 1);
    assert_eq!(
        store
            .receipt(user().id, CommandId::from_u128(900))
            .await
            .expect("receipt")
            .expect("receipt exists"),
        first_receipt
    );
}

struct FixedClock;
impl central_application::ports::Clock for FixedClock {
    fn now(&self) -> Timestamp {
        Timestamp(1_700_000_000_000)
    }
}

struct FakeIds(std::sync::atomic::AtomicU64);
impl central_application::ports::IdSource for FakeIds {
    fn next_run_id(&self) -> RunId {
        RunId::from_u128(5_000 + self.0.fetch_add(1, Ordering::SeqCst) as u128)
    }
    fn next_intent_id(&self) -> IntentId {
        IntentId::from_u128(6_000 + self.0.fetch_add(1, Ordering::SeqCst) as u128)
    }
    fn next_command_id(&self) -> CommandId {
        CommandId::from_u128(7_000 + self.0.fetch_add(1, Ordering::SeqCst) as u128)
    }
}

#[derive(Default)]
struct CountingGateway {
    sends: AtomicU64,
}
impl CountingGateway {
    fn send_count(&self) -> u64 {
        self.sends.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl central_application::ports::AgentGateway for CountingGateway {
    async fn capabilities(
        &self,
        _profile: &central_domain::ResolvedProfile,
    ) -> Result<central_application::contracts::Capabilities, AppError> {
        Ok(central_application::contracts::Capabilities {
            provider: "fake".to_string(),
            supports_resume: true,
            features: Vec::new(),
        })
    }
    async fn open(
        &self,
        _request: central_application::contracts::OpenSession,
    ) -> Result<central_application::contracts::SessionRef, AppError> {
        Ok(central_application::contracts::SessionRef {
            provider: "fake".to_string(),
            instance_epoch: "epoch-1".to_string(),
            thread_id: "thread-1".to_string(),
        })
    }
    async fn send(
        &self,
        request: central_application::contracts::SendTurn,
    ) -> Result<
        central_application::contracts::TurnRef,
        central_application::contracts::DispatchError,
    > {
        self.sends.fetch_add(1, Ordering::SeqCst);
        Ok(central_application::contracts::TurnRef {
            session: request.session,
            turn_id: "turn-1".to_string(),
        })
    }
    async fn observe(
        &self,
        _session: &central_application::contracts::SessionRef,
    ) -> Result<Vec<central_domain::Observation>, AppError> {
        Ok(Vec::new())
    }
    async fn answer(
        &self,
        _answer: central_application::contracts::ExternalAnswer,
    ) -> Result<(), central_application::contracts::DispatchError> {
        Ok(())
    }
    async fn cancel(
        &self,
        _target: &central_application::contracts::ExecutionRef,
    ) -> Result<central_application::contracts::CancelObservation, AppError> {
        Ok(central_application::contracts::CancelObservation {
            cancelled: true,
            detail: "cancelled".to_string(),
        })
    }
    async fn reconcile(
        &self,
        target: &central_application::contracts::ExecutionRef,
    ) -> Result<central_application::contracts::ReconcileEvidence, AppError> {
        Ok(central_application::contracts::ReconcileEvidence {
            attempt_id: target.attempt_id,
            status: central_domain::AttemptStatus::UnknownOutcome,
            detail: "no evidence".to_string(),
        })
    }
}

#[tokio::test]
async fn runtime_execute_over_duckdb_replays_after_reopen() {
    use std::sync::Arc;

    let path = temp_db("runtime-reopen");
    let gateway = Arc::new(CountingGateway::default());
    let envelope = central_domain::CommandEnvelope {
        id: CommandId::from_u128(900),
        expected_revision: None,
        command: Command::CreateRun {
            goal: "Build the durable runtime".to_string(),
            repository: "D:/Code/enolalab/central".to_string(),
        },
    };
    let first = {
        let store = Arc::new(DuckDbStore::open(&path).expect("open store"));
        let runtime = central_application::Runtime::new(
            store.clone(),
            Arc::new(FixedClock),
            Arc::new(FakeIds(AtomicU64::new(0))),
            gateway.clone(),
            run_id(),
        );
        runtime
            .execute(user(), envelope.clone())
            .await
            .expect("execute create over DuckDB")
    };
    assert_eq!(first.revision, Revision(1));
    assert_eq!(gateway.send_count(), 0);

    // After a reopen, the same envelope replays the persisted receipt
    // instead of deciding CreateRun again (which would fail as duplicate).
    let store = Arc::new(DuckDbStore::open(&path).expect("reopen store"));
    let runtime = central_application::Runtime::new(
        store.clone(),
        Arc::new(FixedClock),
        Arc::new(FakeIds(AtomicU64::new(0))),
        gateway.clone(),
        run_id(),
    );
    let replayed = runtime
        .execute(user(), envelope)
        .await
        .expect("replay after reopen");
    assert_eq!(replayed, first);
    assert_eq!(store.events(run_id()).await.expect("events").len(), 1);
    assert_eq!(gateway.send_count(), 0);
}

#[tokio::test]
async fn newer_schema_fails_closed() {
    let path = temp_db("newer-schema");
    {
        let conn = duckdb::Connection::open(&path).expect("raw open");
        conn.execute_batch(
            "CREATE TABLE schema_migrations (version BIGINT PRIMARY KEY);
             INSERT INTO schema_migrations VALUES (999);
             CREATE TABLE runs (id VARCHAR PRIMARY KEY, revision BIGINT, snapshot_json VARCHAR);
             INSERT INTO runs VALUES ('kept-run', 7, '{}');",
        )
        .expect("seed newer schema");
    }
    let err = DuckDbStore::open(&path).expect_err("newer schema must fail closed");
    assert!(matches!(err, AppError::Storage(_)), "got {err:?}");
    // The existing data must not be reset or dropped by the failed open.
    let conn = duckdb::Connection::open(&path).expect("raw reopen");
    let mut stmt = conn
        .prepare("SELECT revision FROM runs WHERE id = 'kept-run'")
        .expect("prepare");
    let revision: i64 = stmt.query_row([], |row| row.get(0)).expect("row kept");
    assert_eq!(revision, 7);
}

#[tokio::test]
async fn corrupt_snapshot_is_not_empty_run() {
    let path = temp_db("corrupt");
    let run = run_id();
    {
        let store = DuckDbStore::open(&path).expect("open store");
        store.commit(batch("a")).await.expect("commit");
    }
    {
        let conn = duckdb::Connection::open(&path).expect("raw open");
        conn.execute(
            "UPDATE runs SET snapshot_json = 'not-json' WHERE id = ?",
            duckdb::params![run.to_string()],
        )
        .expect("corrupt snapshot");
    }
    let store = DuckDbStore::open(&path).expect("reopen store");
    let err = store
        .load(run)
        .await
        .expect_err("corrupt snapshot must error");
    assert!(matches!(err, AppError::Storage(_)), "got {err:?}");

    // A snapshot from a newer persisted state version also fails closed.
    let path = temp_db("future-snapshot");
    {
        let store = DuckDbStore::open(&path).expect("open store");
        store.commit(batch("a")).await.expect("commit");
    }
    {
        let conn = duckdb::Connection::open(&path).expect("raw open");
        let mut stmt = conn
            .prepare("SELECT snapshot_json FROM runs WHERE id = ?")
            .expect("prepare");
        let json: String = stmt
            .query_row(duckdb::params![run.to_string()], |row| row.get(0))
            .expect("snapshot");
        let mut value: serde_json::Value = serde_json::from_str(&json).expect("json");
        value["version"] = serde_json::json!(999);
        // Keep the map non-empty so this exercises version handling, not emptiness.
        let _ = BTreeMap::<String, String>::new();
        conn.execute(
            "UPDATE runs SET snapshot_json = ? WHERE id = ?",
            duckdb::params![value.to_string(), run.to_string()],
        )
        .expect("write future snapshot");
    }
    let store = DuckDbStore::open(&path).expect("reopen store");
    let err = store
        .load(run)
        .await
        .expect_err("future snapshot must error");
    assert!(matches!(err, AppError::Storage(_)), "got {err:?}");
}

#[tokio::test]
async fn non_pending_intent_insert_is_rejected() {
    let store = DuckDbStore::open(&temp_db("non-pending")).expect("open store");
    let mut bad = batch("a");
    bad.intents[0].status = OutboxStatus::Completed;
    let err = store.commit(bad).await.expect_err("non-pending insert");
    assert!(matches!(err, AppError::Validation(_)), "got {err:?}");
    assert_eq!(store.load(run_id()).await.expect("load"), None);
}

#[tokio::test]
async fn guarded_outbox_transitions_require_dispatch_evidence() {
    use central_application::ports::Store as _;
    let store = DuckDbStore::open(&temp_db("guarded")).expect("open store");
    store.commit(batch("a")).await.expect("commit");
    let intent = IntentId::from_u128(700);

    // Pending -> Completed skips dispatch: rejected outright.
    let err = store
        .transition_outbox(intent, OutboxStatus::Pending, OutboxStatus::Completed)
        .await
        .expect_err("direct completion must be rejected");
    assert!(matches!(err, AppError::Validation(_)), "got {err:?}");

    // The evidence path works exactly once per guard.
    assert!(store
        .transition_outbox(intent, OutboxStatus::Pending, OutboxStatus::Dispatching)
        .await
        .expect("claim"));
    assert!(!store
        .transition_outbox(intent, OutboxStatus::Pending, OutboxStatus::Dispatching)
        .await
        .expect("second claim loses the guard"));
    assert!(store
        .transition_outbox(intent, OutboxStatus::Dispatching, OutboxStatus::Completed)
        .await
        .expect("evidence completes"));
    assert_eq!(store.pending(run_id()).await.expect("pending").len(), 0);
    let all = store.outbox(run_id()).await.expect("outbox");
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].status, OutboxStatus::Completed);
}
