//! DuckDB-backed `Store`. A single dedicated blocking writer thread owns
//! the connection and receives work over a bounded Tokio channel; callers
//! await only the channel reply, so no transaction is held across an
//! await point. A second process opening the same file is rejected by
//! DuckDB's own file lock.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use async_trait::async_trait;
use central_application::contracts::{AppError, CommitBatch, OutboxIntent, OutboxStatus, Receipt};
use central_application::ports::Store;
use central_domain::{ActorId, CommandId, Event, IntentId, RunId, RunState};
use duckdb::Connection;
use tokio::sync::{mpsc, oneshot};

use super::writer::{self, FailPoint, SharedFailPoint};

pub const SCHEMA_VERSION: i64 = 1;
const MIGRATION_SQL: &str = include_str!("migrations/001_runtime.sql");
const CHANNEL_CAPACITY: usize = 64;

type Reply<T> = oneshot::Sender<Result<T, AppError>>;

enum WriterMessage {
    Load {
        id: RunId,
        reply: Reply<Option<RunState>>,
    },
    Receipt {
        actor: ActorId,
        id: CommandId,
        reply: Reply<Option<Receipt>>,
    },
    Commit {
        batch: Box<CommitBatch>,
        reply: Reply<Receipt>,
    },
    Pending {
        id: RunId,
        reply: Reply<Vec<OutboxIntent>>,
    },
    Events {
        id: RunId,
        reply: Reply<Vec<Event>>,
    },
    MarkOutbox {
        id: IntentId,
        status: OutboxStatus,
        reply: Reply<()>,
    },
}

pub struct DuckDbStore {
    sender: Option<mpsc::Sender<WriterMessage>>,
    fail_point: SharedFailPoint,
    writer_thread: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for DuckDbStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckDbStore")
            .field("writer_running", &self.writer_thread.is_some())
            .finish_non_exhaustive()
    }
}

impl DuckDbStore {
    pub fn open(path: &Path) -> Result<Self, AppError> {
        let (sender, receiver) = mpsc::channel(CHANNEL_CAPACITY);
        let (init_sender, init_receiver) = std::sync::mpsc::sync_channel::<Result<(), AppError>>(1);
        let fail_point: SharedFailPoint = Arc::new(Mutex::new(None));
        let thread_fail_point = fail_point.clone();
        let owned_path: PathBuf = path.to_path_buf();
        let writer_thread = std::thread::spawn(move || {
            writer_loop(owned_path, receiver, init_sender, thread_fail_point);
        });

        match init_receiver.recv() {
            Ok(Ok(())) => Ok(Self {
                sender: Some(sender),
                fail_point,
                writer_thread: Some(writer_thread),
            }),
            Ok(Err(error)) => {
                let _ = writer_thread.join();
                Err(error)
            }
            Err(_) => {
                let _ = writer_thread.join();
                Err(AppError::Storage(
                    "storage writer exited during open".to_string(),
                ))
            }
        }
    }

    /// Test support: make the next commit fail at the given statement and
    /// roll back. One-shot; consumed when it fires.
    pub fn inject_commit_failure(&self, point: FailPoint) {
        if let Ok(mut guard) = self.fail_point.lock() {
            *guard = Some(point);
        }
    }

    /// All events persisted for a run, in sequence order.
    pub async fn events(&self, id: RunId) -> Result<Vec<Event>, AppError> {
        let (reply, wait) = oneshot::channel();
        self.send(WriterMessage::Events { id, reply }).await?;
        wait.await
            .map_err(|_| AppError::Storage("storage writer dropped its reply".to_string()))?
    }

    /// Move an outbox intent to a new status (for example `UnknownOutcome`
    /// after an ambiguous dispatch — never straight to `Completed` without
    /// dispatch evidence).
    pub async fn mark_outbox_status(
        &self,
        id: IntentId,
        status: OutboxStatus,
    ) -> Result<(), AppError> {
        let (reply, wait) = oneshot::channel();
        self.send(WriterMessage::MarkOutbox { id, status, reply })
            .await?;
        wait.await
            .map_err(|_| AppError::Storage("storage writer dropped its reply".to_string()))?
    }

    async fn send(&self, message: WriterMessage) -> Result<(), AppError> {
        let sender = self
            .sender
            .as_ref()
            .ok_or_else(|| AppError::Storage("storage writer is closed".to_string()))?;
        sender
            .send(message)
            .await
            .map_err(|_| AppError::Storage("storage writer unavailable".to_string()))
    }
}

impl Drop for DuckDbStore {
    fn drop(&mut self) {
        // Closing the channel ends the writer loop; joining guarantees the
        // DuckDB file lock is released before a reopen in the same process.
        self.sender.take();
        if let Some(handle) = self.writer_thread.take() {
            let _ = handle.join();
        }
    }
}

fn writer_loop(
    path: PathBuf,
    mut receiver: mpsc::Receiver<WriterMessage>,
    init: std::sync::mpsc::SyncSender<Result<(), AppError>>,
    fail_point: SharedFailPoint,
) {
    let mut conn = match Connection::open(&path) {
        Ok(conn) => conn,
        Err(error) => {
            let _ = init.send(Err(writer::storage_error(error)));
            return;
        }
    };
    if let Err(error) = initialize_schema(&conn) {
        let _ = init.send(Err(error));
        return;
    }
    if init.send(Ok(())).is_err() {
        return;
    }

    while let Some(message) = receiver.blocking_recv() {
        match message {
            WriterMessage::Load { id, reply } => {
                let _ = reply.send(writer::load(&conn, id));
            }
            WriterMessage::Receipt { actor, id, reply } => {
                let _ = reply.send(writer::receipt(&conn, actor, id));
            }
            WriterMessage::Commit { batch, reply } => {
                let _ = reply.send(writer::commit(&mut conn, &batch, &fail_point));
            }
            WriterMessage::Pending { id, reply } => {
                let _ = reply.send(writer::pending(&conn, id));
            }
            WriterMessage::Events { id, reply } => {
                let _ = reply.send(writer::events(&conn, id));
            }
            WriterMessage::MarkOutbox { id, status, reply } => {
                let _ = reply.send(writer::mark_outbox_status(&conn, id, status));
            }
        }
    }
}

fn initialize_schema(conn: &Connection) -> Result<(), AppError> {
    conn.execute_batch("CREATE TABLE IF NOT EXISTS schema_migrations (version BIGINT PRIMARY KEY)")
        .map_err(writer::storage_error)?;
    let mut stmt = conn
        .prepare("SELECT COALESCE(MAX(version), 0) FROM schema_migrations")
        .map_err(writer::storage_error)?;
    let version: i64 = stmt
        .query_row([], |row| row.get(0))
        .map_err(writer::storage_error)?;
    // Fail closed on a schema this build does not understand: never reset,
    // migrate down or drop an existing database.
    if version > SCHEMA_VERSION {
        return Err(AppError::Storage(format!(
            "unsupported schema version {version} (supported {SCHEMA_VERSION})"
        )));
    }
    conn.execute_batch(MIGRATION_SQL)
        .map_err(writer::storage_error)?;
    if version == 0 {
        conn.execute(
            "INSERT INTO schema_migrations (version) VALUES (?)",
            duckdb::params![SCHEMA_VERSION],
        )
        .map_err(writer::storage_error)?;
    }
    Ok(())
}

#[async_trait]
impl Store for DuckDbStore {
    async fn load(&self, id: RunId) -> Result<Option<RunState>, AppError> {
        let (reply, wait) = oneshot::channel();
        self.send(WriterMessage::Load { id, reply }).await?;
        wait.await
            .map_err(|_| AppError::Storage("storage writer dropped its reply".to_string()))?
    }

    async fn receipt(&self, actor: ActorId, id: CommandId) -> Result<Option<Receipt>, AppError> {
        let (reply, wait) = oneshot::channel();
        self.send(WriterMessage::Receipt { actor, id, reply })
            .await?;
        wait.await
            .map_err(|_| AppError::Storage("storage writer dropped its reply".to_string()))?
    }

    async fn commit(&self, batch: CommitBatch) -> Result<Receipt, AppError> {
        let (reply, wait) = oneshot::channel();
        self.send(WriterMessage::Commit {
            batch: Box::new(batch),
            reply,
        })
        .await?;
        wait.await
            .map_err(|_| AppError::Storage("storage writer dropped its reply".to_string()))?
    }

    async fn pending(&self, id: RunId) -> Result<Vec<OutboxIntent>, AppError> {
        let (reply, wait) = oneshot::channel();
        self.send(WriterMessage::Pending { id, reply }).await?;
        wait.await
            .map_err(|_| AppError::Storage("storage writer dropped its reply".to_string()))?
    }
}
