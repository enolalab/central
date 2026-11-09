use duckdb::{params, Connection};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

pub fn append_marker(marker_path: &Path, line: &str) {
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(marker_path) {
        let _ = writeln!(f, "{}", line);
        let _ = f.flush();
        let _ = f.sync_all();
    }
}

pub fn init_schema(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS snapshots(run_id VARCHAR PRIMARY KEY, revision BIGINT, state VARCHAR);
         CREATE TABLE IF NOT EXISTS events(event_id VARCHAR PRIMARY KEY, run_id VARCHAR, seq BIGINT, payload VARCHAR);
         CREATE TABLE IF NOT EXISTS outbox(intent_id VARCHAR PRIMARY KEY, run_id VARCHAR, payload VARCHAR);
         CREATE TABLE IF NOT EXISTS receipts(actor VARCHAR, command_id VARCHAR, payload_hash VARCHAR, revision BIGINT, PRIMARY KEY(actor, command_id));
         CREATE TABLE IF NOT EXISTS task_meta(run_id VARCHAR, task_id VARCHAR, title VARCHAR, PRIMARY KEY(run_id, task_id));",
    )?;
    Ok(())
}

pub fn run_child(db_path: &Path, cycle: i64, marker_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = Connection::open(db_path)?;
    init_schema(&conn)?;
    // Committed transaction: snapshot + event + outbox + receipt atomically.
    {
        let tx = conn.transaction()?;
        tx.execute(
            "UPDATE snapshots SET revision = ?, state = ? WHERE run_id = 'crash-run'",
            params![cycle, format!("committed-{}", cycle)],
        )?;
        tx.execute(
            "INSERT INTO events VALUES (?, 'crash-run', ?, ?)",
            params![format!("committed-{}", cycle), cycle, format!("committed-payload-{}", cycle)],
        )?;
        tx.execute(
            "INSERT INTO outbox VALUES (?, 'crash-run', ?)",
            params![format!("committed-{}", cycle), format!("committed-payload-{}", cycle)],
        )?;
        tx.execute(
            "INSERT INTO receipts VALUES ('crash-actor', ?, 'hash-committed', ?)",
            params![format!("cmd-committed-{}", cycle), cycle],
        )?;
        tx.commit()?;
    }
    append_marker(marker_path, &format!("AFTER_COMMIT {}", cycle));
    // Uncommitted transaction: must never become visible after kill.
    {
        let tx = conn.transaction()?;
        tx.execute(
            "UPDATE snapshots SET revision = ?, state = ? WHERE run_id = 'crash-run'",
            params![cycle + 100000, format!("uncommitted-{}", cycle)],
        )?;
        tx.execute(
            "INSERT INTO events VALUES (?, 'crash-run', ?, ?)",
            params![format!("uncommitted-{}", cycle), cycle, format!("uncommitted-payload-{}", cycle)],
        )?;
        tx.execute(
            "INSERT INTO outbox VALUES (?, 'crash-run', ?)",
            params![format!("uncommitted-{}", cycle), format!("uncommitted-payload-{}", cycle)],
        )?;
        append_marker(marker_path, &format!("BEFORE_COMMIT {}", cycle));
        // Hold the transaction open until the parent kills us.
        loop {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }
}

pub fn run_holdlock(db_path: &Path, marker_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = Connection::open(db_path)?;
    init_schema(&conn)?;
    let tx = conn.transaction()?;
    tx.execute(
        "UPDATE snapshots SET revision = 999999, state = 'lock-holder' WHERE run_id = 'crash-run'",
        [],
    )?;
    append_marker(marker_path, "LOCK_HELD");
    loop {
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}
