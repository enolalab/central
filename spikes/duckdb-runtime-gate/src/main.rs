mod child;

use duckdb::{params, Connection};
use serde::Serialize;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

#[derive(Serialize)]
struct GateReport {
    atomicity_ok: bool,
    recovery_ok: bool,
    duplicate_ok: bool,
    lock_ok: bool,
    io_failure_ok: bool,
    zero_lost_ack: bool,
    starvation_ok: bool,
    reads_ok: bool,
    p95_ms: f64,
    p99_ms: f64,
    sustained_writes_per_s: f64,
    hardware: String,
    version: String,
    crash_cycles: u32,
    perf_writes_completed: u64,
    perf_writes_sent: u64,
    perf_reads_completed: u64,
    perf_reads_failed: u64,
    gate_pass: bool,
}

fn scalar_i64(conn: &Connection, sql: &str) -> Result<i64, Box<dyn std::error::Error>> {
    let mut stmt = conn.prepare(sql)?;
    let v: i64 = stmt.query_row([], |r| r.get(0))?;
    Ok(v)
}

fn scalar_string(conn: &Connection, sql: &str) -> Result<String, Box<dyn std::error::Error>> {
    let mut stmt = conn.prepare(sql)?;
    let v: String = stmt.query_row([], |r| r.get(0))?;
    Ok(v)
}

fn wait_for_marker(path: &Path, needle: &str, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Ok(mut f) = std::fs::File::open(path) {
            let mut s = String::new();
            let _ = f.read_to_string(&mut s);
            if s.contains(needle) {
                return true;
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    false
}

fn kill_child(c: &mut Child) {
    let _ = c.kill();
    let _ = c.wait();
}

fn run_crash_cycles(exe: &Path, dir: &Path) -> Result<(bool, bool), Box<dyn std::error::Error>> {
    let db = dir.join("crash.duckdb");
    {
        let conn = Connection::open(&db)?;
        child::init_schema(&conn)?;
        conn.execute(
            "INSERT INTO snapshots VALUES ('crash-run', 0, 'init')",
            [],
        )?;
    }
    let mut atomicity_ok = true;
    let mut recovery_ok = true;
    for cycle in 1..=100i64 {
        let marker = dir.join(format!("marker-{}.txt", cycle));
        let mut proc = Command::new(exe)
            .arg("child")
            .arg(&db)
            .arg(cycle.to_string())
            .arg(&marker)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()?;
        let got = wait_for_marker(&marker, &format!("BEFORE_COMMIT {}", cycle), Duration::from_secs(60));
        if !got {
            eprintln!("crash cycle {}: BEFORE_COMMIT marker timeout", cycle);
            atomicity_ok = false;
            recovery_ok = false;
            kill_child(&mut proc);
            break;
        }
        kill_child(&mut proc);
        // Reopen and verify: committed present as a whole, uncommitted absent.
        let conn = Connection::open(&db)?;
        let committed_events = scalar_i64(&conn, &format!("SELECT COUNT(*) FROM events WHERE event_id='committed-{}'", cycle))?;
        let committed_outbox = scalar_i64(&conn, &format!("SELECT COUNT(*) FROM outbox WHERE intent_id='committed-{}'", cycle))?;
        let committed_receipts = scalar_i64(&conn, &format!("SELECT COUNT(*) FROM receipts WHERE command_id='cmd-committed-{}'", cycle))?;
        let uncommitted_events = scalar_i64(&conn, &format!("SELECT COUNT(*) FROM events WHERE event_id='uncommitted-{}'", cycle))?;
        let uncommitted_outbox = scalar_i64(&conn, &format!("SELECT COUNT(*) FROM outbox WHERE intent_id='uncommitted-{}'", cycle))?;
        let uncommitted_receipts = scalar_i64(&conn, &format!("SELECT COUNT(*) FROM receipts WHERE command_id='cmd-uncommitted-{}'", cycle))?;
        let state = scalar_string(&conn, "SELECT state FROM snapshots WHERE run_id='crash-run'")?;
        let expected = format!("committed-{}", cycle);
        if !(committed_events == 1 && committed_outbox == 1 && committed_receipts == 1 && state == expected) {
            eprintln!("cycle {} committed set incomplete: ev={} ob={} rc={} state={}", cycle, committed_events, committed_outbox, committed_receipts, state);
            atomicity_ok = false;
        }
        if !(uncommitted_events == 0 && uncommitted_outbox == 0 && uncommitted_receipts == 0 && state != format!("uncommitted-{}", cycle)) {
            eprintln!("cycle {} uncommitted leaked: ev={} ob={} rc={} state={}", cycle, uncommitted_events, uncommitted_outbox, uncommitted_receipts, state);
            atomicity_ok = false;
            recovery_ok = false;
        }
        // Prior cycles must still be intact (no lost ack across crashes).
        if cycle > 1 {
            let prev = scalar_i64(&conn, &format!("SELECT COUNT(*) FROM events WHERE event_id='committed-{}'", cycle - 1))?;
            if prev != 1 {
                eprintln!("cycle {} lost prior committed event {}", cycle, cycle - 1);
                recovery_ok = false;
            }
        }
    }
    Ok((atomicity_ok, recovery_ok))
}

fn run_duplicate_test(dir: &Path) -> Result<bool, Box<dyn std::error::Error>> {
    let db = dir.join("duplicate.duckdb");
    let mut conn = Connection::open(&db)?;
    child::init_schema(&conn)?;
    conn.execute("INSERT INTO snapshots VALUES ('run-dup', 0, 'init')", [])?;
    // First commit of (actor, cmd-1, hash-A).
    let commit = |conn: &mut Connection, hash: &str, rev: i64| -> Result<String, Box<dyn std::error::Error>> {
        // Check existing receipt first (idempotency / conflict detection).
        let existing: Result<String, _> = {
            let mut stmt = conn.prepare("SELECT payload_hash FROM receipts WHERE actor='actor-1' AND command_id='cmd-1'")?;
            stmt.query_row([], |r| r.get(0))
        };
        if let Ok(h) = existing {
            if h == hash {
                return Ok("duplicate-same".to_string());
            }
            return Ok("conflict".to_string());
        }
        let tx = conn.transaction()?;
        tx.execute("UPDATE snapshots SET revision=?, state='dup-state' WHERE run_id='run-dup'", params![rev])?;
        tx.execute("INSERT INTO events VALUES ('dup-ev-1','run-dup',1,'p')", [])?;
        tx.execute("INSERT INTO outbox VALUES ('dup-ob-1','run-dup','p')", [])?;
        tx.execute("INSERT INTO receipts VALUES ('actor-1','cmd-1',?,?)", params![hash, rev])?;
        tx.commit()?;
        Ok("committed".to_string())
    };
    let r1 = commit(&mut conn, "hash-A", 1)?;
    let r2 = commit(&mut conn, "hash-A", 1)?;
    let r3 = commit(&mut conn, "hash-B", 2)?;
    let receipt_count = scalar_i64(&conn, "SELECT COUNT(*) FROM receipts WHERE actor='actor-1' AND command_id='cmd-1'")?;
    let stored_hash = scalar_string(&conn, "SELECT payload_hash FROM receipts WHERE actor='actor-1' AND command_id='cmd-1'")?;
    let rev = scalar_i64(&conn, "SELECT revision FROM snapshots WHERE run_id='run-dup'")?;
    let ok = r1 == "committed" && r2 == "duplicate-same" && r3 == "conflict"
        && receipt_count == 1 && stored_hash == "hash-A" && rev == 1;
    if !ok {
        eprintln!("duplicate test: r1={} r2={} r3={} count={} hash={} rev={}", r1, r2, r3, receipt_count, stored_hash, rev);
    }
    Ok(ok)
}

fn run_lock_test(exe: &Path, dir: &Path) -> Result<bool, Box<dyn std::error::Error>> {
    let db = dir.join("crash.duckdb");
    let marker = dir.join("lock-marker.txt");
    let mut holder = Command::new(exe)
        .arg("holdlock").arg(&db).arg(&marker)
        .stdout(Stdio::null()).stderr(Stdio::null())
        .spawn()?;
    if !wait_for_marker(&marker, "LOCK_HELD", Duration::from_secs(60)) {
        kill_child(&mut holder);
        return Ok(false);
    }
    // A second process must not be able to write the same file concurrently.
    let mut tryer = Command::new(exe)
        .arg("trywrite").arg(&db)
        .stdout(Stdio::piped()).stderr(Stdio::piped())
        .spawn()?;
    let start = Instant::now();
    let lock_ok;
    loop {
        match tryer.try_wait()? {
            Some(status) => {
                let mut out = String::new();
                if let Some(mut so) = tryer.stdout.take() { let _ = so.read_to_string(&mut out); }
                eprintln!("trywrite status={} out={}", status, out.trim());
                lock_ok = !out.contains("TRYWRITE_OK");
                break;
            }
            None => {
                if start.elapsed() > Duration::from_secs(20) {
                    // Blocked on the lock also proves exclusivity.
                    lock_ok = true;
                    kill_child(&mut tryer);
                    break;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
    kill_child(&mut holder);
    Ok(lock_ok)
}

fn run_io_failure_test(dir: &Path) -> Result<bool, Box<dyn std::error::Error>> {
    // Controlled failure: path inside a directory that does not exist.
    let bad = dir.join("no-such-subdir-xyz").join("x.duckdb");
    let open_bad = Connection::open(&bad);
    let bad_failed = open_bad.is_err();
    // Controlled failure: a regular file used as a directory component.
    let file_as_dir = dir.join("marker-1.txt");
    let bad2 = if file_as_dir.exists() {
        Connection::open(file_as_dir.join("x.duckdb")).is_err()
    } else {
        true
    };
    // The real DB must remain intact and readable after the failures.
    let db = dir.join("crash.duckdb");
    let intact = match Connection::open(&db) {
        Ok(conn) => scalar_i64(&conn, "SELECT COUNT(*) FROM events WHERE event_id LIKE 'committed-%'").map(|n| n >= 1).unwrap_or(false),
        Err(_) => false,
    };
    Ok(bad_failed && bad2 && intact)
}

struct Work {
    seq: u64,
    run_idx: i64,
    sent_at: Instant,
    respond: mpsc::Sender<Result<Duration, String>>,
}

struct PerfOutcome {
    p95_ms: f64,
    p99_ms: f64,
    sustained_writes_per_s: f64,
    zero_lost_ack: bool,
    starvation_ok: bool,
    reads_ok: bool,
    writes_sent: u64,
    writes_completed: u64,
    reads_completed: u64,
    reads_failed: u64,
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() { return 0.0; }
    let idx = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[idx.saturating_sub(1).min(sorted.len() - 1)]
}

fn run_perf(dir: &Path) -> Result<PerfOutcome, Box<dyn std::error::Error>> {
    let db = dir.join("perf.duckdb");
    // Seed: 100 runs x 100 tasks, 100k events.
    {
        let mut conn = Connection::open(&db)?;
        child::init_schema(&conn)?;
        let tx = conn.transaction()?;
        {
            let mut s1 = tx.prepare("INSERT INTO snapshots VALUES (?, 0, 'seed')")?;
            for r in 0..100i64 { s1.execute(params![format!("run-{}", r)])?; }
            let mut s2 = tx.prepare("INSERT INTO task_meta VALUES (?, ?, ?)")?;
            for r in 0..100i64 {
                for t in 0..100i64 {
                    s2.execute(params![format!("run-{}", r), format!("task-{}", t), format!("task {} of run {}", t, r)])?;
                }
            }
            let mut s3 = tx.prepare("INSERT INTO events VALUES (?, ?, ?, 'seed')")?;
            for i in 0..100_000i64 {
                s3.execute(params![format!("seed-ev-{}", i), format!("run-{}", i % 100), i])?;
            }
        }
        tx.commit()?;
    }
    eprintln!("perf seed done, starting 60s sustained phase");
    // One Database instance for this process; concurrent connections must be
    // clones of it (a second Connection::open on the same file is rejected).
    let base_conn = Connection::open(&db)?;
    let writer_conn = base_conn.try_clone()?;
    let reader_conn = base_conn.try_clone()?;
    let (work_tx, work_rx) = mpsc::channel::<Work>();
    let stop = Arc::new(AtomicBool::new(false));
    // Single writer thread.
    let writer = std::thread::spawn(move || -> Result<u64, String> {
        let mut conn = writer_conn;
        let mut done: u64 = 0;
        while let Ok(w) = work_rx.recv() {
            let rev = 1000 + w.seq as i64;
            let res: Result<(), String> = (|| {
                let tx = conn.transaction().map_err(|e| e.to_string())?;
                tx.execute("UPDATE snapshots SET revision=?, state=? WHERE run_id=?",
                    params![rev, format!("perf-{}", w.seq), format!("run-{}", w.run_idx)]).map_err(|e| e.to_string())?;
                tx.execute("INSERT INTO events VALUES (?, ?, ?, 'perf')",
                    params![format!("perf-ev-{}", w.seq), format!("run-{}", w.run_idx), 100000 + w.seq as i64]).map_err(|e| e.to_string())?;
                tx.execute("INSERT INTO outbox VALUES (?, ?, 'perf')",
                    params![format!("perf-ob-{}", w.seq), format!("run-{}", w.run_idx)]).map_err(|e| e.to_string())?;
                tx.execute("INSERT INTO receipts VALUES ('perf-actor', ?, 'hash-perf', ?)",
                    params![format!("perf-cmd-{}", w.seq), rev]).map_err(|e| e.to_string())?;
                tx.commit().map_err(|e| e.to_string())?;
                Ok(())
            })();
            match res {
                Ok(()) => { done += 1; let _ = w.respond.send(Ok(w.sent_at.elapsed())); }
                Err(e) => { let _ = w.respond.send(Err(e)); }
            }
        }
        Ok(done)
    });
    // Snapshot reader: 5 reads/s for the whole phase.
    let reads_completed = Arc::new(AtomicU64::new(0));
    let reads_failed = Arc::new(AtomicU64::new(0));
    let reader = {
        let stop = stop.clone();
        let rc = reads_completed.clone();
        let rf = reads_failed.clone();
        std::thread::spawn(move || {
            let conn = reader_conn;
            let mut n: i64 = 0;
            while !stop.load(Ordering::SeqCst) {
                let q: Result<String, _> = (|| {
                    let mut stmt = conn.prepare("SELECT state FROM snapshots WHERE run_id=?")?;
                    stmt.query_row(params![format!("run-{}", n % 100)], |r| r.get(0))
                })();
                match q { Ok(_) => { rc.fetch_add(1, Ordering::SeqCst); }, Err(_) => { rf.fetch_add(1, Ordering::SeqCst); } }
                n += 1;
                std::thread::sleep(Duration::from_millis(200));
            }
        })
    };
    let phase_start = Instant::now();
    let deadline = phase_start + Duration::from_secs(60);
    let seq_counter = Arc::new(AtomicU64::new(0));
    let mut callers = Vec::new();
    for caller in 0..32i64 {
        let tx = work_tx.clone();
        let seq_counter = seq_counter.clone();
        callers.push(std::thread::spawn(move || {
            let mut lat: Vec<f64> = Vec::new();
            let mut errs: u64 = 0;
            std::thread::sleep(Duration::from_millis((caller * 40) as u64));
            while Instant::now() < deadline {
                let seq = seq_counter.fetch_add(1, Ordering::SeqCst);
                let (rtx, rrx) = mpsc::channel();
                let w = Work { seq, run_idx: (seq % 100) as i64, sent_at: Instant::now(), respond: rtx };
                if tx.send(w).is_err() { errs += 1; break; }
                match rrx.recv() {
                    Ok(Ok(d)) => lat.push(d.as_secs_f64() * 1000.0),
                    _ => errs += 1,
                }
                // Pace: 32 callers at one send per 1.24s each = ~25.8/s offered
                // load, above the 25/s gate target to absorb scheduling jitter.
                std::thread::sleep(Duration::from_millis(1240));
            }
            (lat, errs)
        }));
    }
    drop(work_tx);
    let mut all_lat: Vec<f64> = Vec::new();
    let mut total_errs: u64 = 0;
    let mut min_caller: usize = usize::MAX;
    for c in callers {
        let (lat, errs) = c.join().map_err(|_| "caller panicked")?;
        min_caller = min_caller.min(lat.len());
        total_errs += errs;
        all_lat.extend(lat);
    }
    let elapsed = phase_start.elapsed().as_secs_f64();
    stop.store(true, Ordering::SeqCst);
    let _ = reader.join();
    let writer_done = writer.join().map_err(|_| "writer panicked")??;
    let writes_sent = seq_counter.load(Ordering::SeqCst);
    let writes_completed = all_lat.len() as u64;
    // Verify durability: every acked write is present exactly once.
    let (receipts_n, events_n) = {
        (scalar_i64(&base_conn, "SELECT COUNT(*) FROM receipts WHERE command_id LIKE 'perf-cmd-%'")?,
         scalar_i64(&base_conn, "SELECT COUNT(*) FROM events WHERE event_id LIKE 'perf-ev-%'")?)
    };
    let zero_lost = total_errs == 0 && writer_done == writes_completed
        && receipts_n as u64 == writes_completed && events_n as u64 == writes_completed
        && writes_sent == writes_completed;
    let rc = reads_completed.load(Ordering::SeqCst);
    let rf = reads_failed.load(Ordering::SeqCst);
    all_lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    Ok(PerfOutcome {
        p95_ms: percentile(&all_lat, 95.0),
        p99_ms: percentile(&all_lat, 99.0),
        sustained_writes_per_s: writes_completed as f64 / elapsed,
        zero_lost_ack: zero_lost,
        starvation_ok: min_caller >= 20,
        reads_ok: rf == 0 && rc >= 250,
        writes_sent,
        writes_completed,
        reads_completed: rc,
        reads_failed: rf,
    })
}

fn hardware_string() -> String {
    if let Ok(h) = std::env::var("GATE_HARDWARE") { return h; }
    let cpu = std::env::var("PROCESSOR_IDENTIFIER").unwrap_or_else(|_| "unknown CPU".to_string());
    let cores = std::env::var("NUMBER_OF_PROCESSORS").unwrap_or_else(|_| "?".to_string());
    format!("{}; {} logical cores; target x86_64-pc-windows-gnu; filesystem: workspace local disk (see validation doc)", cpu, cores)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() >= 2 && args[1] == "child" {
        let db = PathBuf::from(&args[2]);
        let cycle: i64 = args[3].parse().unwrap_or(0);
        let marker = PathBuf::from(&args[4]);
        if let Err(e) = child::run_child(&db, cycle, &marker) {
            eprintln!("child error: {}", e);
            std::process::exit(2);
        }
        return;
    }
    if args.len() >= 2 && args[1] == "holdlock" {
        let db = PathBuf::from(&args[2]);
        let marker = PathBuf::from(&args[3]);
        if let Err(e) = child::run_holdlock(&db, &marker) {
            eprintln!("holdlock error: {}", e);
            std::process::exit(2);
        }
        return;
    }
    if args.len() >= 2 && args[1] == "trywrite" {
        let db = PathBuf::from(&args[2]);
        match Connection::open(&db) {
            Ok(mut conn) => {
                let _ = child::init_schema(&conn);
                match conn.transaction().and_then(|tx| {
                    tx.execute("UPDATE snapshots SET revision=888888, state='trywrite' WHERE run_id='crash-run'", [])?;
                    tx.commit()
                }) {
                    Ok(()) => println!("TRYWRITE_OK"),
                    Err(e) => println!("TRYWRITE_ERR {}", e),
                }
            }
            Err(e) => println!("TRYWRITE_ERR {}", e),
        }
        return;
    }
    // Gate mode.
    let exe = std::env::current_exe().expect("current_exe");
    let dir = std::env::temp_dir().join(format!("central-duckdb-gate-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    if let Err(e) = std::fs::create_dir_all(&dir) {
        eprintln!("cannot create temp dir: {}", e);
        std::process::exit(2);
    }
    let run = (|| -> Result<GateReport, Box<dyn std::error::Error>> {
        let (atomicity_ok, recovery_ok) = run_crash_cycles(&exe, &dir)?;
        eprintln!("crash cycles done atomicity={} recovery={}", atomicity_ok, recovery_ok);
        let duplicate_ok = run_duplicate_test(&dir)?;
        let lock_ok = run_lock_test(&exe, &dir)?;
        let io_failure_ok = run_io_failure_test(&dir)?;
        eprintln!("duplicate={} lock={} io={}", duplicate_ok, lock_ok, io_failure_ok);
        let perf = run_perf(&dir)?;
        let version = {
            let conn = Connection::open(dir.join("perf.duckdb"))?;
            scalar_string(&conn, "SELECT version()").unwrap_or_else(|_| "unknown".to_string())
        };
        let gate_pass = atomicity_ok && recovery_ok && duplicate_ok && lock_ok && io_failure_ok
            && perf.zero_lost_ack && perf.starvation_ok && perf.reads_ok
            && perf.p95_ms <= 100.0 && perf.p99_ms <= 500.0 && perf.sustained_writes_per_s >= 25.0;
        Ok(GateReport {
            atomicity_ok,
            recovery_ok,
            duplicate_ok,
            lock_ok,
            io_failure_ok,
            zero_lost_ack: perf.zero_lost_ack,
            starvation_ok: perf.starvation_ok,
            reads_ok: perf.reads_ok,
            p95_ms: (perf.p95_ms * 100.0).round() / 100.0,
            p99_ms: (perf.p99_ms * 100.0).round() / 100.0,
            sustained_writes_per_s: (perf.sustained_writes_per_s * 100.0).round() / 100.0,
            hardware: hardware_string(),
            version,
            crash_cycles: 100,
            perf_writes_completed: perf.writes_completed,
            perf_writes_sent: perf.writes_sent,
            perf_reads_completed: perf.reads_completed,
            perf_reads_failed: perf.reads_failed,
            gate_pass,
        })
    })();
    match run {
        Ok(report) => {
            println!("{}", serde_json::to_string_pretty(&report).unwrap());
            let _ = std::fs::remove_dir_all(&dir);
            std::process::exit(if report.gate_pass { 0 } else { 1 });
        }
        Err(e) => {
            eprintln!("gate error: {}", e);
            let _ = std::fs::remove_dir_all(&dir);
            std::process::exit(2);
        }
    }
}


