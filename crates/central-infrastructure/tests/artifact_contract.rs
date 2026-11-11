//! P2 Task 5: immutable artifact store contract (TDD: RED first).
//!
//! Blobs are content-addressed and immutable: a publish stages to a temp
//! file, hashes, flushes and renames, and a read re-hashes before trusting.
//! An interrupted publish leaves at most an orphan blob, which is never a
//! committed artifact and is removed explicitly; referenced blobs are never
//! deleted. Checks run only approved program+argv.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use central_application::ports::artifacts::{ArtifactStore, PublishArtifact};
#[cfg(windows)]
use central_application::ports::checks::{CheckRequest, CheckRunner};
use central_domain::{AttemptId, ContentHash, TaskId};
use central_infrastructure::artifacts::FilesystemArtifactStore;
#[cfg(windows)]
use central_infrastructure::checks::{ApprovedCommand, SupervisedCheckRunner};
use sha2::{Digest, Sha256};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn scratch_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "central-{}-{}-{}",
        name,
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("clear stale scratch");
    }
    root
}

fn content_hash(bytes: &[u8]) -> ContentHash {
    let digest = Sha256::digest(bytes);
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        hex.push_str(&format!("{byte:02x}"));
    }
    ContentHash::from_hex(hex)
}

fn publish_request(path: &str, bytes: &[u8], expected: Option<ContentHash>) -> PublishArtifact {
    PublishArtifact {
        attempt_id: AttemptId::from_u128(9_201),
        task_id: Some(TaskId::from_u128(9_101)),
        path: path.to_string(),
        kind: "candidate".to_string(),
        bytes: bytes.to_vec(),
        expected_hash: expected,
        provenance: None,
    }
}

fn blob_count(store: &FilesystemArtifactStore) -> usize {
    let mut count = 0;
    let entries = std::fs::read_dir(store.blobs_dir()).expect("list blobs");
    for entry in entries {
        let entry = entry.expect("blob entry");
        if entry.file_type().expect("file type").is_file() {
            count += 1;
        }
    }
    count
}

#[tokio::test]
async fn artifact_root_and_hash_checked() {
    let root = scratch_root("artifact-contract");
    let store = FilesystemArtifactStore::open(&root).expect("open store");
    let bytes = b"candidate-bytes for one fixture function";
    let expected = content_hash(bytes);

    // Happy path: publish commits, read returns the exact bytes and hash.
    let committed = store
        .publish(publish_request(
            "candidate/patch.bin",
            bytes,
            Some(expected.clone()),
        ))
        .await
        .expect("publish");
    assert_eq!(committed.hash, expected);
    let read_back = store.read(committed.id).await.expect("read");
    assert_eq!(read_back.bytes, bytes);
    assert_eq!(read_back.hash, expected);

    // A path escaping the run root is refused before anything is written.
    let escape = store
        .publish(publish_request("../escape.bin", bytes, None))
        .await;
    assert!(escape.is_err());
    let rooted = store
        .publish(publish_request("/abs/root.bin", bytes, None))
        .await;
    assert!(rooted.is_err());

    // A wrong expected hash aborts the publish and leaves no blob behind.
    let wrong = store
        .publish(publish_request(
            "candidate/wrong.bin",
            bytes,
            Some(ContentHash::from_hex(
                "0000000000000000000000000000000000000000000000000000000000000000",
            )),
        ))
        .await;
    assert!(wrong.is_err());
    assert_eq!(blob_count(&store), 1);

    // A blob tampered with after commit fails the re-hash on read.
    let blob_path = store.blobs_dir().join(expected.as_str());
    std::fs::write(&blob_path, b"tampered").expect("tamper blob");
    assert!(store.read(committed.id).await.is_err());

    std::fs::remove_dir_all(&root).expect("clean scratch");
}

#[tokio::test]
async fn orphan_is_not_committed_artifact() {
    let root = scratch_root("artifact-orphan");
    let store = FilesystemArtifactStore::open(&root).expect("open store");

    // A crashed publish leaves a blob with no committed metadata: it is not
    // a committed artifact, and the explicit sweep removes exactly it.
    let _orphan = store.import_orphan_blob(b"orphan-bytes").expect("orphan");
    assert_eq!(store.sweep_orphans().await.expect("sweep"), 1);
    assert_eq!(store.sweep_orphans().await.expect("sweep"), 0);

    // A committed blob is referenced: the sweep keeps it and reads still pass.
    let bytes = b"committed candidate";
    let committed = store
        .publish(publish_request(
            "candidate/kept.bin",
            bytes,
            Some(content_hash(bytes)),
        ))
        .await
        .expect("publish");
    assert_eq!(store.sweep_orphans().await.expect("sweep"), 0);
    let read_back = store.read(committed.id).await.expect("read");
    assert_eq!(read_back.bytes, bytes);

    std::fs::remove_dir_all(&root).expect("clean scratch");
}

#[cfg(windows)]
#[tokio::test]
async fn unapproved_program_is_forbidden() {
    let root = scratch_root("check-allowlist");
    std::fs::create_dir_all(&root).expect("work root");
    let runner = SupervisedCheckRunner::new(
        central_domain::PlanHash::from_hex("plan TASK-5"),
        vec![ApprovedCommand {
            program: "cmd.exe".to_string(),
            args: vec!["/C".to_string(), "exit 0".to_string()],
        }],
        root.clone(),
    )
    .expect("runner");

    // Same program, different argv: not the approved command.
    let wrong_argv = CheckRequest {
        approved_plan_hash: central_domain::PlanHash::from_hex("plan TASK-5"),
        program: "cmd.exe".to_string(),
        args: vec!["/C".to_string(), "exit 1".to_string()],
        cwd: ".".to_string(),
        timeout_ms: 10_000,
        allowed_write_roots: Vec::new(),
    };
    assert!(runner.run(wrong_argv).await.is_err());

    // Unknown program: never runs.
    let unknown = CheckRequest {
        approved_plan_hash: central_domain::PlanHash::from_hex("plan TASK-5"),
        program: "powershell.exe".to_string(),
        args: Vec::new(),
        cwd: ".".to_string(),
        timeout_ms: 10_000,
        allowed_write_roots: Vec::new(),
    };
    assert!(runner.run(unknown).await.is_err());

    // A cwd escaping the work root is refused even for an approved command.
    let escape = CheckRequest {
        approved_plan_hash: central_domain::PlanHash::from_hex("plan TASK-5"),
        program: "cmd.exe".to_string(),
        args: vec!["/C".to_string(), "exit 0".to_string()],
        cwd: "../escape".to_string(),
        timeout_ms: 10_000,
        allowed_write_roots: Vec::new(),
    };
    assert!(runner.run(escape).await.is_err());

    std::fs::remove_dir_all(&root).expect("clean scratch");
}

#[cfg(windows)]
#[tokio::test]
async fn mismatched_plan_hash_is_rejected() {
    let root = scratch_root("check-plan-hash");
    std::fs::create_dir_all(&root).expect("work root");
    let runner = SupervisedCheckRunner::new(
        central_domain::PlanHash::from_hex("plan TASK-5"),
        vec![ApprovedCommand {
            program: "cmd.exe".to_string(),
            args: vec!["/C".to_string(), "echo hi".to_string()],
        }],
        root.clone(),
    )
    .expect("runner");

    // Same program, same argv, but a request naming a different plan: the
    // runner must refuse without spawning anything.
    let other_plan = runner
        .run(CheckRequest {
            approved_plan_hash: central_domain::PlanHash::from_hex("plan OTHER-PLAN"),
            program: "cmd.exe".to_string(),
            args: vec!["/C".to_string(), "echo hi".to_string()],
            cwd: ".".to_string(),
            timeout_ms: 10_000,
            allowed_write_roots: Vec::new(),
        })
        .await;
    assert!(
        other_plan.is_err(),
        "a request naming a different plan must be rejected even with identical argv"
    );

    std::fs::remove_dir_all(&root).expect("clean scratch");
}

#[cfg(windows)]
#[tokio::test]
async fn approved_check_captures_evidence() {
    let root = scratch_root("check-evidence");
    std::fs::create_dir_all(&root).expect("work root");
    let runner = SupervisedCheckRunner::new(
        central_domain::PlanHash::from_hex("plan TASK-5"),
        vec![ApprovedCommand {
            program: "cmd.exe".to_string(),
            args: vec!["/C".to_string(), "echo hi".to_string()],
        }],
        root.clone(),
    )
    .expect("runner");
    let evidence = runner
        .run(CheckRequest {
            approved_plan_hash: central_domain::PlanHash::from_hex("plan TASK-5"),
            program: "cmd.exe".to_string(),
            args: vec!["/C".to_string(), "echo hi".to_string()],
            cwd: ".".to_string(),
            timeout_ms: 10_000,
            allowed_write_roots: Vec::new(),
        })
        .await
        .expect("approved check runs");
    assert_eq!(evidence.exit_code, 0);
    assert!(!evidence.timed_out);
    assert!(evidence.stdout_tail.contains("hi"));
    assert_eq!(
        evidence.stdout_hash,
        content_hash("hi\r\n".as_bytes()),
        "evidence hashes the exact captured stdout"
    );

    std::fs::remove_dir_all(&root).expect("clean scratch");
}
