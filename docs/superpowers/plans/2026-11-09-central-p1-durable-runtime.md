# Central P1 — Durable Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Có storage đã kiểm chứng, lõi Rust và CLI/daemon lưu được kế hoạch, quyết định, outbox; restart không tự giao việc.

**Architecture:** Pure domain -> application ports -> DuckDB writer; CLI gọi HTTP daemon. Fake agent chỉ ở test-support feature.

**Tech Stack:** Rust 1.99.1/MSVC, DuckDB 1.20000.1 bundled, Tokio/Axum/Clap/serde; versions trong plan tổng.

**Spec:** `docs/superpowers/specs/2026-11-09-central-mvp-design.md`; đọc cùng `2026-11-09-central-mvp.md`.

## Global Constraints

- Một daemon sở hữu storage; CLI không mở DuckDB.
- State/event/outbox/receipt nguyên tử; ack sau commit.
- Không đổi storage âm thầm khi DuckDB gate thất bại.
- Sau restart chờ `central resume`; chưa gọi agent thật trong P1.
- Domain không import Tokio/DB/HTTP/Codex; global contracts ở plan tổng áp dụng.

## Review Focus

1. Crash giữa state update và event insert -> rollback toàn bộ — T1,T3.
2. Cùng command ID khác payload -> conflict, không replay sai — T3.
3. Hai daemon tranh storage -> daemon thứ hai thất bại rõ — T4.
4. DB/IO failure -> không trả success hoặc dispatch — T3,T4.
5. Startup còn Dispatching -> UnknownOutcome, zero external send — T4.

### Task 1: Preflight và DuckDB suitability gate

**Files:** Create `rust-toolchain.toml`, `Cargo.toml`, `Cargo.lock`, `spikes/duckdb-runtime-gate/{Cargo.toml,src/main.rs,src/child.rs}`, `scripts/test-duckdb-gate.ps1`, `docs/validation/duckdb-gate.md`; extend `.gitignore` cho target/state, giữ spike cũ.
**Interfaces:** Consumes none. Produces JSON `GateReport { atomicity_ok, recovery_ok, duplicate_ok, p95_ms, p99_ms, sustained_writes_per_s, hardware, version }`; exit 0 chỉ khi mọi gate đạt.

- [ ] **1. Preflight:** kiểm tra Rust/MSVC/Node/Git, cài phần thiếu từ nguồn chính thức theo quyền môi trường. Pin toolchain. Nếu chưa có `.git`, `git init`; không reset/clean dữ liệu cũ. Chỉ tạo Cargo workspace tối thiểu cần cho spike.
- [ ] **2. Viết spike thật:** trong transaction update snapshot + append event/outbox/receipt; child báo marker trước/sau commit. Parent kill đúng child, reopen DB và kiểm tra uncommitted không xuất hiện, committed đã ack còn nguyên. 100 crash cycles; không gọi đó là power-loss test. Spike được miễn TDD sản phẩm nhưng assertions bắt buộc.
- [ ] **3. Đo release:** seed 100 run × 100 task metadata, 100k events; 32 caller, một writer, 25 durable transition/s trong 60s và 5 snapshot read/s. Gate: zero lost ack/partial commit, p95<=100ms, p99<=500ms gồm cả chờ queue; không starvation. Ghi hardware/filesystem, không suy thành SLA mọi máy.
- [ ] **4. Verify:** `pwsh -File scripts/test-duckdb-gate.ps1`; expected exit 0 và evidence đủ. Thử command lặp, lock và injected IO failure có kiểm soát, không làm đầy ổ thật. Fail gate -> dừng adoption, báo số đo/user; không tự thay SQLite.
- [ ] **5. Commit:** `git add` đúng file task rồi `git commit -m "test: validate DuckDB runtime suitability"`; không commit DB/binary.

### Task 2: Domain plan/approval và command contracts

**Files:** Create `crates/central-domain/{Cargo.toml,src/lib.rs,src/ids.rs,src/run.rs,src/plan.rs,src/profile.rs,src/command.rs,src/event.rs,src/decision.rs,src/error.rs,tests/plan_lifecycle.rs,tests/support/mod.rs}`, `contracts/plan-v1.schema.json`; modify root Cargo members.
**Interfaces:** Produces ID/state/decision ở plan tổng; `validate_plan(&PlanDocumentV1)->Result<PlanHash,DomainError>`; `Command::{CreateRun,ProposePlan,ApprovePlan,Pause,Stop,Resume}`. Task/attempt enums tồn tại dù scheduler chưa dispatch.

- [ ] **1. Write failing tests:** `approve_exact_revision`, `agent_cannot_approve`, `reject_cycle_or_missing_verify`, `pause_preserves_running_attempt`. Assertions: `assert_eq!(approved.active_plan,Some(h1)); assert_eq!(stale,Err(DomainError::StaleRevision)); assert_eq!(agent,Err(DomainError::Forbidden)); assert_eq!(paused.attempts[&a].status,AttemptStatus::Running);`. Setup helper thuộc tests/support, không gọi IO.
- [ ] **2. Run red:** `cargo test -p central-domain --test plan_lifecycle`; expected fail vì contracts/logic chưa implement.
- [ ] **3. Implement:** pure `decide`, strict versioned plan schema, canonical JSON hash với map ordering ổn định, timestamps sinh tự động ngoài hash; DAG/verify validation. Actor User/Leader/Worker/Verifier/System là identity nội bộ. Envelope expected revision và target plan hash kiểm tra riêng.
- [ ] **4. Run green:** cùng command; thêm empty plan, duplicate task ID, thay criteria trong nội dung được duyệt -> hash khác/conflict. `cargo metadata --no-deps --format-version 1` xác nhận domain không phụ thuộc runtime/adapter; fmt/clippy phù hợp.
- [ ] **5. Commit:** `feat: add plan approval domain and command contracts`.

### Task 3: Atomic store và application command handler

**Files:** Create `crates/central-application/{Cargo.toml,src/lib.rs,src/contracts.rs,src/runtime.rs,src/ports/{mod.rs,store.rs,agent.rs,process.rs,clock.rs,ids.rs},tests/command_runtime.rs}`, `crates/central-infrastructure/{Cargo.toml,src/lib.rs,src/storage/{mod.rs,duckdb_store.rs,writer.rs,codec.rs,migrations/001_runtime.sql},tests/store_contract.rs}`.
**Interfaces:** Produces Store/CommitBatch/Runtime::execute/DispatchError ở plan tổng; `DuckDbStore::open(&Path)->Result<Self,AppError>`. Tests dùng fake clock và AgentGateway có `send_count()`.

- [ ] **1. Write failing tests:** `atomic_commit_and_receipt`, `same_id_different_payload_conflicts`, `expected_revision_conflicts`, `failed_commit_never_dispatches`. After success: `assert_eq!(state.revision,Revision(1)); assert_eq!(events.len(),1); assert_eq!(intents.len(),1); assert_eq!(duplicate_receipt,first_receipt);`. Inject rollback -> state/events/intents/receipt đều không đổi; `assert_eq!(gateway.send_count(),0);`.
- [ ] **2. Run red:** `cargo test -p central-infrastructure --test store_contract`; `cargo test -p central-application --test command_runtime`.
- [ ] **3. Implement:** dedicated blocking writer qua bounded Tokio channel; không giữ transaction qua await. SQL: `schema_migrations(version PK)`, `runs(id PK,revision,snapshot_json)`, `events(run_id,seq,event_json,PK(run_id,seq))`, `outbox(id PK,run_id,intent_json,status)`, `command_receipts(actor_id,command_id,payload_hash,receipt_json,PK(actor_id,command_id))`. Parameter binding, CAS revision, actor+command dedup. Runtime validates domain -> commit, receipt replay trước decide; raw JSON codec không lan SQL vào application.
- [ ] **4. Run green:** hai suites trên, injected failure ở từng statement, reopen giữ cùng revision; `cargo fmt --check`; `cargo clippy --workspace --all-targets -- -D warnings`. Tests `newer_schema_fails_closed` và `corrupt_snapshot_is_not_empty_run`: unsupported schema/JSON hỏng trả lỗi, không reset/mất run. Outbox intent không chứng minh external effect thành công; ambiguous không được đánh Completed.
- [ ] **5. Commit:** `feat: persist atomic commands events and outbox with DuckDB`.

### Task 4: Daemon/API/CLI và restart barrier

**Files:** Create `crates/central-server/{Cargo.toml,src/main.rs,src/bootstrap.rs,src/http/{mod.rs,auth.rs,commands.rs,runs.rs},src/recovery.rs,tests/local_runtime.rs}`, `crates/central-cli/{Cargo.toml,src/main.rs,src/commands.rs,src/client.rs}`, `crates/central-infrastructure/src/process/{mod.rs,daemon.rs}`, `tests/fixtures/fake_agent.rs`, `scripts/test-runtime-recovery.ps1`.
**Interfaces:** Runtime::recover/dispatch_once từ plan tổng; `POST /v1/commands` CommandEnvelope, Actor từ token; `GET /v1/runs[/{id}]`, `/healthz`. Binary `central-server --state-dir PATH --listen 127.0.0.1:0`. CLI daemon start/status/shutdown, init/start/status/plan import/plan approve/pause/stop/resume.

- [ ] **1. Write failing tests:** `daemon_is_single_owner`, `recover_requires_explicit_resume`, `auth_cannot_be_claimed_in_body`, `storage_failure_is_not_success`: `assert_eq!(second_start.code,AlreadyRunning); assert_eq!(restored.control,AwaitingResume); assert_eq!(fake.sends,0); assert_eq!(bad_auth.status,401); assert_ne!(db_failure.status,200);`.
- [ ] **2. Run red:** `cargo test -p central-server --test local_runtime`. Child fixture create/import/approve/pause/restart qua API, không trực tiếp đổi DB để giả runtime.
- [ ] **3. Implement:** owner file lock trước mở DB; OS-owner-only token/server-info; loopback auth; command body max 1 MiB. CLI spawn daemon detached/hidden bằng Windows adapter, không shell string; đóng CLI không giết daemon. Startup nonterminal -> AwaitingResume; Dispatching thiếu receipt -> UnknownOutcome. Fake agent chỉ test-support feature, release không trả output giả.
- [ ] **4. Run green:** `pwsh -File scripts/test-runtime-recovery.ps1`: create -> approve -> crash -> reopen -> zero dispatch -> explicit resume -> fake workflow tiếp tục; event không lặp, independent run không mất. UI chưa build -> clear 503, không crash server. No live-model calls.
- [ ] **5. Commit:** `feat: expose durable local runtime through daemon and CLI`; lưu `docs/validation/p1-runtime.md`. Gate không được tuyên bố đã hoàn thành MVP hay tích hợp model thật.

## Trạng thái nghiệm thu 2026-11-11 (Linux fix rerun)

Evidence: `docs/validation/2026-11-11-linux-fix-rerun.md` (base `a00a2b5`,
3 file sửa chưa commit). Các checkbox gốc giữ nguyên lịch sử; dưới đây là
trạng thái mới, chỉ tick những gì phiên này chứng minh trực tiếp.

- [x] Suite xác định xanh trên Linux: `plan_lifecycle` 11,
  `command_runtime` 5, `recovery_dispatch` 5, `store_contract` 10,
  `local_runtime` 10; `cargo fmt --check` và clippy `-D warnings` đạt.
- [ ] DuckDB suitability gate không chạy lại trên host này (gate PowerShell
  Windows); evidence lịch sử trong `docs/validation/duckdb-gate.md` giữ nguyên.
- P1 không có nội dung live-model theo thiết kế; không tuyên bố gì thêm.
