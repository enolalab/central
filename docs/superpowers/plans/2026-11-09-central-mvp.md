# Central MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Xây Central local bằng Rust, điều phối Codex theo kế hoạch user duyệt, lưu tiến độ và khôi phục an toàn, với web UI văn phòng pixel.

**Architecture:** Domain/application độc lập database, harness và UI. Một daemon sở hữu DuckDB; Codex app-server, Git và filesystem là adapter. CLI/web dùng cùng API; Central không có model/tool loop.

**Tech Stack:** Rust 1.99.1/MSVC, duckdb 1.20000.1 bundled, Tokio/Axum/Clap; Codex app-server JSONL/stdio; React/TypeScript/Vite và Canvas 2D.

**Spec:** `docs/superpowers/specs/2026-11-09-central-mvp-design.md`; visual reference `docs/superpowers/mockups/central-immersive-office-v1.html`.

## Global Constraints

- Local-first, Rust, cá nhân trước và có khả năng mở rộng.
- Central không phải agent hay harness.
- Mọi sửa đổi kế hoạch cần user duyệt; chat không tự cấp approval.
- Hai vòng sửa sau lần verify đầu tiên, cấu hình được theo task.
- Một ngân sách chung cho mọi vai trò; MVP không có quỹ dự phòng.
- Usage không xác định khi có giới hạn tiền/token: giữ chờ việc mới trong run đó.
- Sau restart chờ `central resume`; pause/hold để attempt đang chạy hoàn thành.
- Windows x64 trước; không HA, worker remote hoặc RBAC tổ chức trong MVP.
- Văn phòng pixel toàn màn hình; panel/chat theo ngữ cảnh, không dashboard cố định.
- Real-model test mặc định gpt-6-luna/low; không fallback model âm thầm.
- Chỉ docs được viết trong lượt lập plan; mọi checkbox dưới đây chưa thực hiện.

## Review Focus

1. Crash sau send nhưng trước receipt: không resend mù — P1/T3, P3/T6.
2. Request ID tái dùng sau harness restart: accept cũ không áp dụng — P2/T3.
3. Windows Unicode/spaced path, junction thoát root: không ghi nhầm — P3/T5.
4. Usage thiếu/đến trễ/cumulative lặp: hold đúng run, không tính hai lần — P3/T3.
5. Tab stale duyệt revision cũ: conflict, không dispatch — P4/T1,T4.

## Chặng và đầu ra độc lập

| Plan | Phụ thuộc | Deliverable |
| --- | --- | --- |
| [P1 — Durable runtime](2026-11-09-central-p1-durable-runtime.md) | Không | DuckDB gate, domain/transaction, daemon/API/CLI, restart qua fake agent |
| [P2 — Codex workflow](2026-11-09-central-p2-codex-workflow.md) | P1 | Leader proposal, worker/verifier thật, approval/question và evidence |
| [P3 — Reliable workflows](2026-11-09-central-p3-reliable-workflows.md) | P2 | DAG đồng thời, budget/retry/revision, Git integration và recovery |
| [P4 — Immersive office](2026-11-09-central-p4-office-and-release.md) | P3 | UI theo mockup, đóng gói Windows và nghiệm thu sản phẩm |

Thực thi tuần tự; UI shell có thể dựng với fixture trước nhưng không nghiệm thu
trước runtime. Không chốt DuckDB khi P1/T1 chưa đạt. Mỗi task có commit riêng.
Không tạo hai DB backend để chứng minh abstraction. Unit/contract tests không gọi model.

## Hiện trạng và versions dự kiến

Repo mới có docs/mockup/spikes, chưa có Cargo workspace/Git repo. Trước đó chưa
có Rust/Cargo trong PATH. Codex CLI 0.167.0-alpha.3.2 đã đăng nhập ChatGPT; Node
24.22.1. Kiểm tra lại khi execution; không đổi auth/config cá nhân.

Toolchain: Rust 1.99.1, target x86_64-pc-windows-msvc, MSVC C++ build tools cho
DuckDB bundled; Node 24.22.1 và pnpm 10 (lock patch thực tế khi bootstrap).
Nếu thiếu thì cài nguồn chính thức theo quyền môi trường, không bỏ sandbox để lách lỗi.

Versions được tra registry ngày 2026-11-09; chưa có nghĩa đã build tương thích:

| Dependency | Version dự kiến |
| --- | --- |
| duckdb | `=1.20000.1`, feature `bundled` |
| tokio / axum / clap | `1.54.1` / `0.8.9` / `4.6.7` |
| serde / serde_json | `1.0.229` / `1.0.154` |
| reqwest | `0.13.5`, CLI HTTP client, không gọi model |
| fs2 / uuid / sha2 / tempfile | `0.4.3` / `1.29.2` / `0.11.1` / `3.27.0` |
| React / React DOM | `19.3.1` |
| Vite / TypeScript / Vitest / Playwright | `8.3.6` / `7.0.2` / `5.1.0` / `1.65.0` |

Chỉ thêm async-trait/thiserror/windows-sys/tracing/tower-http/serde_yaml khi task
cần; pin resolution trong lockfiles. Nếu version không tương thích, ghi evidence
và chỉnh dependency trong task, không giả compatibility đã được chứng minh.

## Bản đồ file

```text
Cargo.toml / Cargo.lock / rust-toolchain.toml
crates/central-domain/src/        # IDs/state/commands/events, pure invariants
crates/central-application/src/   # contracts/ports/use cases/scheduler
crates/central-infrastructure/src/
  storage/                       # DuckDB writer, SQL, codec
  codex/                         # schema/version, stdio RPC, mapping
  process/                       # daemon, ownership, Windows Job Objects
  workspace/                     # snapshot/worktree/integration
  artifacts/                     # immutable content-addressed filesystem
  checks/                        # lệnh verify đã duyệt, không agent loop
crates/central-server/src/       # composition root, runtime, HTTP/read models
crates/central-cli/src/          # parse CLI/API client; không DB
web/src/                        # React state, scene, contextual panels
contracts/                      # plan/result schemas, Codex schema subset
tests/fixtures/                 # tiny Git repo, RPC transcripts, fake harness
scripts/                        # gates, packaging, opt-in model smoke
docs/validation/                # evidence theo gate
```

Domain không import Tokio/DB/HTTP/Codex. Application dùng async ports với
async-trait khi cần Arc<dyn Port>; infrastructure implement. Không trait CRUD
cho mọi bảng hoặc module chưa có consumer.

## Contracts dùng chung giữa các chặng

Định nghĩa ở P1/T2–T3; task sau chỉ thêm variant có owner, không đổi tên tự phát.
Tất cả path trong subplan là repo-relative.

### Domain

- ID newtype: RunId/TaskId/AttemptId/CommandId/IntentId/ArtifactId/ActorId/ProfileId
  chứa UUID;
  PlanHash/ContentHash là hex SHA-256; Revision(u64); Timestamp(i64) UTC ms.
- RunState có id/revision/goal/repository/control/active_plan/plans/tasks/attempts/
  inbox/artifacts/usage/holds. BTreeMap cho ordering ổn định, persisted version.
- RunControl: Running/Paused/Stopped/AwaitingResume. Holds độc lập: BudgetExceeded,
  UsageUnknown, PlanImpact, ResourceConflict; resume không xóa mọi hold.
- TaskStatus: Pending/Ready/Running/AwaitingVerification/Succeeded/Blocked/Failed/Cancelled.
- AttemptStatus: Queued/Dispatching/Running/WaitingForApproval/WaitingForInput/
  Reconciling/UnknownOutcome/Completed/Failed/Cancelled.
- VerificationOutcome: Pass/Fail/Blocked. Observation có attempt/provider epoch/dedup key.
- ResolvedProfile được định nghĩa kiểu dữ liệu ở P1/T2: id/provider/model/effort/
  skills/subagents/permissions/fallbacks/hash. Policy validation đến P2/T2, không
  tạo hai type cùng tên trong domain và application.
- `decide(&RunState, &Actor, &Command, Timestamp) -> Result<Decision, DomainError>`;
  `Decision { next: RunState, events: Vec<Event>, effects: Vec<Effect> }`, zero IO.
- `CommandEnvelope { id: CommandId, expected_revision: Option<Revision>, command: Command }`.
  Actor được suy ra từ auth/session mapping, không tin body tự khai.

### Application ports

Types đặt ở `central-application/src/contracts.rs`; trait files ở `src/ports/`.
Async trait dùng async-trait để object-safe:

```rust
trait Store {
    async fn load(&self, id: RunId) -> Result<Option<RunState>, AppError>;
    async fn receipt(&self, actor: ActorId, id: CommandId) -> Result<Option<Receipt>, AppError>;
    async fn commit(&self, batch: CommitBatch) -> Result<Receipt, AppError>;
    async fn pending(&self, id: RunId) -> Result<Vec<OutboxIntent>, AppError>;
}
trait AgentGateway {
    async fn capabilities(&self, profile: &ResolvedProfile) -> Result<Capabilities, AppError>;
    async fn open(&self, request: OpenSession) -> Result<SessionRef, AppError>;
    async fn send(&self, request: SendTurn) -> Result<TurnRef, DispatchError>;
    async fn observe(&self, session: &SessionRef) -> Result<Vec<Observation>, AppError>;
    async fn answer(&self, answer: ExternalAnswer) -> Result<(), DispatchError>;
    async fn cancel(&self, target: &ExecutionRef) -> Result<CancelObservation, AppError>;
    async fn reconcile(&self, target: &ExecutionRef) -> Result<ReconcileEvidence, AppError>;
}
```

`observe` drains bounded notification buffer, không poll inference. Overflow hay
mất kết nối -> ReconcileRequired, không bỏ event quan trọng trong im lặng.

- `Runtime::execute(&self, Actor, CommandEnvelope)->Result<Receipt,AppError>`.
- `Runtime::dispatch_once(&self, RunId)->Result<DispatchSummary,AppError>`.
- `Runtime::recover(&self)->Result<RecoverySummary,AppError>`.
- CommitBatch: actor/command_id/payload_hash/expected_revision/next_state/events/
  intents/receipt; ghi nguyên tử. Duplicate actor+command ID khác payload -> conflict.
- DispatchError: DefinitelyNotSent/OutcomeUnknown/Rejected. Transport mất sau send
  mặc định OutcomeUnknown; không có exactly-once từ outbox.
- SessionRef: provider/instance_epoch/thread_id. TurnRef: session/turn_id.
  ExternalRequestKey: session/turn_id/item_id/request_id.
- OpenSession: attempt_id/profile/workspace/resume_from:Option<SessionRef>;
  SendTurn: session/attempt_id/input/output_schema/command_id;
  ExecutionRef: attempt_id/session/turn:Option<TurnRef>. AppError có Validation,
  Conflict, Forbidden, Storage, Capability, Transport, NotFound; Receipt có
  command_id/run_id/revision/result, không credentials.
- WorkspaceManager async snapshot/create/seal/prepare_integration/promote; P3/T5
  định nghĩa request/result. ArtifactStore publish/read; CheckRunner run lệnh đã
  duyệt. ProcessSupervisor spawn/inspect/terminate bằng ProcessIdentity gồm PID,
  creation time và ownership/job ID. Clock/IdSource có fake deterministic.

Mọi signature IO viết rút gọn trong subplan được hiểu là async và trả
`Result<T,AppError>` trừ khi đã ghi DispatchError/RpcError. Các struct request/
result của port được khai báo ở contracts trước consumer đầu tiên; P1/T3 tạo
ProcessSupervisor trait, P2/T2 mới tạo Windows implementation. Không có Windows
handle trong application contracts.

Test fixture `Fixture::approved_run()` tạo plan h1 với a/b độc lập, c phụ thuộc
output của a, fake clock/IDs/profile. Mỗi crate tự sở hữu helper không có
dependency test ngược từ domain sang application. Mẫu assertions trong subplan
là phần cần chứng minh, implementer viết setup đúng contracts này.

## State machine cố định

| Trigger | Transition/guard |
| --- | --- |
| Approve plan | Active revision đổi đúng hash; stale -> conflict |
| Dispatch | Task Ready->Running; Attempt Queued->Dispatching->Running sau receipt |
| Ambiguous send | Attempt UnknownOutcome; không resend, descendants chờ |
| Approval/question | Running->WaitingForApproval/Input; response phải còn hiệu lực |
| Worker candidate | Attempt Completed; Task AwaitingVerification, chưa Succeeded |
| Verifier pass | Task Succeeded nếu input/candidate/criteria khớp; stale -> reverify |
| Verifier fail | Dưới 2 vòng sửa -> retry chất lượng; đủ 2 -> leader |
| Verifier blocked | Blocked, không tăng vòng sửa chất lượng |
| Pause/budget/unknown usage | Đóng gate mới, giữ đang chạy |
| Stop | Đóng gate, yêu cầu cancel; chưa rõ -> unknown |
| Daemon startup | Nonterminal run AwaitingResume; zero new inference |
| Resume | Bỏ AwaitingResume/Paused; không xóa UsageUnknown/BudgetExceeded |
| Plan đổi khi đang chạy | Attempt giữ revision cũ; output cần assessment trước reuse |

## Execution handoff

### Tự rà soát plan (không phải implementation tests đã chạy)

| Spec | Task bao phủ |
| --- | --- |
| Mục 1–5: ranh giới, Clean Architecture, storage choice | P1/T1–T4, dependency check |
| Mục 6–7: model, proposal/approval, revision | P1/T2, P2/T4, P3/T4 |
| Mục 8: DAG/control/retry | P3/T1–T3,T6 |
| Mục 9: atomicity/outbox/artifact/recovery | P1/T1,T3,T4; P2/T5; P3/T6 |
| Mục 10: adapter/profile/capability/subagent | P2/T1–T4, P3/T3 |
| Mục 11: verification/workspace/integration | P2/T5, P3/T2,T4,T5 |
| Mục 12: inbox/quyền/secret/stale answer | P2/T3, P4/T1,T4 |
| Mục 13: budget/unknown/observability | P3/T3, P4/T1,T5 |
| Mục 14: CLI/browser office | P1/T4, P4/T1–T6 |
| Mục 15–18: giới hạn, nghiệm thu, version evidence | Gates từng chặng, P4/T6 |

Đã rà tên types/signatures và ordering: profile DTO/process port định nghĩa
trước consumer; live stdio smoke sau supervisor; production Git workspaces ở P3,
P2 chỉ chạy fixture-owned scratch. Secret payload không vào durable outbox;
dispatched-unknown và usage-unknown là hai điều kiện khác nhau. Mỗi Review Focus
có test ở task sở hữu. Bốn subplan có 21 task, tất cả có red/green/evidence và
commit (storage spike dùng experiment assertions thay vì TDD sản phẩm).
Các benchmark/build/test/model-call trong plan đều **chưa thực hiện**.

Sau mỗi chặng chạy gate, lưu evidence/giới hạn và commit. Không đưa fake mockup
actions vào release. Hai cách thực thi sau review: native (một implementer tuần
tự, reviewer độc lập cuối nhánh), hoặc subagent-driven (review theo task).
Đề xuất native cho P1 để tiết kiệm context và ổn định interfaces; chỉ delegate
khi user chọn. Yêu cầu viết plan chưa bắt đầu execution.

Nguồn: [Rust stable channel](https://static.rust-lang.org/dist/channel-rust-stable.toml),
[DuckDB crate metadata](https://crates.io/api/v1/crates/duckdb/1.20000.1),
[DuckDB concurrency](https://duckdb.org/docs/2.0/connect/concurrency),
[Codex protocol](https://learn.chatgpt.com/docs/app-server); frontend versions từ
npm registry. Bằng chứng local: `spikes/codex-lifecycle/CRASH-FINDINGS.md` và
`spikes/codex-app-server/FINDINGS.md`.
