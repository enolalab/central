# Central P3 — Reliable Workflows Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Điều phối DAG nhiều agent, verify/retry, budget/fallback, revision và Git integration; khôi phục có đối soát và bằng chứng.

**Architecture:** Scheduler/use cases quyết định thuần theo snapshot; side effect đi qua outbox và ports. Git/Codex/DB không được quyết định nghiệp vụ thay application.

**Tech Stack:** Rust domain/application, DuckDB, Codex adapter P2, Git CLI qua argv, deterministic clock/failure injection.

**Spec:** `docs/superpowers/specs/2026-11-09-central-mvp-design.md`; contracts trong `2026-11-09-central-mvp.md`; prerequisites P1/P2.

## Global Constraints

- Dependency chỉ mở sau nghiệm thu đúng input/candidate/criteria, không chỉ exit 0.
- Mặc định verifier khác phiên; tối đa 2 vòng sửa sau verification đầu.
- Một budget chung; usage thiếu sau interruption giữ chờ việc mới đúng run.
- Task đang chạy theo plan cũ được hoàn thành, output phải assessment trước reuse.
- Mỗi implementation attempt có workspace riêng; user chỉ sync khi yêu cầu.
- Unknown outcome không retry mù; stop không đồng nghĩa rollback.

## Review Focus

1. Hai scheduler tick tranh cùng task/resource -> chỉ một attempt — T1.
2. Verifier dùng evidence cũ hoặc sửa candidate -> không pass — T2,T5.
3. Usage cumulative lặp/missing-final và fallback -> không né budget — T3.
4. Plan revision mới khi output cũ đến muộn -> quarantine/assessment — T4.
5. Git promote xong nhưng DB receipt mất -> đối soát ref, không ghép lặp — T5,T6.

### Task 1: DAG scheduling, concurrency và tài nguyên

**Files:** Create `crates/central-domain/src/{dependency.rs,scheduling.rs}`, `crates/central-application/src/{scheduler.rs,dispatcher.rs}`, `crates/central-application/tests/scheduler.rs`; extend Command/Observation/Runtime and fake gateway.
**Interfaces:** `eligible_tasks(&RunState,Timestamp)->Vec<TaskId>`; `Scheduler::tick(RunId)->Result<ScheduleReport,AppError>` dùng Runtime::execute; `ResourceClaim { key:String, mode:Exclusive }`; typed order/data dependency có expected output hash. `DispatchSummary` counts sent/unknown/deferred.

- [ ] **1. Tests:** `two_ticks_do_not_duplicate`, `dependency_needs_verified_output`, `resource_is_exclusive`, `pause_keeps_existing_attempt`: `assert_eq!(a_attempts,1); assert_eq!(max_parallel,2); assert!(!eligible.contains(&c_before_a_pass)); assert_eq!(running_after_pause,running_before);`. Independent b vẫn ready khi a unknown; named port/db resource giữ độc quyền.
- [ ] **2. Run red:** `cargo test -p central-application --test scheduler`; expected fail trước scheduler implementation.
- [ ] **3. Implement:** topo-ready selection với tie-break TaskId ổn định; CAS run revision và resource claim khi queue attempt. Dispatch rechecks control/plan/capability/budget và resolves named resource config; không held transaction khi gọi agent. Resource claim tied attempt và chỉ release khi terminal đã xác minh; unknown giữ claim. Daemon scan eligible định kỳ và khi event mới; không scheduling theo animation.
- [ ] **4. Run green:** fake gateway delay và concurrent tick test không dùng sleep thời gian thật; property-style shuffled completion vẫn giữ invariants. New input không tự thay input của attempt đang chạy.
- [ ] **5. Commit:** `feat: schedule dependency workflows with bounded concurrency`.

### Task 2: Verification rounds và leader escalation

**Files:** Create `crates/central-domain/src/quality.rs`, `crates/central-application/src/quality_loop.rs`, `crates/central-application/tests/quality_loop.rs`; extend verification/result contracts and server read models.
**Interfaces:** `route_verification(&RunState,&VerificationV1)->Result<QualityDecision,DomainError>`; QualityDecision Accepted/Repair{round:u8}/Blocked/AskLeader/Disputed. Command adds SubmitVerification/DisputeVerification; criteria includes exact hash and environment/check references.

- [ ] **1. Tests:** `two_repairs_then_leader`, `blocked_does_not_consume_round`, `leader_cannot_override_fail`, `stale_evidence_cannot_pass`: `assert_eq!(routes,[Repair{round:1},Repair{round:2},AskLeader]); assert_eq!(after_blocked.repair_round,0); assert_eq!(leader_force_pass,Err(Forbidden)); assert_ne!(stale_task.status,Succeeded);` verifier same session bị từ chối trừ user override theo task.
- [ ] **2. Run red:** `cargo test -p central-application --test quality_loop`.
- [ ] **3. Implement:** repair tạo attempt mới liên kết predecessor; verifier nêu failed criteria/evidence; blocked riêng với quality fail. Leader escalation là work item đi qua scheduler/budget, không gọi model từ domain hoặc bypass gate. Dispute tạo independent reviewer nếu trong allowance đã duyệt; nếu không chuyển user. Verification report không sinh vòng verifier-of-verifier.
- [ ] **4. Run green:** candidate changed sau pass -> stale; verifier sửa tracked source -> invalid evidence. User chỉnh tiêu chí phải qua plan revision, không update verdict lịch sử. Positive fixture fail->repair->pass mở downstream đúng một lần.
- [ ] **5. Commit:** `feat: enforce independent verification and bounded repair loops`.

### Task 3: Shared budget, classified retry và fallback

**Files:** Create `crates/central-domain/src/{budget.rs,retry.rs}`, `crates/central-application/src/{usage.rs,retries.rs,fallback.rs}`, `crates/central-application/tests/{budget.rs,retry_fallback.rs}`; extend run limits, profile policy, CLI budget commands.
**Interfaces:** `admission(&RunState,&WorkRequest)->Admission::{Allowed,Held(HoldReason)}`; `apply_usage(&mut UsageLedger,UsageObservation)->Result<(),DomainError>`; `choose_fallback(&ResolvedProfile,&Failure,&Capabilities)->Option<ProfileId>`. UsageObservation has provider/epoch/turn/sequence/cumulative/finality/source; Command adds SetLimits/ReconcileUsage/ResolveUsageGap (User only).

- [ ] **1. Tests:** `unknown_usage_holds_only_affected_run`, `resume_does_not_clear_unknown`, `cumulative_usage_not_double_counted`, `fallback_keeps_permissions_and_budget`, `quota_has_no_retry_storm`: `assert_eq!(run_a.admission,Held(UsageUnknown)); assert_eq!(run_b.admission,Allowed); assert_eq!(observed_tokens,120); assert_eq!(new_fallback_attempts,0)` khi còn old unknown hoặc budget hết. Retry clock kiểm tra đủ delay, không sleep thật.
- [ ] **2. Run red:** `cargo test -p central-application --test budget --test retry_fallback`.
- [ ] **3. Implement:** một ledger cho mọi role, count/token/time limits + optional user-configured monetary estimate. Phân biệt live InFlight chưa có final với MissingFinal sau crash; InFlight là ongoing cost theo soft threshold, không giả đã có tổng cuối. MissingFinal/unsupported metric cần cap -> hold; không tự điền 0/estimate để giải phóng. User resolve có audit/source, vẫn giữ observed thiếu dưới dạng thiếu. Không cộng cached thêm vào input. Hết limit giữ đang chạy, chặn mọi attempt mới kể cả leader/verifier.
- [ ] **4. Implement retry policy:** safe DefinitelyNotSent tối đa 3 lần, delay 2s/10s/30s (default triển khai có thể cấu hình trong plan); OutcomeUnknown không tự retry. Quota/auth -> fallback chain user đã cấp, confirmed old stop, new attempt + notification; profile không đáp ứng capability/quyền không được chọn. Không giữ quỹ riêng.
- [ ] **5. Run green:** hai suites ở bước 2; old cumulative event không giảm số đã biết hoặc xóa missing-final khi chưa có bằng chứng. ResolveUsageGap user audit không đổi estimated thành observed.
- [ ] **6. Commit:** `feat: gate retries and profile fallback on shared budget`.

### Task 4: Revision impact, stale outputs và reuse assessment

**Files:** Create `crates/central-domain/src/revision.rs`, `crates/central-application/src/revisions.rs`, `contracts/plan-change-v1.schema.json`, `crates/central-application/tests/revision_impact.rs`; extend proposal/approval/assessment task kinds.
**Interfaces:** `assess_impact(old:&PlanDocumentV1,new:&PlanDocumentV1)->Result<ImpactSet,DomainError>` computes explicit changed nodes + data descendants; leader supplies semantic affected nodes. `ReuseDecision::{VerifyAgain,Repair,Discard,ReuseUnchangedEvidence}`; Command adds ProposeRevision/ApproveRevision/AssessReuse.

- [ ] **1. Tests:** `old_attempt_finishes_without_becoming_new_plan_success`, `hold_only_impacted_queued_tasks`, `criteria_change_needs_explanation`, `late_old_result_cannot_overwrite_active_revision`: `assert_eq!(old_attempt.bound_plan,h1); assert_eq!(run.active_plan,Some(h2)); assert_eq!(old_candidate.reuse,NeedsAssessment); assert!(b_independent.is_ready());`. Missing explanation -> validation error.
- [ ] **2. Run red:** `cargo test -p central-application --test revision_impact`.
- [ ] **3. Implement:** immutable proposals/diff, pending proposal holds impacted queued descendants; user approval validates hash/expected revision and activates. Existing running attempts continue old binding unless explicit stop/revoke. Old result stored with original provenance, never relabeled. Changed data input invalidates acceptance, first schedule verification rather than whole implementation. Reuse unchanged evidence chỉ khi all input/candidate/criteria/environment bindings vẫn khớp.
- [ ] **4. Run green:** verify changed order-only dependency không invalidates unrelated data evidence; leader semantic impact expands machine result, cannot shrink known data impacts. Conflicting proposal approval -> conflict; criteria explanation lists requirement/verdict impact and reverify tasks.
- [ ] **5. Commit:** `feat: version plans and assess reuse of stale candidates`.

### Task 5: Git workspaces, sealed candidate và incremental integration

**Files:** Create `crates/central-infrastructure/src/workspace/{mod.rs,git.rs,paths.rs,integration.rs}`, `crates/central-application/src/integration.rs`, `crates/central-application/src/ports/workspace.rs`, `crates/central-infrastructure/tests/{git_workspace.rs,git_integration.rs}`, `tests/fixtures/git-repo/README.md`; extend artifact provenance and CLI sync.
**Interfaces:** WorkspaceManager::snapshot(SnapshotRequest{repo,commit,selected_dirty_paths})->InputSnapshot; create(WorkspaceRequest{attempt_id,input})->WorkspaceRef; seal(WorkspaceRef)->CandidateRef; prepare_integration(IntegrationRequest{base,candidates,approved_plan})->IntegrationCandidate; promote(PromoteRequest{candidate,expected_parent,verification})->PromotionReceipt. `central sync RUN --commit SHA` queues user commit, không sửa worktree user.

- [ ] **1. Tests:** `space_unicode_and_junction_paths`, `candidate_seal_detects_mutation`, `integration_failure_preserves_last_good_ref`, `promotion_uses_compare_and_swap`, `sync_does_not_stash_user_work`: `assert_eq!(user_dirty_after,user_dirty_before); assert_eq!(main_ref_after_failure,previous_good); assert!(junction_escape.is_err()); assert_eq!(promote_with_old_parent,Err(Conflict));` hashes/evidence bind exact candidate.
- [ ] **2. Run red:** `cargo test -p central-infrastructure --test git_workspace --test git_integration`; fixture repo generated under tempfile, no migration thật.
- [ ] **3. Implement:** Git via argv, no concatenated shell, canonical path/Windows prefix checks and junction containment. Hooks disabled for Central's mechanical git operations via per-command owned empty hooks dir, no global config mutation. Dirty input only explicit selected snapshot/commit, no automatic stash. One detached worktree per attempt; source candidate commit immutable; verifier writes scratch, tracked changes invalidate evidence.
- [ ] **4. Implement integration:** serialize approved integration nodes per run, temporary candidate branch, baseline checks + affected checks + final acceptance từ plan. Không tạo task ngoài kế hoạch để tự giải quyết conflict; leader đề xuất khi vượt scope. Only promote by expected-parent CAS after pass; CAS failed -> new candidate/checks. Persist intent before Git mutation; missing receipt recovered by inspecting actual ref/object IDs.
- [ ] **5. Run green:** suites ở bước 2; old worker không bị rebase, sync không hút dirty files, failed checks không cập nhật good ref. Artifact hash giữ nguyên; cleanup worktree/process chỉ tường minh và ownership checked.
- [ ] **6. Commit:** `feat: integrate verified candidates through isolated Git workspaces`.

### Task 6: Recovery/failure injection và first reliable workflow gate

**Files:** Create `crates/central-application/src/recovery.rs`, `crates/central-server/tests/workflow_recovery.rs`, `tests/fixtures/faults/{before_commit.json,after_send.json,after_git_promote.json}`, `scripts/test-workflow-recovery.ps1`, `docs/validation/p3-workflow.md`; extend Runtime::recover and daemon commands.
**Interfaces:** RecoverySummary lists passive findings and required user actions per run; `reconcile_attempt(AttemptId)->Result<ReconcileEvidence,AppError>` never starts inference. `Runtime::recover` persists AwaitingResume plus remaining holds, not clears them.

- [ ] **1. Tests:** `send_succeeded_receipt_missing_never_resends`, `pending_approval_epoch_changed_stays_stale`, `candidate_published_without_metadata_is_orphan`, `promote_done_receipt_missing_converges`, `boot_requires_resume_even_when_evidence_complete`. Assertions: `assert_eq!(duplicate_external_actions,0); assert_eq!(inference_calls_during_boot,0); assert_eq!(hold_after_resume,UsageUnknown); assert_eq!(git_commit_count,expected_count);` no forced claim unknown became failed.
- [ ] **2. Run red:** `cargo test -p central-server --test workflow_recovery`; fixtures can kill only own child at named crash barriers; no real model used.
- [ ] **3. Implement:** passive inspect ownership/session/history/artifacts, store reconciliation evidence, resume same session only when profile/input match and safe. Else new attempt with handover after old outcome known. Unknown branch holds its descendants/resources; independent work can run after explicit run resume unless global budget hold. Cancel requests not terminal until evidence; zero blind replay approvals/outbox sends.
- [x] **4. Run green (Windows 2026-11-12: WORKFLOW RECOVERY GATE: PASS, 39 asserts — `docs/validation/2026-11-12-windows-vm.md`):** `pwsh -File scripts/test-workflow-recovery.ps1`: 2 independent implementation tasks -> verifier -> integration; inject each barrier and restart. `cargo test --workspace`; fake models default. One opt-in Luna/low workflow smoke only after deterministic gate, configurable max calls, stop on quota/auth/unknown accounting. Reboot/manual Windows acceptance recorded separately, not inferred from process kill.
- [ ] **5. Commit:** `test: prove workflow recovery and side-effect reconciliation`; record remaining external-effect limits. P3 gate: trace input/plan/attempt/profile/verification for every fixture output.

## Trạng thái nghiệm thu 2026-11-11 (Linux fix rerun)

Evidence: `docs/validation/2026-11-11-linux-fix-rerun.md` (base `a00a2b5`,
committed as `12e0217`).
Các checkbox gốc giữ nguyên lịch sử.

- [x] Suite xác định xanh: `scheduler` 12, `quality_loop` 16, `budget` 4,
  `retry_fallback` 6, `revision_impact` 10, `git_workspace` 4,
  `git_integration` 4 (gồm bài baseline/affected/acceptance từng lỗi,
  đã sửa containment trong `checks/runner.rs` + 4 unit test regression),
  `workflow_recovery` 7.
- [x] Reboot/manual Windows acceptance ĐÃ thực hiện 2026-11-12
  (OS-initiated `shutdown /r`, SSH rớt ~20 phút, login screen qua screendump,
  SSH tự quay lại, binaries + repo + logs nguyên vẹn —
  `docs/validation/2026-11-12-windows-vm.md`); không suy từ process kill.
  Gate PowerShell (`test-workflow-recovery.ps1`) đã chạy PASS trên Windows VM.
- [x] Dispatcher provision scratch thật cho attempt (late, chưa commit):
  `attempt_scratch_root` + 2 test mới (`provision + cleanup +
  defer-khi-root-hỏng`); daemon bật root này để live turn có cwd owned.
  Đây là scratch cô lập, KHÔNG phải Git worktree/sealed candidate.
