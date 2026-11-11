# Central P2 — Codex Workflow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Leader proposal -> user approval -> một worker -> verifier độc lập với Codex thật và evidence có nguồn gốc.

**Architecture:** Codex app-server là harness; adapter Rust JSONL/stdio normalize request/event. Application giữ workflow/approval; app-server sở hữu theo từng phiên agent để cô lập workspace/config ban đầu.

**Tech Stack:** Rust/Tokio/serde, Windows Job Objects, versioned RPC schemas, fixture transcripts và opt-in Luna/low smoke.

**Spec:** `docs/superpowers/specs/2026-11-09-central-mvp-design.md`; contracts trong `2026-11-09-central-mvp.md`; prerequisite P1.

## Global Constraints

- Không model/tool loop trong Central, không scrape hội thoại để mutate state.
- Không provider fallback thay model đã chọn.
- Real smoke gpt-6-luna/low; dừng khi quota/auth lỗi.
- RPC ID gắn epoch; không replay accept cũ qua server restart.
- Startup/budget/concurrency gate áp dụng cho mọi vai trò.

## Review Focus

1. JSONL chia chunk/response ngược thứ tự -> correlation đúng — T1.
2. Child chết, PID tái dùng -> không kill process khác — T2.
3. Request resolved/đổi epoch -> stale, không gửi accept — T3.
4. Candidate ngoài root/hash sai -> từ chối artifact — T4,T5.
5. Skill/effort không đọc lại được -> unknown thay vì giả supported — T2.

### Task 1: Versioned stdio RPC transport

**Files:** Create `crates/central-infrastructure/src/codex/{mod.rs,version.rs,wire.rs,stdio.rs,gateway.rs}`, `contracts/codex/0.167.0-alpha.3.2/{manifest.json,protocol-subset.json}`, `tests/fixtures/codex/{initialize.jsonl,chunked.jsonl,out-of-order.jsonl}`, `crates/central-infrastructure/tests/codex_transport.rs`.
**Interfaces:** Implements AgentGateway; internal `RpcClient::request(&str,Value)->Result<Value,RpcError>`, `respond(RpcId,Value)->Result<(),RpcError>`; `CodexGateway::new(Arc<dyn ProcessSupervisor>,CodexConfig)->Self`.

- [ ] **1. Write failing tests:** `chunked_frames_and_out_of_order_responses`, `unknown_version_rejected`, `bounded_buffer_requires_reconciliation`: `assert_eq!(response_for(101),expected101); assert!(unsupported.is_err()); assert!(observations.contains(&Observation::ReconcileRequired));`. Fixtures lấy spike đã lọc, không auth data.
- [ ] **2. Run red:** `cargo test -p central-infrastructure --test codex_transport`. Generate schema từ đúng CLI, lưu subset/SHA; không coi schema tự đoán là tested.
- [ ] **3. Implement:** separate responses/server requests/notifications; initialize/initialized, typed v2 subset. Reader không bị block vì approval; CLI version ngoài allowlist -> actionable error. Timeout sau write -> OutcomeUnknown, không resend turn/start. Giới hạn frame 8 MiB, oversize đóng transport và reconcile, không parse một phần thành event.
- [ ] **4. Run green:** fixture suite; live stdio initialize smoke chờ supervisor T2, không tạo concrete process adapter tạm thời ở đây. Mỗi process có epoch mới. Log không dump token/environment, request error không lộ secret. Set allowProviderModelFallback=false và đối chiếu requested/observed model; lệch -> capability error thay vì âm thầm tiếp tục.
- [ ] **5. Commit:** `feat: add versioned Codex app-server stdio adapter`.

### Task 2: Profile isolation và process ownership

**Files:** Modify `crates/central-domain/src/profile.rs`; create `crates/central-application/src/profiles.rs`, `crates/central-infrastructure/src/process/{supervisor.rs,windows_job.rs}`, `crates/central-infrastructure/tests/{profile_isolation.rs,process_ownership.rs}`; extend gateway/contracts.
**Interfaces:** `resolve_profile(ProfileLayers,&Capabilities)->Result<ResolvedProfile,ProfileError>`; ProcessSupervisor spawn(SpawnSpec)->ProcessIdentity, inspect(&ProcessIdentity)->ProcessObservation, terminate(&ProcessIdentity)->CancelObservation. Capabilities Supported/Unsupported/Unknown; enforce Enforced/InstructionOnly/Unsupported.

- [ ] **1. Tests:** `task_override_freezes_at_attempt`, `model_unavailable_no_silent_fallback`, `never_kill_reused_pid`, `unobservable_skill_hash_visible`: `assert_eq!(attempt.profile.hash,original); assert!(unsupported_model.is_err()); assert_eq!(wrong_creation_time.kill_count,0); assert_eq!(skill.observed_hash,None);` profile A/B concurrent không lẫn model/config.
- [ ] **2. Run red:** `cargo test -p central-infrastructure --test profile_isolation --test process_ownership`.
- [ ] **3. Implement:** explicit per-session config/snapshot. Owned agent processes vào Job Object KILL_ON_JOB_CLOSE; daemon không nằm trong job sống theo CLI. Test nested job/sandbox coverage; không enforce được -> công bố unknown/instruction-only, user chấp nhận nếu cần. Không thay auth/config cá nhân. Profile resolution đúng cá nhân->workflow->task->attempt.
- [ ] **4. Run green:** helper process/cancellation tests và installed-CLI stdio initialize/thread start-read-resume smoke không inference. Chỉ dừng owned app-server sau khi terminal receipt/output đã lưu; giữ session history để resume sau này, không giữ process idle cho mọi task đã xong. Skill/effort/subagent chỉ advertise supported khi có config smoke; requirement không đáp ứng -> hold. Không biến subagent harness thành worker Central.
- [ ] **5. Commit:** `feat: isolate agent profiles and supervise owned processes`.

### Task 3: Durable approval/question inbox

**Files:** Create `crates/central-domain/src/inbox.rs`, `crates/central-application/src/inbox.rs`, `crates/central-infrastructure/src/codex/requests.rs`, `crates/central-server/src/http/inbox.rs`, `crates/central-application/tests/inbox_recovery.rs`; extend CLI approve/decline/answer.
**Interfaces:** Command adds RecordObservation/AnswerQuestion/ApproveAction/DeclineAction; `ExternalAnswer { key:ExternalRequestKey,payload:AnswerPayload,scope:Once }`; InboxEntry Pending/Answered/Resolved/Stale, immutable preview hash/actor.

- [ ] **1. Tests:** `reply_scoped_to_epoch_and_preview`, `replayed_request_is_one_entry`, `resolved_cannot_accept`, `answer_waits_for_resume`: `assert_eq!(inbox.len(),1); assert_eq!(old_epoch,Err(StaleRequest)); assert_eq!(before_resume.reply_count,0); assert_eq!(after_resume.reply_count,1);`. Plan approval không map thành tool approval.
- [ ] **2. Run red:** `cargo test -p central-application --test inbox_recovery`. Fixtures approval/question/decline/cancel/resolved, restart tái dùng request ID 0.
- [ ] **3. Implement:** persist pending trước UI notice, response intent/receipt và passive reconcile. Đổi epoch -> stale cho tới xác minh request mới; không accept text giống nhau. Secret answer memory-only qua VolatileAnswerRef ngẫu nhiên, durable intent không chứa payload secret hoặc content-derived hash; restart cần nhập lại. Test sentinel secret không xuất hiện trong snapshot/outbox/audit/trace. Leader xử lý worker question qua task có budget, không Central tự suy luận trả lời.
- [ ] **4. Run green:** reconnect và harness restart là hai case riêng; stopped/AwaitingResume không mở inference khi answer. Opt-in stdio smoke: harmless command approval + Plan-mode question, script trả lời demo; kiểm tra final turn và marker. Không gọi đó là approval thật của user.
- [ ] **5. Commit:** `feat: persist scoped approvals and questions with stale-request protection`.

### Task 4: Structured leader/result bridge và handover

**Files:** Create `contracts/{leader-reply-v1.schema.json,worker-result-v1.schema.json,verification-v1.schema.json}`, `crates/central-application/src/{leader.rs,handover.rs}`, `crates/central-infrastructure/src/codex/{bridge.rs,prompts.rs}`, `crates/central-application/tests/leader_bridge.rs`.
**Interfaces:** LeaderReplyV1 message/proposal:Option<PlanDocumentV1>; WorkerResultV1 attempt_id/input_hash/candidate_path/candidate_hash/evidence_paths/summary; VerificationV1 subject_hash/criteria_hash/outcome/evidence_ids/findings; HandoverV1 predecessor/input/artifacts/verified_facts/unverified_notes/next_step.

- [ ] **1. Tests:** `ok_in_chat_does_not_approve`, `proposal_needs_user_hash`, `handover_separates_claims`, `agent_token_cannot_approve`: `assert_eq!(run.active_plan,None); assert_eq!(leader_approve.status,403); assert!(handover.unverified_notes.contains(&claim)); assert!(foreign_candidate.is_err());` malformed/unknown schema rejected.
- [ ] **2. Run red:** `cargo test -p central-application --test leader_bridge`; no extraction of markdown JSON to mutate state.
- [ ] **3. Implement:** `LeaderService::message(RunId,String)->Result<ExecutionSegment,AppError>` qua common gate. App-server dynamic tools central_propose_plan/central_report_result khi supported; alternatively outputSchema final DTO. Agent bridge token chỉ proposal/report đúng run/attempt; không approve. File import CLI đi cùng schema/hash. Smoke channel phải đạt trước bật worker execution; không scrape chat khi thiếu capability.
- [ ] **4. Run green:** fixture brainstorm->proposal->approve hash; scope-changing answer phải user duyệt revision. New-session handover inspect evidence trước write. Opt-in smoke kiểm tra actual structured channel; unsupported -> actionable capability + validated import route, không bỏ yêu cầu.
- [ ] **5. Commit:** `feat: add structured leader proposals worker reports and handovers`.

### Task 5: Immutable artifacts và one-task independent verification

**Files:** Create `crates/central-infrastructure/src/{artifacts/{mod.rs,filesystem.rs},checks/{mod.rs,runner.rs}}`, `crates/central-application/src/{artifacts.rs,verification.rs,ports/artifacts.rs,ports/checks.rs}`, `crates/central-infrastructure/tests/artifact_contract.rs`, `tests/fixtures/tiny-repo/README.md`, `scripts/test-codex-single-task.ps1`, `docs/validation/p2-codex.md`.
**Interfaces:** ArtifactStore::publish(PublishArtifact{attempt_id,source,expected_hash})->ArtifactRef, read(ArtifactId)->ArtifactBytes; CheckRunner::run(CheckRequest{approved_plan_hash,program,args,cwd,timeout,allowed_write_roots})->CheckEvidence; `VerificationService::accept(VerificationV1)->Result<Receipt,AppError>`.

- [ ] **1. Tests:** `artifact_root_and_hash_checked`, `orphan_is_not_committed_artifact`, `exit_zero_is_not_task_success`, `verifier_uses_sealed_candidate`: `assert_ne!(worker_done.task_status,Succeeded); assert_eq!(accepted.candidate_hash,sealed); assert!(path_escape.is_err()); assert_eq!(after_pass,Succeeded);` mismatched input -> stale/reverify.
- [ ] **2. Run red:** `cargo test -p central-infrastructure --test artifact_contract`; `cargo test -p central-application verification`.
- [ ] **3. Implement:** temp-write/hash/flush/rename blob rồi commit metadata; orphan dọn tường minh, không xóa referenced blob. CheckRunner chỉ program+argv đã duyệt, capture stdout/stderr/exit/hash, cwd/root/timeout qua supervisor; không eval model prose. Verifier phiên/scratch riêng, không sửa candidate/criteria; không recursive verifier.
- [ ] **4. Run green:** `pwsh -File scripts/test-codex-single-task.ps1 -RealModel`: fixture one function/test, worker sửa, verifier độc lập, evidence; restart Central -> zero inference trước resume. Initial smoke tối đa 3 agent turns; stop khi quota/auth; tests thường fake. Ghi usage/unknown, không gửi repo thật.
- [ ] **5. Commit:** `feat: verify one Codex task with immutable provenance`; P2 evidence không chỉ lời agent nói xong.

## Trạng thái nghiệm thu 2026-11-11 (Linux fix rerun)

Evidence: `docs/validation/2026-11-11-linux-fix-rerun.md` (base `a00a2b5`,
committed as `12e0217`).
Các checkbox gốc giữ nguyên lịch sử.

- [x] Suite xác định xanh: `verification` 5, `artifact_contract` 2 bài chạy
  được trên Linux (3 bài còn lại `cfg(windows)`, không biên dịch ở đây),
  `codex_transport` 9, `profile_isolation` 6, `process_ownership` 9 đạt +
  1 ignored (smoke opt-in, chạy riêng đạt `initialize ok`).
- [x] Live smoke đúng thủ tục manual của P2 (scratch fixture-owned, tối đa
  3 turn, dừng khi quota/auth lỗi): 2 turn qua `codex exec` trực tiếp —
  worker sửa `calc.py` (hash đổi `ba1a531f…` → `2ce8d715…`,
  `add(1,2)==3` giữ, bool bị `TypeError`; usage in 62984/cached 53120/
  out 187) + verifier độc lập PASS (usage in 41378/cached 31872/out 133).
  Model yêu cầu `gpt-6-luna`/low; output `--json` không mang tên model quan
  sát nên ghi nhận theo yêu cầu, không suy diễn fallback.
- Smoke trên chạy ngoài daemon (đúng phạm vi manual của P2); workflow live
  qua daemon thuộc P4 và đang bị chặn — xem P4.
- [x] Transport đã nối vào gateway (late 2026-11-11, cùng base, chưa
  commit): `spawn_piped` + RPC attach (initialize/thread/turn/answer/
  observe/reconcile), test duplex e2e + test piped spawn thật đều xanh;
  workspace suite 243 passed. Chi tiết trong evidence, mục transport wiring.
