# Central P4 — Immersive Office and Release Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Browser UI văn phòng pixel dùng runtime thật, có chat/panel theo ngữ cảnh; phát hành MVP local có kiểm chứng.

**Architecture:** React is a client of Central application API. Canvas chỉ trình bày trạng thái; native buttons/DOM panels cung cấp thao tác và accessibility. Không duplicate scheduler trong frontend.

**Tech Stack:** React 19.3.1, TypeScript 7.0.2, Vite 8.3.6, Vitest 5.1.0, Playwright 1.65.0; Axum API/SSE và static assets. Node 24.22.1/pnpm 10.

**Spec:** `docs/superpowers/specs/2026-11-09-central-mvp-design.md`; `docs/superpowers/mockups/central-immersive-office-v1.html`; prerequisite P3 và contracts plan tổng.

## Global Constraints

- Văn phòng là màn hình chính, không sidebar/dashboard/chat cột cố định.
- Bấm leader mở chat giữ lịch sử/bản nháp; bấm nhân vật/đồ vật mở panel ngữ cảnh.
- Native labels/keyboard và reduced motion; màu/animation không là bằng chứng.
- Chat/character click không tự approve; stale revision phải conflict.
- Owner-only local access; không lộ credentials trong URL/log/JS state.
- Không đem scenario picker, dữ liệu giả và scripted agent reply của mockup vào release.

## Review Focus

1. SSE replay trùng hoặc mất khoảng seq -> dedup/resnapshot, không giả live — T1,T2.
2. Model text/diff chứa HTML/script -> hiển thị như dữ liệu, không XSS — T4.
3. UI tab stale approve hash cũ -> conflict/refetch, không optimistic grant — T1,T4.
4. Daemon offline/usage hold -> badge rõ, không fake typing — T3,T5.
5. Cross-origin page gọi localhost/asset path traversal -> từ chối — T1,T6.

### Task 1: Read models, browser auth và durable event stream

**Files:** Create `crates/central-server/src/{read_models.rs,http/{events.rs,browser_session.rs,assets.rs,meta.rs}}`, `contracts/api-v1.schema.json`, `crates/central-server/tests/{browser_security.rs,event_stream.rs}`; extend Store with events/read query methods, CLI ui command.
**Interfaces:** `GET /v1/meta` returns protocolVersion=1; RunView/OfficeAgentView/InboxView/ChatMessageView typed camelCase DTOs; `GET /v1/runs/{id}/events?after=SEQ` SSE with id=durable seq; Store::events(RunId,after:u64,limit:u32)->Vec<EventRecord>; `POST /v1/session/bootstrap` consumes one-time bootstrap token, returns HttpOnly SameSite=Strict cookie.

- [ ] **1. Tests:** `sse_replay_dedup_and_gap`, `stale_approval_returns_409`, `wrong_origin_or_host_rejected`, `agent_token_cannot_bootstrap_owner`, `artifact_path_not_arbitrary_read`: `assert_eq!(replayed_seqs,[11,12]); assert_eq!(stale.status,409); assert_eq!(cross_origin.status,403); assert_eq!(no_auth.status,401); assert!(escape_read.is_err());` completed run event replay cũng đúng.
- [ ] **2. Run red:** `cargo test -p central-server --test browser_security --test event_stream`.
- [ ] **3. Implement:** same-origin API/static assets; no wildcard CORS, Host allowlist đúng loopback/port, Origin validation cho mutations. CLI `central ui` creates single-use 60s bootstrap URL with token in fragment, browser POST exchanges it then removes fragment; never logs secrets. Persist only hashed owner session token with 24h TTL so cookie can survive daemon restart; credentials remain owner-protected. CLI bearer token và scoped agent bridge token không interchangeable.
- [ ] **4. Run green:** event queue has backpressure; gap requires refetch snapshot not fake interpolation, keepalive 15s không là activity evidence. Read models omit credentials/secret answers/raw auth paths. Stale target requires targetHash+expectedRevision from DTO. Assets fixed manifest paths; absent UI ->503. Add CSP frame-ancestors none and content restrictions phù hợp same-origin bundle, không cho model text thêm script/image HTML.
- [ ] **5. Commit:** `feat: expose authenticated browser projections and replayable events`.

### Task 2: Browser client state và persisted conversation

**Files:** Create `web/{package.json,pnpm-lock.yaml,tsconfig.json,vite.config.ts,index.html,src/{main.tsx,app/App.tsx,api/{client.ts,types.ts,events.ts},state/{runStore.ts,drafts.ts},test/{fixtures.ts,runStore.test.ts}}}`; Vite dev proxy same-origin backend, test fixtures chỉ test bundle.
**Interfaces:** `CentralClient.command(envelope:CommandEnvelope):Promise<Receipt>`; `subscribeRun(runId:string,after:number,onEvent:(e:EventView)=>void):()=>void`; `useRun(runId:string):RunViewState`; `drafts.get/set(runId,text)` sessionStorage cho normal draft, no secret answer storage.

- [ ] **1. Tests:** `duplicate_event_not_double_applied`, `gap_refetches_snapshot`, `closing_chat_keeps_draft`, `cookie_failure_shows_reconnect`: `expect(store.lastSeq).toBe(12); expect(store.revision).toBe(snapshot.revision); expect(drafts.get(runId)).toBe('giữ API contract'); expect(state.connection).toBe('authRequired');` no client-side invented approval.
- [ ] **2. Run red:** `pnpm --dir web test -- runStore`; expected tests fail before store/client implementation.
- [ ] **3. Implement:** strict API DTOs aligned contract fixture, resumable SSE and snapshot/sequence handling; no domain transitions in JS. Authoritative receipt before UI marks permission granted. Chat messages persisted on server with segment/session mapping; panel state local, normal draft per run; secret answer memory-only. Scope run switching explicitly, no draft crossing rooms.
- [ ] **4. Run green:** `pnpm --dir web test -- runStore`; `pnpm --dir web exec tsc --noEmit`; fixture 401/409/503 leaves draft intact. Node/pnpm lock pinned, no model calls in frontend tests.
- [ ] **5. Commit:** `feat: add typed browser client and persistent conversation state`.

### Task 3: Immersive office scene and accessible hit targets

**Files:** Create `web/src/office/{OfficeScene.tsx,drawOffice.ts,sprites.ts,layout.ts,statusPresentation.ts,office.css,OfficeScene.test.tsx}`, `web/src/components/{OfficeHud.tsx,OfficeDock.tsx,ContextLayer.tsx}`; port authored art from mockup, không reuse fake runtime script.
**Interfaces:** `OfficeScene({agents:OfficeAgentView[],selectedId:string|null,onSelect:(id:string)=>void,reducedMotion:boolean})`; `presentStatus(agent:OfficeAgentView,connection:ConnectionState):StatusPresentation`; click board invokes onOpenPlan(), not domain commands.

- [ ] **1. Tests:** `not_spawned_has_empty_seat`, `unknown_has_label_and_no_work_animation`, `keyboard_can_select_every_agent`, `world_coordinates_match_resize`: `expect(screen.queryByLabelText('Worker API đang làm')).toBeNull()` before spawn; `expect(animation.active).toBe(false)` for unknown/offline; every selectable agent has a native labeled button and same mapped task ID after resize.
- [ ] **2. Run red:** `pnpm --dir web test -- OfficeScene`; expected failures before scene/components exist.
- [ ] **3. Implement:** full-screen office, small HUD/dock, object/person hit targets, on-demand context panes, room reflects selected run. Canvas pixels authored locally; native overlays scale with same transform. Animation based on observed active state only; honor prefers-reduced-motion and explicit toggle. No XP, coins, game editor or permanent dashboard panels.
- [ ] **4. Run green:** visual checks at 1440×900, 1024×768, 390×844; desktop office no fixed chat column, mobile reachable controls without overlap. Canvas coordinates use container ResizeObserver; device pixel ratio does not change hit mapping. Record screenshots as review artifacts, no arbitrary image-generation task needed.
- [ ] **5. Commit:** `feat: render immersive pixel office from runtime state`.

### Task 4: Contextual chat, plans, task inspector và real inbox

**Files:** Create `web/src/panels/{LeaderChat.tsx,PlanBoard.tsx,TaskBook.tsx,TaskInspector.tsx,ApprovalPanel.tsx,QuestionPanel.tsx,ArtifactPanel.tsx,ProfilePanel.tsx}`, `web/src/panels/panels.test.tsx`, `web/e2e/contextual-workflow.spec.ts`; extend server chat/profile/proposal DTO endpoints.
**Interfaces:** `ContextTarget = {kind:'chat'}|{kind:'plan',hash:string}|{kind:'task',taskId:string}|{kind:'inbox',id:string}|{kind:'artifacts',taskId:string}`; client mutations use CommandEnvelope with UUID command ID, target hash/preview and expected revision. ProfilePanel submits explicit user config; does not mutate active attempt snapshot.

- [ ] **1. Tests:** `ok_message_is_not_plan_approval`, `stale_tab_keeps_approval_ungranted`, `untrusted_text_not_html`, `artifact_provenance_links_exact_attempt`: `expect(approvalCalls).toHaveLength(0)` after sending ok; stale409 renders changed-revision notice; `<img onerror=...>` appears as text; artifact link targets correct plan/attempt/verification not latest attempt by name.
- [ ] **2. Run red:** `pnpm --dir web test -- panels`; Playwright fixture service returns real DTO shapes, not product mocks shipped to user.
- [ ] **3. Implement:** leader history/draft retained across pane close; physical board shows proposal diff/scope/verify and exact approval target. Worker inspector exposes task/input/profile/workspace/candidate/verify evidence and log. Inbox distinguishes plan vs action vs question. Leader answers within approved scope require requirement refs; changes route to revision approval. Profile UI shows supported/unsupported/unknown model/effort/skill/subagent and explicit instruction-only acceptance/fallback config.
- [ ] **4. Run green:** `pnpm --dir web exec playwright test e2e/contextual-workflow.spec.ts`: click leader, send message, close/reopen; plan approval; worker selection; tool approval; question; candidate evidence; new profile applies only new attempt. Render text/diff escaped, no raw HTML injection; panels keyboard close and focus returns to source.
- [ ] **5. Commit:** `feat: connect contextual office interactions to real workflow commands`.

### Task 5: Recovery/budget/unknown UI and run controls

**Files:** Create `web/src/components/{RunPicker.tsx,RunControls.tsx,AttentionIndicator.tsx,RecoveryNotice.tsx,BudgetNotice.tsx}`, `web/src/panels/UsageResolutionPanel.tsx`, `web/e2e/recovery-and-holds.spec.ts`; extend server recovery/accounting DTOs, not new scheduler logic.
**Interfaces:** RecoveryView shows findings/unknownAttemptIds/resumeRequired; BudgetView shows observed/estimated/unknown, holds and admission reason. UsageResolutionPanel invokes User-only ResolveUsageGap; Runtime remains authoritative.

- [ ] **1. Tests:** `boot_does_not_resume_on_page_load`, `usage_hold_survives_resume_click`, `independent_run_remains_available`, `pause_does_not_fake_cancel`: `expect(newAttemptCount).toBe(0)` after reload; `expect(hold).toBe('UsageUnknown')` after resume; run B still selectable; running attempt still displayed while pause gate closed.
- [ ] **2. Run red:** `pnpm --dir web exec playwright test e2e/recovery-and-holds.spec.ts` with deterministic fake backend, no model calls.
- [ ] **3. Implement:** top attention and character state show waiting/unknown/offline unambiguously; no fake animation on disconnected stream. Resume preview describes passive findings and remaining holds; never implies global rollback. Missing usage requires evidence or explicit user handling with audit. Stop differs from pause, show cancellation pending until confirmed; fallbacks are notified with old/new profile and cause.
- [ ] **4. Run green:** restart actual test daemon while browser remains open; cookie/SSE reconnect, pending request may become stale, UI never sends cached accept to new epoch. Changing run preserves chat draft separately. Live budget figures use server semantics, not animation or sum of cumulative events.
- [ ] **5. Commit:** `feat: surface recovery holds and safe run controls in office UI`.

### Task 6: Packaging, data lifecycle and end-to-end acceptance

**Files:** Create `scripts/{build-release.ps1,test-mvp-e2e.ps1}`, `README.md`, `docs/{getting-started.md,recovery.md,validation/mvp-acceptance.md}`, `crates/central-server/tests/{artifact_retention.rs,release_security.rs}`; modify CLI daemon/ui startup and release asset discovery.
**Interfaces:** Windows zip contains `central.exe`, `central-server.exe`, `ui/`, notices; no Node required at runtime. State dir explicit via --state-dir, default `%LOCALAPPDATA%/Central`; owned working dirs/artifacts beneath it. CLI `artifacts`, `logs`, `export`, `gc --dry-run`/explicit gc; no autonomous destructive cleanup.

- [ ] **1. Tests:** `referenced_artifact_never_gc`, `active_or_unknown_workspace_never_gc`, `release_has_no_demo_runtime`, `asset_escape_rejected`: `assert!(dry_run.kept.contains(referenced)); assert!(kept.contains(unknown_workspace)); assert_eq!(shipped_scenario_controls,0); assert_eq!(escape.status,403);` export báo cáo không được tự gắn nhãn là database backup.
- [ ] **2. Run red:** `cargo test -p central-server --test artifact_retention --test release_security`; release fixture initially missing files is expected.
- [ ] **3. Implement:** package static built assets beside binaries; no implicit Rust install for end user. Start daemon hidden, validate CLI/server protocol versions; `central ui` opens authenticated bootstrap. Logs redact secrets; artifact export includes manifest/hashes; GC only explicit, owned paths, never active/unknown or referenced blobs. Document offline backup: daemon stopped, owner lock free, copy DB plus existing WAL plus artifact store together; no live DB-only copy. Preserve library notices; no user Git auth/config mutation.
- [ ] **4. Run green:** `cargo fmt --check`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo test --workspace`; `pnpm --dir web test`; `pnpm --dir web exec playwright test`; `pwsh -File scripts/build-release.ps1`; `pwsh -File scripts/test-mvp-e2e.ps1`. Gate: plan->2 workers->independent verify->integration->crash->boot waits->resume->provenance. Smoke zip under Windows user/VM without Rust/Node; audit native DLL requirements and document official VC++ runtime if required. Manual reboot recorded separately. One bounded opt-in Luna/low run after fake suite, stop on quota/auth; no unobserved promise of pending approval surviving harness restart.
- [ ] **5. Commit:** `feat: package and validate local Central MVP`; final `docs/validation/mvp-acceptance.md` lists measured guarantees, unsupported capabilities and unknown accounting limitations. Implementation stops for review if any mandatory gate fails, not merely because UI looks complete.
