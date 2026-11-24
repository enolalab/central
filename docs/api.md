# Central Public API v1

The daemon owns state, scheduling, approval, budget and verification.
The CLI and every independent client use this same versioned HTTP JSON
API plus SSE. There is no UI, no cookie bootstrap, no LAN exposure:
loopback only, bearer auth on every call (header only), one user.

Contract sources of truth:

- `contracts/openapi-v1.yaml` (human mirror) and
  `contracts/openapi-v1.json` (served at `GET /v1/openapi.json`).
- `contracts/events-v1.schema.json` for `EventViewV1`.
- `contracts/examples/api-v1.json` for redacted payloads.

## Auth

Three credential kinds, all sent as `Authorization: Bearer <token>`:

| Kind | Form | May do |
|---|---|---|
| Owner | daemon token in `<state-dir>/daemon.token` | everything, incl. clients, maintenance, shutdown |
| Client `read` | `<client_id>.<secret>` | GET only |
| Client `control` | `<client_id>.<secret>` | workflow control as the user; no clients, maintenance or shutdown |
| Bridge | `<client_id>.<secret>` bound to one role + attempt | result/handover import for its attempt, plus reads; never approvals |

Issue with `POST /v1/clients` (owner only). The secret is returned
**once** and never stored — only its hash persists. Revocation
(`POST /v1/clients/{id}/revoke`) takes effect on new requests
immediately and closes open streams at the next tick. Query-string
tokens are ignored everywhere (still 401).

## Errors

Every error is `{ "error": { "code", "message", "details" } }`.
Match on `code`, never on `message`:

`validation_failed` (400), `auth_required` (401), `forbidden` (403),
`not_found` (404), `stale_revision` / `stale_target` /
`command_conflict` / `conflict` (409), `resync_required` (409),
`capability_unavailable` / `transport_error` (503),
`internal_error` (500). Error bodies never carry secrets.

## Writes: receipts, not results

`POST /v1/commands?run=<id>` commits one `CommandEnvelopeV1`
(`CreateRun` needs no `run` query). The response is a `ReceiptV1`
proving the commit — HTTP 200 does **not** mean the task passed.
Task outcomes arrive later through reads, events and verification.

- Same command id + same payload → the first receipt replays,
  byte-identical (safe retry after a lost response).
- Same command id + changed payload → 409 `command_conflict`, and
  nothing is re-sent to the agent.
- `expected_revision` guards concurrency: a stale client gets 409
  `stale_revision`/`stale_target` and no privilege changes.
- Scheduler-internal and harness-internal commands (`QueueAttempt`,
  `StartDispatch`, `AttemptRunning`, …, `RecordObservation`) are not
  operator rights: 403 for everyone, including the owner. Specialized
  endpoints (`results`, `handover`, `verdicts`, inbox actions) validate
  first and then commit through the same application use cases.
- `POST /v1/runs/{id}/verdicts` submits one independent verifier verdict
  (`contracts/verdict-submit-v1.schema.json`: task, attempt, `verdict`
  per `verification-v1.schema.json`, verifier `context`). The daemon
  re-derives the seal from the persisted `WorkerResultRecorded` events —
  exact subject, exact criteria, sealed input cited — and commits
  `SubmitVerification`, which routes quality (pass → `Succeeded`,
  fail → repair rounds then leader, blocked → parked). Stale subjects
  are 409, malformed fail verdicts 400, unknown seals 404. Owner or
  control only; the response is the receipt plus the task status.
- `POST /v1/runs/{id}/profile` binds (`{"role","profile"}`) or unbinds
  (`"profile": null`) the run-wide agent profile for one role
  (`worker` or `leader`). Worker attempts with no pinned task profile
  dispatch through the run default; pinned tasks keep their own
  profile; leader reviews dispatch only through the leader profile.
  Unknown runs or profiles are 404, bad roles 400, reader/bridge
  tokens 403.
- `POST /v1/runs/{id}/leader/review` (`{"task","objective"}`) queues
  one leader review turn. The scan dispatches it through the run's
  leader profile with the same gateway machinery as worker turns; the
  review never seals a candidate and the task keeps its status, so the
  leader stays a reviewer, never a shadow worker. Unknown runs/tasks
  are 404. Covering rule (live-verified 24/11): an active leader review
  covers its task — no worker attempt queues until it settles — while a
  running worker never blocks a queued leader review. Cancel is async
  through the scan outbox: `stop` parks the attempt at once, the serve
  child is reaped on the next tick, not instantly.

## Reads: snapshots, pages, streams

- `GET /v1/runs/{id}` returns `RunSnapshotV1`: `instance_epoch`,
  `revision`, `last_seq` and the curated `state` projection (internal
  scheduling maps never leave the server; inbox previews are redacted).
- Lists and history paginate as `PageV1 { items, next_cursor }`:
  default limit 100, max 500, opaque cursors bound to their
  collection/run. Bad cursors and limits are 400.
- `GET /v1/runs/{id}/events?after=&limit=` pages the durable log;
  `GET .../events/stream` is SSE with the durable seq as the event id,
  `?after=` or `Last-Event-ID` resume, a 15 s keepalive and bounded
  backpressure. A cursor beyond history, or an epoch that changed
  across a restart, yields `resync_required` (409 on JSON, a terminal
  frame on SSE): refetch the snapshot, then stream from its cursor.
  Restarting never auto-resumes: runs park in `AwaitingResume` until
  an explicit `resume`, and dispatch stays closed before that.

## Capabilities and versioning

`GET /v1/meta` reports `version`, `api: v1`, `instance_epoch` and the
real capabilities (fake-agent flag, stored profiles). After v1 ships,
new fields/endpoints stay compatible; breaking changes take a new
major with migration notes. The version never implies harness support
for an unverified feature.

## Browser clients

Requests without `Origin` (CLI, scripts) are unaffected. A present
`Origin` must be in the owner's `origins.json` allowlist or the call
is 403 — reads and streams included. Preflight answers 204 with the
echoed origin (never `*`). Use `fetch` with an `Authorization`
header and a manual stream reader; never `EventSource` with a token
in the URL, and keep tokens in memory only. See
`examples/browser-client.mjs` and `docs/client-integration.md`.
