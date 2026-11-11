# Client integration guide

How to write an independent client against the public v1 API using
only the published contract. The worked PowerShell client is
`examples/client.ps1`; the browser sketch is
`examples/browser-client.mjs`. Neither imports a Central crate nor
reads the database.

## 1. Discover

```text
GET /v1/meta            -> { version, api: "v1", instance_epoch, capabilities }
GET /v1/openapi.json    -> the contract this client was written against
```

Both need the bearer token. Abort when `api` is not `v1`: never
misread a newer daemon.

## 2. Authenticate

- Native clients: read the owner token from the state directory
  (`daemon.token`, owner-only file permissions), or use a scoped
  client token the owner issued (`POST /v1/clients`).
- Send `Authorization: Bearer <token>` on every request, streams
  included. Never put the token in a URL, and never log it.
- Browser clients: keep the token in memory only (a module-scoped
  variable, never `localStorage`), and confirm the page's origin is
  in the owner's `origins.json` before connecting.

## 3. Work a run end to end

```text
POST /v1/commands                      {CreateRun: {...}} -> receipt.run_id
POST /v1/commands?run=<id>             {ProposePlan: {...}} -> receipt
GET  /v1/runs/<id>/plans               -> pending_plan hash (quote this exact hash)
POST /v1/commands?run=<id>             {ApprovePlan: {plan_hash}} + expected_revision
GET  /v1/runs/<id>                     -> snapshot {instance_epoch, revision, last_seq, state}
GET  /v1/runs/<id>/inbox               -> entries with key + preview_hash
POST /v1/runs/<id>/inbox/approve       -> receipt (exact key + hash or 409)
GET  /v1/runs/<id>/events?after=<last_seq>  -> ordered pages, next_cursor to resume
```

Generate one UUID per command id and reuse it for retries: a lost
response is safe to replay (same receipt), and a changed payload
under the same id is a 409, never a duplicate side effect.

## 4. Stay connected

- Snapshot first, then stream from `last_seq`:
  `GET /v1/runs/<id>/events/stream?after=<last_seq>`.
- Reconnect with `Last-Event-ID: <last-seq>` (or `?after=`).
- On `resync_required` (409, or the SSE frame): refetch
  `GET /v1/runs/<id>` and stream from the new `last_seq`. Never
  resume blind from an old cursor after a restart.
- `next_cursor` values are opaque: store and replay them, never
  parse or mint them.

## 5. Handle the hard cases

- **Stale approval (two clients):** the second approver holds an old
  `expected_revision` and gets 409. Re-read the snapshot and only
  approve the hash the user actually reviewed.
- **Held runs:** `holds` (budget exceeded, unknown usage, plan
  impact) close the dispatch gate. `GET .../usage` explains the
  admission reason; missing finals resolve only through the explicit
  audited `POST .../usage/resolve`.
- **Stopped or parked runs:** after a restart every nonterminal run
  is `AwaitingResume`. Only an explicit `resume` reopens dispatch;
  the client never auto-resumes and never answers inbox on reconnect.
- **Large reads:** artifacts and logs page with bounded windows
  (content caps at 1 MiB per response). Artifact reads are id-scoped
  with hash verification; `..` and path escapes are refused.

## 6. What clients must never do

Read the database or queue files directly; replay domain transitions
from events to compute state (events are notifications — the snapshot
is the state); approve a plan hash the user did not review; treat an
HTTP 200 receipt as a task pass; or send a secret anywhere except the
one-time issuance answer and the `Authorization` header.
