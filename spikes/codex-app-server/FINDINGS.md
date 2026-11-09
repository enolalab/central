# App-server findings — 2026-11-09 (Asia/Bangkok)

Evidence: `results/2026-11-09T02-51-04-875Z/report.json` and `rpc.jsonl`.
Codex CLI 0.167.0-alpha.3.2; gpt-6-luna; low effort; existing ChatGPT auth.
The experiment took approximately 17 seconds. This is a Codex investigation,
not a Central recovery test.

## Results

| Check | Command approval | Structured question |
| --- | --- | --- |
| Real model issued server request | `item/commandExecution/requestApproval` | `item/tool/requestUserInput` |
| Waiting status before disconnect | active / waitingOnApproval | active / waitingOnUserInput |
| Waiting status after reconnect and resume | active / waitingOnApproval | active / waitingOnUserInput |
| Same thread after resume | Yes | Yes |
| Pending request replayed to new WebSocket client | Yes, same request ID in this server lifetime | Yes, same request ID in this server lifetime |
| Test client's answer | accept (once) | Alpha |
| serverRequest/resolved observed | Yes | Yes |
| Final turn status | completed | completed |
| External/final evidence | No marker before accept; exact nonce marker afterward | Agent's final message was Alpha |

## Telemetry

Take the final cumulative `tokenUsage.total` for each distinct thread, not the sum
of intermediate cumulative updates. These are reported protocol values, not a
pricing/billing calculation:

| Thread | Input tokens | Cached input tokens (included in input) | Output tokens |
| --- | ---: | ---: | ---: |
| Approval | 30,878 | 28,160 | 157 |
| Question | 31,765 | 25,856 | 72 |

## Meaning for Central's design (proposals)

1. App-server is a promising integration surface for an interactive leader and
   workers: structured server requests distinguish waiting from ongoing work.
2. An adapter should separate tool permission approval from a question/answer.
   Neither is itself approval of Central's versioned plan.
3. Track thread ID, turn ID, item ID, RPC request ID and the server instance.
   IDs replayed unchanged here; this does not establish their identity or
   validity across server restarts. Do not blindly replay a saved accept.
4. Re-initialize the connection, resume the intended thread, and reconcile its
   waiting state before displaying or responding to a pending request.
5. `serverRequest/resolved` and final item/turn state should clear stale UI.
   A waiting flag alone does not contain the full question or command preview.
6. The probe implements scripted test answers only. A real Central UI must route
   questions/approvals to the authorized user or leader under the agreed policy.
7. Keep plan/workflow ownership in Central; leave the model/tool execution loop
   to Codex. Connecting to this protocol does not require Central to be a harness.

## Not established

- Survival of pending requests if the app-server itself crashes or restarts.
- Recovery after the controller process exits; this probe closes and recreates
  WebSocket connections inside one controller process.
- Requests missed before the client first recorded them; longer disconnects,
  multiple active controllers, stale/duplicate answers or timeout cleanup.
- Deny/cancel paths, file-change and permissions approvals, or free-form/secret
  questions. The question was forced in Plan mode; arbitrary Default-mode
  questioning has not been established.
- Rust adapter implementation, Central persistence, scheduling or web UI.
- Stable support contract: WebSocket transport and structured questions are
  experimental in the documentation/schema used for this test.

## Sources

Official documentation: https://learn.chatgpt.com/docs/app-server
Actual schemas: generated locally with
`codex app-server generate-json-schema --experimental --out schema`.
Observations above come from real traffic, not from fabricated protocol events.
