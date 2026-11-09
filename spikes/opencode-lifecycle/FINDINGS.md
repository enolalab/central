# Findings — OpenCode 1.19.5, Windows x64

Observed on 2026-11-09 (Asia/Bangkok). Raw evidence is in
`results/report.json`; API schema is in `results/openapi.json`.
This is a throwaway investigation, not an approved Central implementation.
No real model was invoked and no user provider credentials were required.

## Observed behavior

| Experiment | Observation |
| --- | --- |
| Start isolated headless server | Health reports version 1.19.5. |
| Create session and save no-reply message | Session created and message persisted. |
| Observe SSE | Session/message events received; sampled packets contain no SSE `id` field. |
| Disconnect and reconnect SSE | Session-created event from the gap was not replayed within one second with a synthetic Last-Event-ID. Querying sessions recovered the missing session. |
| Kill server tree and restart | Previously saved session and message remained readable. This was an idle session, not interrupted model inference. |
| Abort a running shell | Abort returned true; the controlled Node child was no longer alive and did not write its completion marker after the original deadline. |
| Kill only server during running shell | Shell child continued and wrote its completion marker after server death. On restart, persisted tool state was `running`, but the session was absent from the session-status response. The original HTTP request failed. |

The last experiment exposes three different realities: Central's transport has
failed, OpenCode's stored tool record remains running, and the external process
can still successfully finish its side effect. None alone proves task outcome.

## Proposed consequences for Central (not yet implemented)

1. Treat SSE as notifications; reconnect by reading current session/messages and
   reconciling artifacts. Do not rely on replay for durability.
2. Maintain Central's own durable run/task/attempt ledger and session mapping.
   An absent session-status entry must not mean completed, failed, or cancelled.
3. A server crash or failed HTTP request puts affected attempts into an unknown
   outcome requiring reconciliation. Do not automatically dispatch duplicates.
4. Check actual process state where accessible, workspace changes, artifact
   versions and verification evidence before accepting or repeating work.
   Marker files in this spike are diagnostic evidence, not a production task
   completion protocol.
5. Require task-specific reconciliation for external side effects such as PRs
   or deployments. Central cannot infer them from local process exit alone.
6. Record configured and observed OpenCode version. Gate the adapter on a tested
   schema/version combination; do not mix documentation generations.
7. Process supervision is separate from session cancellation. A later Rust
   implementation should evaluate Windows Job Objects or equivalent lifecycle
   management. No such mechanism is established by this experiment.

## Remaining verification before adapter design is finalized

- Real model request, structured task result and continuation after interruption.
- Permission and question requests, including reloading pending requests.
- Profile/model/variant/skill/subagent configuration and concurrent isolation.
- Cost/token visibility and limitations of a shared budget.
- Recovering or replacing an interrupted shell/tool record in a session.
- Cancellation of deeper, detached and independently scheduled process trees.
- Reboot/power-loss behavior and durable writes under storage failure.

No result here proves exactly-once execution, universal cancellation, general
session resume, or agent reasoning quality. The tests support only the bounded
observations listed above.
