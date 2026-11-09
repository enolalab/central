# Real inference attempt — 2026-11-09

- OpenCode 1.19.5; isolated state and workspace; tools denied.
- No usable user provider credentials were available. The default user database
  also failed the CLI auth-list command with `Database is not empty and has no
  session table`; it was not modified or repaired.
- The isolated server exposed the `opencode` provider. Selected its catalogued
  zero-price model `opencode/glm-5.3-flash-free`.
- Submitted a real model prompt asking only for a JSON object with a random nonce
  and `sum: 42`. No project source was included in the prompt. OpenCode may add
  its normal agent system instructions and environment metadata.
- OpenCode's own log recorded a real stream request followed by:

  `AI_APICallError: Free usage limit reached.`

- Stopped the isolated server after observing the quota error to avoid further
  automatic retries. The waiting HTTP request consequently ended with
  `fetch failed`. This transport error is secondary to the recorded provider
  rejection.
- No successful model text, token accounting or agent task completion was
  observed. A catalogued zero price does not establish available quota.

Next live attempt requires an available provider/quota configured by the user.
Do not repeat this free-provider request until that external condition changes.

Reproducible script: `../live-probe.mjs`. Raw summary: `live-report.json`.
