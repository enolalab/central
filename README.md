# Central

Local coordinator for long-running agent work: one daemon owns state,
scheduling, approval, budget and verification; the CLI and any
independent client drive it through a documented public HTTP API.
No bundled UI, no remote access — loopback only, one user.

## Layout

```text
crates/central-domain/          pure invariants (no IO)
crates/central-application/     use cases + ports
crates/central-infrastructure/  DuckDB, Codex, Git, filesystem, process adapters
crates/central-server/          daemon, public v1 API, SSE
crates/central-cli/             the official client (pure API consumer)
contracts/                      openapi-v1.yaml/json, events schema, examples
examples/                       independent PowerShell + browser clients
scripts/                        gates, packaging, opt-in model smoke
docs/                           api.md, client-integration.md, getting-started.md, recovery.md
docs/validation/                per-gate evidence
```

## Quick start

See `docs/getting-started.md`. In short:

```powershell
$env:CENTRAL_STATE_DIR = "$HOME\.central"
central daemon start
central meta
central init --goal "..." --repository "D:/work/demo"
```

The public contract is `docs/api.md` with
`contracts/openapi-v1.yaml`; client writers start at
`docs/client-integration.md`.

## Gates

```powershell
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
pwsh -File scripts/test-mvp-e2e.ps1
pwsh -File scripts/build-release.ps1
```

Evidence per gate lives in `docs/validation/`.
