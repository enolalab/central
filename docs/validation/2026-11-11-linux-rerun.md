# Linux verification rerun — 2026-11-11

Source revision: `a00a2b5fda46887fdd9dda8fbe2b9b32868d2cdf`.
Host: Linux `7.0.0-42-generic`, `x86_64-unknown-linux-gnu`.
Rust: `1.99.1 (58cf8f9c3 2026-10-13)`; Cargo: `1.99.1`.
Codex CLI: `0.166.0`.

This is a fresh execution on Linux, separate from the earlier Windows
evidence. Product and test source files were not changed for this rerun.
`CXXFLAGS=-g0` disables C++ debug symbols for the bundled DuckDB build;
the test profile, debug assertions, and test assertions remain enabled.
The initial builds were restarted with 12 jobs to reduce build time.

## Formatting

`cargo fmt --check`: **PASS**, exit 0.

## Clippy

```sh
CXXFLAGS=-g0 cargo clippy --workspace --all-targets --locked -j 12 -- -D warnings
```

**FAIL**, exit 101. The Linux build reports four diagnostics in two test
targets:

- `tests/process_ownership.rs:22`: unused imports
  `current_process_in_job`, `process_creation_time_ms`, `process_in_job`.
- `tests/process_ownership.rs:74`: unused helper `fail_assign`.
- `tests/artifact_contract.rs:13`: unused imports `CheckRequest`, `CheckRunner`.
- `tests/artifact_contract.rs:16`: unused imports `ApprovedCommand`,
  `SupervisedCheckRunner`.

These declarations serve Windows-only tests but are not themselves
conditionally compiled for Windows. `-D warnings` makes them gate failures
on Linux. No warning suppression or source fix was applied for the rerun.

## Workspace tests

```sh
CXXFLAGS=-g0 cargo test --workspace --locked --no-fail-fast -j 12
```

**FAIL**, exit 101: **236 passed, 1 failed, 1 ignored** across the reported
unit, integration, and doc-test targets. `--no-fail-fast` allowed the
remaining targets to run after the failure.

The failed test is
`central-infrastructure::git_integration::integration_checks_run_baseline_affected_acceptance`,
at `crates/central-infrastructure/tests/git_integration.rs:403`:

```text
run checks: Forbidden("check path escapes the work root:
/tmp/central-integration-checks-34181-10003/state/worktrees/integration-00000000-0000-0000-0000-000000001900-2")
```

Running the same compiled test binary with only this test selected also
failed (exit 101, 1 failed), using a new fixture directory:

```sh
target/debug/deps/git_integration-4d88f845d802fb99 \
  integration_checks_run_baseline_affected_acceptance --exact --nocapture
```

Source inspection points to absolute-path normalization in
`crates/central-infrastructure/src/checks/runner.rs:91`: the normalizer
starts from an empty path for an absolute input and skips the initial
empty component, turning a Unix `/tmp/...` path into relative `tmp/...`.
The containment check then rejects the integration worktree. This is a
reproducible failure on this host; Windows behavior was not retested.

Notable passing suites:

| Suite | Passed |
| --- | ---: |
| `api_auth` | 8 |
| `api_contract` | 8 |
| `api_events` | 7 |
| `api_workflow` | 6 |
| `artifact_retention` | 3 |
| `release_security` | 3 |
| CLI `api_parity` | 3 |
| `local_runtime` | 10 |
| `workflow_recovery` | 7 |
| `store_contract` | 10 |
| `quality_loop` | 16 |
| `git_workspace` | 4 |

## Installed Codex app-server initialization

The workspace run ignored `installed_cli_stdio_smoke` by default. It was
then explicitly selected from the compiled test binary:

```sh
CENTRAL_CODEX_SMOKE=1 \
  target/debug/deps/process_ownership-edfc1f0409d3c3f9 \
  installed_cli_stdio_smoke --ignored --exact --nocapture
```

**PASS**, exit 0, 1 passed:

```text
codex version: codex-cli 0.166.0
initialize ok (no inference performed)
```

This proves CLI execution and an app-server `initialize` response only.
It does not prove authenticated model execution, thread lifecycle,
worker/verifier completion, or a live end-to-end workflow.

## Scope limits

- Windows-only tests selected with `cfg(windows)` are not compiled or
  executed on this Linux host; they are separate from the one ignored test.
- No `pwsh` or `.toolchain/env.ps1` exists on this host. Windows-oriented
  PowerShell gates, release packaging, and the Windows zip smoke were not
  rerun. Their earlier evidence remains historical.
- The standalone DuckDB suitability benchmark/crash gate was not executed;
  `cargo test` builds its test target with zero tests and does not execute
  the gate binary's main program.
- No model inference or manual reboot acceptance was performed.
- The existing GC implementation remains dry-run only.

## Assessment

The repository contains substantial P1–P4 implementation, and the API,
CLI parity, storage, and recovery suites pass on this host. The current
workspace test and Clippy gates are nevertheless **not green** on Linux.
Together with the unverified live workflow and the scope limits above,
this does not support marking every plan or the MVP acceptance complete.
The successful installed-CLI initialization supersedes the earlier
environment's lack of a reachable Codex executable only for this narrow
smoke; it does not supersede the unverified live-model workflow.

Machine-local raw logs from this execution:

- `/tmp/central-test-suite-20261111.log`
- `/tmp/central-git-integration-recheck-20261111.log`
- `/tmp/central-codex-init-smoke-20261111.log`
- `/tmp/central-clippy-20261111.log`
