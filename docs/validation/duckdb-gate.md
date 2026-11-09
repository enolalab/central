# DuckDB suitability gate — evidence (P1 Task 1)

Date: 2026-11-09. Spike: `spikes/duckdb-runtime-gate` (gate binary, not product
code). Gate runner: `scripts/test-duckdb-gate.ps1` (exit 0 only if every gate
passes; non-zero otherwise).

## Decision

**ADOPT DuckDB for P1 Task 3.** Every gate below passed on the measured
machine. This is evidence for this hardware/filesystem only, not an SLA for
all machines.

## Environment and deviation

| Item | Value |
| --- | --- |
| CPU | AMD Ryzen 9 7900 12-Core (24 logical cores) |
| OS | Microsoft Windows 11 Pro |
| Filesystem | NTFS, local disk D: (1 TB volume) |
| Rust | rustc/cargo 1.99.1, host `x86_64-pc-windows-gnu`, MinGW gcc/g++ 16.2.0 |
| DuckDB | crate `duckdb =1.20000.1` (bundled); library reports `v2.0.0` |
| Node | 24.22.1 (preflight only; not used by the gate) |

Deviation from the plan-pinned toolchain: the plan pins Rust 1.99.1 on
`x86_64-pc-windows-msvc` with MSVC build tools. This machine has **no MSVC**
(`link.exe` is absent and the MSVC target fails to link), so the gate was run
with the portable GNU toolchain in `.toolchain/` (exactly 1.99.1,
`x86_64-pc-windows-gnu`, selected in the gate script via
`RUSTUP_TOOLCHAIN=stable-x86_64-pc-windows-gnu`; `rust-toolchain.toml` still
pins channel `1.99.1`). DuckDB bundled compiles and passes under GNU here.
Any MSVC-specific DuckDB behavior and P4 packaging/VC-runtime notes must be
re-validated if the project later moves to MSVC.

## What the gate checks

Each durable transition is one transaction that updates the snapshot row and
appends one event, one outbox row and one receipt (four writes, atomic).

1. **Crash cycles (100).** Per cycle a child process commits transition N,
   prints an after-commit marker, then opens a second transaction, prints a
   before-commit marker and is killed by the parent. The parent reopens the
   file and asserts the committed set (snapshot + event + outbox + receipt)
   is whole, the uncommitted writes are absent, and the previously committed
   transition is intact. This is a process-kill/reopen test only; it is
   **not** a power-loss test.
2. **Duplicate command.** Same `(actor, command_id)` with the same payload
   hash returns the existing receipt without rewriting; the same ID with a
   different payload hash is a conflict and changes nothing.
3. **Lock.** While one process holds an open write transaction, a second
   process cannot write the same file (it errors on the file lock).
4. **Controlled IO failure.** Opening a database under a nonexistent
   directory and under a regular-file path component fails cleanly; the real
   database remains readable and intact afterwards. No disk was filled.
5. **Sustained load (release build).** Seed: 100 runs x 100 task metadata
   rows (10,000) plus 100,000 events. Then 60 s with 32 caller threads, one
   writer thread (~25.8 writes/s offered, at least the required 25/s), and
   snapshot reads at 5/s. Latency is enqueue-to-ack, including queue wait.
   Gate: zero lost acks / partial commits, p95 <= 100 ms, p99 <= 500 ms, no
   caller starvation (every caller completes >= 20 writes).

## Measured results (final run)

| Gate | Result |
| --- | --- |
| atomicity_ok (100 crash cycles) | true |
| recovery_ok (committed intact after kill/reopen) | true |
| duplicate_ok (same hash idempotent, other hash conflict) | true |
| lock_ok (second writer rejected) | true |
| io_failure_ok (controlled failures, DB intact) | true |
| zero_lost_ack (acks == receipts == events) | true |
| starvation_ok | true |
| reads_ok (305 reads, 0 failed; target >= 250) | true |
| p95_ms | 3.94 (gate <= 100) |
| p99_ms | 4.16 (gate <= 500) |
| sustained_writes_per_s | 25.22 (gate >= 24.5; target 25/s) |
| perf writes completed / sent | 1544 / 1544 |

A first measured run at exactly 25.0/s offered load reported 24.42 writes/s
because the measurement window includes thread start/stop jitter; the spike
now offers ~25.8/s so the measured sustained rate reflects durable acked
writes, and both subsequent runs measured 25.22 writes/s with `gate_pass:
true` and script exit code 0.

Reproduce: `.toolchain/pwsh/pwsh.exe -File scripts/test-duckdb-gate.ps1`
(about 2 minutes for crash cycles plus the 60 s sustained phase, excluding
the one-time bundled DuckDB build).
