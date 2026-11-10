# Git-repo fixture (P3 Task 5)

Integration tests never touch a real repository. Every test generates its
own fixture repo under the OS temp dir
(`central-<name>-<pid>-<counter>`) with `git init`, so:

- no migration of a real repo, no production worktree, no global git
  config mutation (hermetic env: `GIT_CONFIG_NOSYSTEM=1`,
  `GIT_TERMINAL_PROMPT=0`, per-command `-c core.hooksPath=<owned-empty>`
  so Central's mechanical git operations never fire user hooks);
- paths with spaces and non-ASCII names flow through argv only —
  see `space_unicode_and_junction_paths` in
  `crates/central-infrastructure/tests/git_workspace.rs`;
- junctions (or, where unavailable, absolute outside paths) must fail
  closed through ancestor-aware containment —
  see `contained_path` in `crates/central-infrastructure/src/workspace/paths.rs`;
- one detached worktree per attempt (`git worktree add --detach`),
  removed only through explicit ownership-checked cleanup;
- the good ref (`refs/heads/central-good` in fixtures) moves only by
  `git update-ref <ref> <new> <old>` compare-and-swap after passing
  baseline + affected + acceptance checks — failed checks and merge
  conflicts never move it;
- `central sync RUN --commit SHA` only reads (`cat-file -e`,
  `status --porcelain`) and appends a sync-intent line under
  `<state-dir>/sync-queue/<run>.jsonl`; it never checks out, never
  stashes, never pulls (daemon pickup of the queue lands in P3/T6,
  which owns the scan-loop wiring).
