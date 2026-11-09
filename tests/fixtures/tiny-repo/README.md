# Tiny fixture repo (P2 Task 5)

One function, one test. The gate script `scripts/test-codex-single-task.ps1`
materializes this fixture into fixture-owned temp scratch (never the real
repo, never a production worktree) and drives one worker edit plus one
independent verifier pass over it.

Canonical content (the script writes exactly this, then the worker edits it):

`calc.py`:

```python
def add(a, b):
    return a + b
```

`test_calc.py`:

```python
from calc import add


def test_add():
    assert add(1, 2) == 3
```

Worker edit (scripted in fake mode, one harness turn in `-RealModel` mode):
change the body to validate inputs, e.g. reject booleans, without changing
the `add(1, 2) == 3` contract. The verifier runs in its own session/scratch,
receives the immutable candidate plus the input and criteria hashes, and
never edits the candidate or the criteria.

Rules for both modes:

- The fixture lives in temp scratch only; P2 never touches production Git
  worktrees (those belong to P3).
- Real-model mode uses at most 3 agent turns, stops on quota/auth errors,
  records usage/unknown, and never sends the real repo.
- P2 evidence is never agent word alone: hashes, check evidence and test
  counts land in `docs/validation/p2-codex.md`.
