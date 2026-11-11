# Central local browser UI — discussion mockup

**Scope update — 2026-11-10:** Central now targets daemon + CLI + a documented
API for independent clients, with no bundled UI. These mockups preserve earlier
design exploration; the UI choices and acceptance notes below are historical,
not current MVP requirements. Anyone may use the public API to build a separate UI.

`central-workspace-v1.html` is a standalone, self-contained HTML/CSS/JS mockup.
It is disposable design material, not the Central web application. Open it in
a browser, or serve through the brainstorming companion for recorded UI choices.
No dependencies, network model calls or Central backend are used.

The user selected a browser UI with leader chat as the primary experience and
structured plan/progress alongside it. This mockup explores that direction; the
specific visual design is not yet approved.

Scenarios: Brainstorm, plan approval, execution and recovery awaiting explicit
resume. Interactions include plan comparison/approval, task overview/diff/verify/
log, artifact provenance, local profile selection and scripted chat messages.
Navigation into other runs, sync and extra actions currently show explanatory
demo feedback; they do not perform real operations.

Browser QA on 2026-11-09:

- Confirmed two-column layout and visible composer in the narrow browser pane.
- Confirmed full layout with run sidebar in a wider browser pane.
- Sent `ok` in demo chat: plan remained awaiting explicit approval.
- Clicked plan approval: scenario moved to execution and task details.
- Inspected independent verifier criteria.
- Opened recovery: scenario waited for explicit resume.

All displayed repository names, commits, times, tasks and outcomes are fictional.
The companion's scenario controls are prototype tooling, not proposed product
navigation. Profile edits change demo labels only. Browser exploration does not
approve the design spec or authorize running a real agent.

The live companion files and session key are under `.superpowers/`, excluded
from source control. Keep this standalone copy as the design reference.

## Pixel office exploration

The user requested a more visual pixel-office direction. The current reference
is `central-pixel-office-v3.html`; v1/v2 preserve earlier layout iterations.
The office art and sprites are drawn locally in canvas, with no downloaded game
assets or image dependencies. Clickable character labels are native buttons.

The office shows the leader, implementation workers, verifier and integrator.
Selecting a character updates the task inspector below the map while leader
chat remains visible. Attention bubbles open the scoped approval in the inspector.
The map can switch to a text task list. Reduced-motion and handoff-line controls
are available. Worker seats remain unoccupied before plan approval.

Browser QA: selected verifier and its criteria, opened worker approval, accepted
the scripted one-time operation and observed the state change, switched to
brainstorm and checked uninitialized worker labels, then toggled task-list and
office views. The live browser was left on the office with the pending demo
approval. No real approval or agent execution occurred. These interactions do
not yet test Central's domain or authorize the complete product design.

## Immersive office — selected direction

After seeing the office/dashboard split, the user requested a different kind of
interface. `central-immersive-office-v1.html` explores a room-first experience:
full-screen office, small floating run indicators, and a bottom interaction dock.
No permanent sidebar, task inspector or chat column reserves space.

- Select the leader to open a floating conversation, preserving its history.
- Select a worker to open contextual task/verify/diff information.
- Select the physical noticeboard to open the plan.
- Open the task book from the dock; close panes or press Escape to return to room.
- Approval attention remains visible on the character and top-right HUD.

The user accepted this direction after viewing it: the prior always-visible
leader chat is replaced by persistent history with on-demand access. The product
design now records this behavior. This acceptance concerns the UI direction;
it does not make the mockup a working product or approve the whole design spec.

Browser QA: opened leader chat from its character, opened the plan from the
noticeboard, closed the pane, opened and closed the task book. The office stayed
visible behind each contextual pane. The live view was left with all panes closed.
All content/actions remain fictional and local; this is not a running Central.
