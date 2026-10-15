# About this fork

This is a fork of [google/ax](https://github.com/google/ax) that runs ax on
[Agent Substrate](https://github.com/agent-substrate/substrate) v0.5.0 for the
agent-substrate demo cluster. google/ax doesn't take outside pull requests, so
the changes live here and the asks go upstream as issues. Each patch below
should be dropped once upstream has an equivalent.

## How the branch is kept

- `main` is upstream `main` plus the patches below, one commit each (or a
  small group per feature). It is **rebased** onto upstream `main`, never
  merged, so it gets force-pushed.
- Rebase whenever we need something from upstream (a fix, a Substrate bump),
  and at least every couple of weeks otherwise, so conflicts stay small.
- Before a rebase replaces a commit that's deployed, tag it
  `deployed/<name>-<short sha>`, so running images still map to a commit.
- After a rebase, run the smoke test: create a task, let it go idle and
  suspend, wake it through the router, and check it resumes from its memory
  snapshot.

## Patches

| Patch | Commits | Upstream | Drop when |
|---|---|---|---|
| Tasks declare egress and injected credentials | 2fd0676 | google/ax#449 | ax creates the Substrate egress policy itself |
| Runner images keep working when the Task schema grows | 37a166c | not filed | ax tolerates unknown Task fields in the runner |
| A client timeout doesn't cancel a reconcile halfway | 4e29148 | google/ax#452 | #452 is fixed |
| A task serves its own API through the runner (`spec.http.port`), readiness waits for it | e3fe814, 6d52ed7 | not filed | upstream can route to a task's own port |
| Suspend idle and finished tasks (`spec.idle`, `spec.onCompletion`), runner reports activity and exit, periodic idle check | 734d1ac, 03461e8, 5c6b392, c718ff9, c0251f9 | google/ax#420 | upstream ships automatic suspension |
| Keep the recorded worker address current, forget deleted tasks; router fallback deadline | 4c8f3af, 03c0cd8 | not filed | upstream tracks router wakes |
| Resume from full (memory) snapshots; Substrate API changes | cb4e28f | google/ax#443 (closed by #457, which uses `VOLUMES`) | upstream offers memory-snapshot resume |
| Apply Task resource limits to the ActorTemplate | cf7fac4 | not filed | upstream sets actor limits |
| Build against Agent Substrate v0.5.0 (MEMORY/VOLUMES fidelity, REVERTING, no golden snapshots for per-task templates) | e8bc98d | — | upstream pins v0.5.0 or later with the same behavior |
| Docs | b7e8b0a, 5500872 | — | with their patches |

Commit hashes are as of the 2026-10-15 rebase onto upstream 22954f6 and change
on every rebase; match patches by subject.
