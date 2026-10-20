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
| Tasks declare egress and injected credentials | 93dc4b1 | google/ax#449 | ax creates the Substrate egress policy itself |
| Runner images keep working when the Task schema grows | 4363c25 | google/ax#503 | the runner ignores unknown Task fields |
| Reconcile and status writes run on a context detached from the caller | 09d9dd8 | google/ax#452 (upstream #490 lengthened the CLI timeout to 5 minutes, so our CLI change was dropped) | #452 is fixed |
| A task serves its own API through the runner (`spec.http.port`), readiness waits for it | 6b24aba, e29b160 | google/ax#501 | upstream can route to a task's own port |
| Suspend idle and finished tasks (`spec.idle`, `spec.onCompletion`), runner reports activity and exit, periodic idle check | fd4183f, fc321c2, 7fc3445, 379013f, 9d7966f | google/ax#420; the status sync after a router wake is google/ax#502 | upstream ships automatic suspension |
| Keep the recorded worker address current, forget deleted tasks; router fallback deadline | 6e480a2, 773a9b1 | fixes to our idle check (#420), not upstream issues | with the idle-suspend patches |
| Resume from full (memory) snapshots; Substrate API changes | 1e6820a | google/ax#443 (closed by #457, which uses `VOLUMES`) | upstream offers memory-snapshot resume |
| Apply Task resource limits to the ActorTemplate | e0549aa | port of google/ax#405, merged upstream 2026-10-19 (b2c59ae) | **drop at the next rebase** |
| Build against Agent Substrate v0.5.0 (MEMORY/VOLUMES fidelity, REVERTING, no golden snapshots for per-task templates) | 6d34eee | upstream now builds against v0.5.0 too (#484, d5ff120); our MEMORY/REVERTING/golden changes remain | upstream has the same snapshot behavior |
| Keep ax-redis data on a disk with an append-only file | 464baeb | google/ax#442 (commented) | upstream persists ax state (planned move to Postgres) |
| Docs | eb9fd12, 8b24f2e | — | with their patches |

Commit hashes are as of the 2026-10-19 rebase onto upstream 8888435 and change
on every rebase; match patches by subject.
