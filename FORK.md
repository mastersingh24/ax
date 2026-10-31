# About this fork

This is a fork of [google/ax](https://github.com/google/ax) that runs ax on
[Agent Substrate](https://github.com/agent-substrate/substrate)
**v1.0.0-rc.1** (`go.ate.dev/substrate`, `api/ateapi/v1`) for the
go-steer clusters (agent-substrate, shipmind-stage, shipmind-prod), which
deploy it through go-steer/shipmind. google/ax doesn't take outside pull
requests, so the changes live here and the asks go upstream as issues. Each
patch below should be dropped once upstream has an equivalent.

## How the branch is kept

- `main` is upstream `main` plus the patches below, one commit each (or a
  small group per feature). It is **rebased** onto upstream `main`, never
  merged, so it gets force-pushed.
- Rebase whenever we need something from upstream (a fix, a Substrate bump),
  and at least every couple of weeks otherwise, so conflicts stay small.
- Before a rebase replaces a commit that's deployed, tag it
  `deployed/<name>-<short sha>`, so running images still map to a commit.
  Each pushed port is also tagged `substrate-v1-<short sha>`.
- After a rebase, run the smoke test: create a task, let it go idle and
  suspend, wake it through the router, and check it resumes from its memory
  snapshot.

## Proto fields

The fork's own `TaskSpec` fields are numbered from **100**, so an upstream
addition can never take one of their numbers (upstream's `config = 7` once
collided with the fork's `egress = 7`). Clients that vendor `ax.proto`
(k8s-lookout, switchboard, substrate-scope) must use these numbers:

| Field | Number | Type |
|---|---|---|
| `TaskSpec.egress` | 100 | `repeated EgressRule` |
| `TaskSpec.http` | 101 | `TaskHTTP` |
| `TaskSpec.idle` | 102 | `TaskIdle` |
| `TaskSpec.on_completion` | 103 | `string` (`Keep`, `Suspend`) |

The fork-only messages number their own fields from 1:
`EgressRule{hosts=1, ports=2, credentials=3}`,
`CredentialInjection{header=1, prefix=2, credential_uri=3}`,
`TaskHTTP{port=1}`, `TaskIdle{suspend_after=1, busy_path=2}`.

The fork also records an ax-owned status condition, `AutoSuspended`
(reasons `IdleSuspended`, `CompletedSuspended`, `ResumedByRequest`); see
[docs/idle-suspend.md](docs/idle-suspend.md).

## Patches

As of the port onto upstream `f0228f1` (2026-10-28) and Substrate
v1.0.0-rc.1. Hashes change on every rebase; match patches by subject.

| Patch | Commits | Upstream | Drop when |
|---|---|---|---|
| Tasks declare egress and injected credentials (`spec.egress`) | 3e2447a | google/ax#449 | ax creates the Substrate egress policy itself |
| Substrate work and its status writes run on a context detached from the caller | b9dc57b | google/ax#452 | #452 is fixed |
| A task serves its own API through the runner (`spec.http.port`); readiness waits for it | 55905d5, 512489d | google/ax#501 (the router's CONNECT path already reaches any actor port; the readiness wait is the part still missing) | upstream can route to, or wait for, a task's own port |
| Suspend idle and finished tasks (`spec.idle`, `spec.onCompletion`): runner activity and exit status, the controller's `CheckIdle`, the periodic pass in ax-server, the `AutoSuspended` condition, the router-fallback deadline, mTLS status reads through the router, separate validation, and the server test | 3f985ce, babe81f, 883f108, fb13d84, 7d13608, 7998552, 48700b6, e29b8e9, 14b0c58, b970f58 | google/ax#420 | upstream ships automatic suspension |
| Resume tasks from memory snapshots (`--snapshot-scope full` → `MEMORY`), golden snapshots off for per-task templates, resume an interrupted `REVERTING` revert | f0d4230, 68a4ede | google/ax#443 (closed by #457, which uses `VOLUMES`) | upstream offers memory-snapshot resume |
| Build against Substrate v1.0.0-rc.1 | eecb85f | — | upstream moves to v1.0 |
| Templates set no snapshot storage location (v1.0 forbids one without the `PerTemplateSnapshotLocation` preview gate) | b7cd79d | not filed | upstream stops sending `storage_location` |
| Fork-only proto fields at 100 and up | 909865a | — | with their patches |
| Docs, AGENTS.md and the substrate skill | d08b125, ebb1d2c, a24a71c, a161f13, 91cfb6e | — | with their patches |

Dropped in this port because upstream now covers them: resource limits
(#405), the runner ignoring unknown Task fields (#506, fixed our #503), Redis
persistence (ax stores its state in Postgres, #515, which closed #442), and
syncing the stored phase and worker address after a router wake (#515 reads
both from Substrate on every get and list, which fixed our #502).

## Deploying

- **Postgres:** `deploy/postgres.yaml` is upstream's, unchanged. It reads its
  password from Secret `ax-system/ax-postgres` (key `password`); shipmind
  creates that Secret once. `ax-server` takes `--store=postgres://...`.
- **Snapshots:** configure `--snapshot-location` in Substrate. AX has no
  snapshot-location setting any more (`AX_SNAPSHOTS_BUCKET` is gone).
- **Router:** mTLS only. `ax-server` uses its projected pod-identity
  certificate; add `spiffe://cluster.local/ns/ax-system/sa/ax-server` to the
  router's allowed SPIFFE IDs.
- **Task images** must carry this fork's `ax-task-runner`, which reads one
  workspace (`AX_WORKSPACE_YAML`, `/metadata/v1alpha1/ax/workspace`). Task
  manifests use `workspace:` (one), and should set `spec.resources.limits`;
  without them a task gets 1 CPU and 512 Mi.

## Building images

The fork uses upstream's build, so it carries no build patch:

```bash
make images AX_IMAGE_REPO=<registry/path> IMAGE_TAG=substrate-v1-<short sha>
```

That cross-compiles `ax-server` and `ax-task-runner` for linux/amd64
(`CGO_ENABLED=0`), builds `Dockerfile.server` (alpine with CA certificates)
and `Dockerfile.task-runner`, pushes both with podman or docker, and writes
their digests to `bin/ax-server.digest` and `bin/ax-task-runner.digest`. A CI
that builds its own images (shipmind) can run the same two `go build` lines
and `docker build -f Dockerfile.server .`. There is no `.ko.yaml`: upstream
dropped ko (#508). `ko build ./cmd/ax-server` still works, since the binary is
pure Go, but gives a different base image than upstream's.
