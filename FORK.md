# About this fork

This is a fork of [google/ax](https://github.com/google/ax) that runs ax on
[Agent Substrate](https://github.com/agent-substrate/substrate)
**v1.0.0** (`go.ate.dev/substrate`, `api/ateapi/v1`) for the
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

As of the rebase onto upstream `241bc4d` (2026-11-06) and Substrate
v1.0.0. Hashes change on every rebase; match patches by subject.

| Patch | Commits | Upstream | Drop when |
|---|---|---|---|
| Tasks declare egress and injected credentials (`spec.egress`) | ba8043d | google/ax#449; the `Gateway`/`TrafficPolicy` proposal google/ax#549 would cover it | ax creates the Substrate egress policy itself |
| Substrate work and its status writes run on a context detached from the caller | 8df0718 | google/ax#452 | #452 is fixed |
| A task serves its own API through the runner (`spec.http.port`); readiness waits for it; forwarded requests carry the loopback Host, with the router's name in X-Forwarded-Host | cbc5b15, 63c1f74, b54ffea | google/ax#501, closed as a duplicate of #549 (ingress rules per task). The router's CONNECT path already reaches any actor port; the readiness wait is the part still missing | upstream can route to, or wait for, a task's own port |
| Suspend idle and finished tasks (`spec.idle`, `spec.onCompletion`): runner activity and exit status, the controller's `CheckIdle`, the periodic pass in ax-server, the `AutoSuspended` condition, the router-fallback deadline, mTLS status reads through the router, separate validation, and the server test | d55028d, 71deda2, 74ef46e, d2bce8e, 3536b9b, 2f7c16e, 0b9d757, 7b75b0a, 96daf35, 3185f6a | google/ax#420 | upstream ships automatic suspension |
| Resume tasks from memory snapshots (`--snapshot-scope full` → `MEMORY`), golden snapshots off for per-task templates, resume an interrupted `REVERTING` revert | 03bbfc2, 7bc73e1 | google/ax#443 (closed by #457, which uses `VOLUMES`) | upstream offers memory-snapshot resume |
| Keep the Ready condition's update time stable across reads: it takes the actor's `metadata.update_time` (or WorkspaceReady's, if later, for a running task) | 36b7135 | google/ax#539 | upstream fixes #539 |
| Fork-only proto fields at 100 and up | cfb7642 | — | with their patches |
| Docs, AGENTS.md, the substrate skill, and this file | 9b6d047, cda992d, c2d9109, 4a6b37b, 6c3c78a, da46aa2 | — | with their patches |

Dropped in the 2026-11-03 rebase because upstream now covers them: building
against Substrate v1.0.0 (upstream #553), and templates setting no
snapshot storage location (#553 leaves `storage_location` empty unless a
bucket is passed, and ax never passes one).

Dropped in the 2026-10-31 port: resource limits (#405), the runner ignoring
unknown Task fields (#506, fixed our #503), Redis persistence (ax stores its
state in Postgres, #515, which closed #442), and syncing the stored phase and
worker address after a router wake (#515, which fixed our #502).

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
