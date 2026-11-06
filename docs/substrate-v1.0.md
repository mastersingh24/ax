# Agent Substrate v1.0

This fork of AX builds against `go.ate.dev/substrate` **v1.0.0** and needs a v1.0 control plane. Coming from v0.5, Substrate is a fresh install: v1.0 moved its API to the `substrate.ateapi.v1` package, renumbered the proto fields and serves only `ate.dev/v1` CRDs, so nothing stored by a v0.5 install (actors, templates, snapshots) carries over. Upgrade `ax-server`, the task images and Substrate together. From v1.0 on, Substrate upgrades between minor versions in place.

## What changed for AX

| Substrate change | What AX does now |
|---|---|
| The control API's Go package moved to `api/ateapi/v1` and its fields were renumbered ([substrate#2996](https://github.com/agent-substrate/substrate/pull/2996), [#2888](https://github.com/agent-substrate/substrate/pull/2888)). | Built against the new module; the import alias stays `ateapipb`. |
| `SnapshotConfig.storage_location` is forbidden unless the `PerTemplateSnapshotLocation` preview gate is on; snapshots go to ate-api-server's `--snapshot-location`. | Templates set no location. There is no `AX_SNAPSHOTS_BUCKET` any more: configure snapshot storage in Substrate. |
| `SandboxConfig` removed from ActorTemplate; the WorkerPool decides the sandbox class ([substrate#2780](https://github.com/agent-substrate/substrate/pull/2780)). | Templates no longer set it. |
| Every ActorTemplate needs CPU and memory limits, as integers (`cpuMillicores`, `memoryBytes`; [#2285](https://github.com/agent-substrate/substrate/pull/2285), [#2769](https://github.com/agent-substrate/substrate/pull/2769)), and they are booked against the worker's capacity. | A task without `spec.resources.limits` gets 1 CPU and 512 Mi (upstream AX). Set limits deliberately: they decide how many tasks fit on a worker. |
| The router accepts only mTLS; the insecure mode is gone ([#2977](https://github.com/agent-substrate/substrate/pull/2977)). | `ax-server` reaches the router with its pod-identity certificate, for workspace readiness and for the idle check's runner status (see [Installation](installation.md)). Its SPIFFE ID must be in the router's allowed list. |
| API authorization is always on. | `ax-server` needs access policies on the atespaces it serves (see [Installation](installation.md)). |
| `PauseActor` removed; `SuspendActor` returns once the snapshot is captured on the node and uploads it in the background ([#2805](https://github.com/agent-substrate/substrate/pull/2805)). | Nothing: AX never pauses actors, and a resume elsewhere waits for or uses the uploaded copy. |
| A memory snapshot is restored only on a worker whose CPU has the snapshot's features; with none free, the resume falls back to `ROOTFS` ([#2660](https://github.com/agent-substrate/substrate/pull/2660)). | Nothing. A `full`-scope task woken by the fallback keeps its files but restarts its processes (see [Automatic suspension](idle-suspend.md#what-a-woken-task-looks-like)). Mixed worker pools no longer crash such restores. |

Unchanged from v0.5 and still relied on: the snapshot fidelity ladder (`VOLUMES`, `ROOTFS`, `MEMORY`; `--snapshot-scope=full` asks for `MEMORY`, `data` for `VOLUMES`), `goldenSnapshotConfig.mode: DISABLED` on per-task templates, and the `REVERTING` actor state, which AX resumes with `RevertActor` instead of deleting the actor.

## Upgrading from the v0.5 fork

1. Install Substrate v1.0 fresh, with `--snapshot-location`, the router's allowed SPIFFE IDs, and access policies for `ax-server`.
2. Deploy AX's Postgres (`deploy/postgres.yaml`, Secret `ax-postgres` with key `password`) and this `ax-server`. AX no longer uses Redis; there is nothing to migrate, because no actor survives the reinstall.
3. Rebuild task images with this fork's `ax-task-runner`. It reads one workspace (`AX_WORKSPACE_YAML`, `/metadata/v1alpha1/ax/workspace`); an older runner finds no workspace, and a command that needs its files exits at startup.
4. Change task manifests: `workspaces: [...]` becomes `workspace: {...}` (one), and set `spec.resources.limits`.
5. Clients that vendor `ax.proto` (lookout, switchboard, substrate-scope) must re-vendor: the fork's own `TaskSpec` fields moved to 100 and up (see [FORK.md](../FORK.md)).
6. Re-apply workspaces and recreate tasks.
