# Worker drain

When a node is drained (a GKE upgrade, a scale-down, a Spot reclaim), the Agent Substrate workers on it go away, and so would any task that is awake on them. AX suspends those tasks first, so they come back from a snapshot instead of failing.

## What Agent Substrate does

When a worker's pod starts terminating, Substrate's controller marks the Worker `WORKER_STATE_DRAINING`. It stops placing actors on it, but "deliberately leaves any bound Actor alone". The worker then sends `SIGTERM` to each hosted actor's main process and allows 30 minutes before killing it. Substrate's upgrade guide is explicit: "Substrate does not suspend them. An actor that is not suspended by then becomes `CRASHED` and loses its work since its last snapshot." A `CRASHED` actor only leaves that state through `RevertActor`.

agent-substrate/substrate#1707 tracks Substrate suspending actors on eviction itself. Until then, whatever drives the actor has to.

## What AX does

**Suspend on drain.** Every `--drain-check-interval` (10s by default; 0 turns it off), ax-server lists the Workers and, for each one that is draining, the actors assigned to it. Each running actor that belongs to a task is suspended with `DURABILITY_MODE_SYNCHRONOUS`, so the call returns only once the snapshot is in durable storage: an asynchronous suspend returns when the snapshot is on the node, and the node is about to go. An actor is suspended once per drain. The task records it on its `AutoSuspended` condition:

| Status | Reason | Message |
|---|---|---|
| `True` | `WorkerDraining` | `Worker <name> is draining; suspended synchronously (2 requests in flight were interrupted). The next request resumes it on another worker.` |

Once the task is running again, the next pass turns the condition `False` with reason `ResumedByRequest`, as the idle check does for idle tasks.

The part about requests in flight comes from the runner's status endpoint, the one the idle check reads. It says `no requests in flight` when there were none, and nothing when the status couldn't be read; the suspend never waits on it. A request open at that moment is cut off, as with any suspend. The next request through the router resumes the task on another worker, from its snapshot.

ax-server needs to read Workers, which takes the global owner role in Substrate's API authorization. An atespace-scoped driver can't see drain state (agent-substrate/substrate#3818).

**Keep the task alive through `SIGTERM`.** The worker's `SIGTERM` reaches `ax-task-runner`, which is the actor's main process. If the runner stopped the command then, there would be nothing alive left to snapshot. So on a first `SIGTERM` the runner keeps the task running for `AX_RUNNER_SIGTERM_GRACE` (a Go duration, `25m` by default, under Substrate's 30 minutes) and only then stops it. `SIGINT` or a second `SIGTERM` still stop it at once, and `AX_RUNNER_SIGTERM_GRACE=0` restores the old behaviour. A suspend freezes the runner; when it is restored on another worker, the wall clock has jumped, and the runner forgets the old worker's `SIGTERM`. Substrate's own terminate path sends `SIGKILL`, so deleting a task is not slowed down.

**Revert as the safety net.** If an actor still ends up `CRASHED` because its worker went away (the suspend failed, a Spot reclaim gave about 30 seconds of notice, the worker process restarted), ax-server reverts it to its latest snapshot and records:

| Status | Reason |
|---|---|
| `True` | `RevertedAfterWorkerLoss` |

The task then reads `Suspended`, not `Failed`, and its next request resumes it. Substrate reports why an actor crashed only as text, so AX matches the messages Substrate v1.0 uses for a lost worker (`worker pod went away while hosting the actor`, `worker restarted while hosting the actor`, `assigned worker is draining`). A crash with any other cause is left alone, so a task that fails as it starts doesn't loop. One crash is reverted once, and a revert that keeps failing is given up after three tries, leaving the task `Failed`.

## When it isn't enough

A suspend snapshots the task as it is. Work the agent hadn't persisted is kept only if the snapshot is a memory snapshot (`--snapshot-scope full`). With volume snapshots, the task resumes with its files but restarts its process. If the node disappears without a drain (a hardware failure), there is no `SIGTERM` and no window: the revert brings the task back to its last snapshot, whenever that was taken.
