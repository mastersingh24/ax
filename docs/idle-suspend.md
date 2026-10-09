# Automatic suspension

A running task is a live sandbox with its memory and CPU on an Agent Substrate worker, whether it is doing anything or not. Substrate packs many actors onto a worker, but only up to the worker's resources. Without help, AX only suspends a task right after creating it and when someone calls `ax suspend`, so a request-driven agent that answered its last request an hour ago, or a batch task whose command finished, keeps using those resources until a person notices. With `spec.resources.limits` set, that is also how new tasks end up with nowhere to run.

Agent Substrate's router resumes a suspended task on the next request addressed to it (about two seconds when warm), so for an agent that serves an API, suspending between requests costs one slow request and gives the worker back the rest of the time. Two optional fields let a task ask for that.

```yaml
spec:
  command: ["my-agent", "serve", "--listen=127.0.0.1:8484"]
  http:
    port: 8484            # the runner forwards requests to the agent here
  idle:
    suspendAfter: 10m     # suspend after ten minutes without a request...
    busyPath: /busy       # ...unless the agent says it is still working
  onCompletion: Suspend   # and suspend as soon as the command exits
```

| Field | Default | Meaning |
|---|---|---|
| `spec.idle.suspendAfter` | none | Suspend once no request has gone through `spec.http.port` for this long. A Go duration, at least `10s`. Requires `spec.http.port`. |
| `spec.idle.busyPath` | none | A path on `spec.http.port` the runner asks before declaring the task idle. See below. |
| `spec.onCompletion` | `Keep` | `Keep` leaves the task running after its command exits, which is today's behavior. `Suspend` suspends it. |

Tasks that set neither field behave exactly as before.

## What counts as idle

Idleness is measured on traffic, by the runner, because the runner already sits in front of the task: with `spec.http.port` set, every request from the router that is not for the runner's own endpoints is forwarded to the task. The runner records when the last forwarded request finished and how many are open. A request that is still open, including a server-sent event stream or a WebSocket, keeps the task active for as long as it stays open. Calls to the guest services (`ax ssh`) count too.

Requests to the runner's own endpoints (`/healthz`, `/readyz`, `/metadata/...`) do not count. The control plane and Agent Substrate poll those, and counting them would keep every task awake forever.

Traffic alone misses one case: an agent that accepted a request, answered "started", and is now an hour into an investigation with nobody connected. That is what `busyPath` is for. Before the control plane suspends an idle task, the runner sends `GET <busyPath>` to the task's port and the task answers:

```http
HTTP/1.1 200 OK
Content-Type: application/json

{"busy": true}
```

Only a `200` whose JSON body has `"busy": true` means busy. Any other answer, including `{"busy": false}`, another status code, an unreadable body, a refused connection, or no answer within two seconds, means not busy. The reason is reported, but the task can still be suspended: an agent whose server has died should not stay running forever. Keep the handler cheap; it is called on every check of an otherwise idle task.

## The runner's status endpoint

The runner reports what it knows at `GET /metadata/v1alpha1/ax/status`:

```json
{"idleSeconds": 412, "inFlight": 0, "busy": false, "exited": false, "exitCode": 0}
```

`idleSeconds` is zero while a request is open. `busyError` appears when the busy check failed. `exited` and `exitCode` describe `spec.command`; the exit code is `-1` when the command was killed by a signal. `resumedAfterExit` appears, set to `true`, when the task was suspended and woken again after the command exited (see [What a woken task looks like](#what-a-woken-task-looks-like)).

With full snapshots (the default, see below) a suspended task resumes with the runner's memory as it was at the suspend, so it remembers the last request before the suspend, possibly hours ago. The runner notices that it was frozen (its once-a-second clock tick jumps, or a status read finds the last tick too far back) and restarts the idle clock. The control plane also never treats a task as idle for longer than it has seen it running, so a task that was just resumed is never suspended straight away. With data-only snapshots the runner starts afresh on resume, so its clock starts at zero anyway.

## How the control plane applies it

`ax-server` checks every task that sets `spec.idle` or `spec.onCompletion: Suspend` and is `Running` or `Suspended`, every `--idle-check-interval` (30 seconds by default; `0` turns it off). For each one, under the task's lock:

1. It asks the Substrate control API for the actor's state. That call never wakes an actor.
2. If the actor is suspended, the runner is not contacted. Asking a suspended task anything through the router would resume it, so this is the rule that keeps suspended tasks suspended. If AX had the task as `Running`, it is recorded as `Suspended` with reason `ActorSuspended`.
3. If the actor is running but AX had the task as `Suspended`, the router resumed it for a request. The task is recorded as `Running` with reason `ResumedByRequest`, so `ax get` matches reality.
4. For a running actor it reads the status endpoint, directly from the worker's address, or through the router with `ate-target-actor` if that address can't be reached. The actor was running a moment ago, so the router has nothing to wake.
5. It suspends the actor when the command has exited and `onCompletion` is `Suspend` (reason `CompletedSuspended`, with the exit code in the message), or when the task has been idle for `suspendAfter`, has no open requests, and isn't busy (reason `IdleSuspended`).

The task's `Ready` condition carries the reason. `ax resume`, or simply the next request through the router, brings it back.

## What a woken task looks like

What a task looks like when it wakes depends on what its snapshots capture. `ax-server` sets that on every ActorTemplate it creates with `--snapshot-scope` (or `AX_SNAPSHOT_SCOPE`); see [Agent Substrate v0.4](substrate-v0.4.md) for the Substrate side.

| | `full` (default) | `data` |
|---|---|---|
| Snapshot holds | process memory, root filesystem, durable directories (`/workspace`) | durable directories only |
| On wake | the task carries on where it was: same processes, same memory, files outside `/workspace` kept | the containers start afresh from the image with `/workspace` restored: the runner and `spec.command` start again |
| Snapshot size, suspend and wake time | larger and slower, growing with the task's memory and filesystem changes | small and quick to save; wake pays the app's startup |
| Good for | agents that keep state in memory (a conversation, a loaded model, a warm cache) | agents that keep everything they need in `/workspace` and start quickly |

The scope is fixed when a task's template is created, so changing the flag affects tasks created afterwards; existing tasks keep the scope they started with.

The two differ for `onCompletion: Suspend`:

- With `full`, the command is not run again. A request or `ax resume` that wakes a finished task finds it as it was when the command exited, which is what you want for looking at results or `ax ssh`. The runner reports `resumedAfterExit`, and instead of suspending the task on the spot the control plane waits until it has gone `spec.idle.suspendAfter` (five minutes when that is not set) without a request, then suspends it again with reason `CompletedSuspended`. To run the command again, create the task again.
- With `data`, the runner starts afresh, so the command runs once more and the task is suspended again when it exits.

Agent Substrate v0.3 restored data-only snapshots on top of the template's golden snapshot. v0.4 removed that mode (`onResume.fromData: GOLDEN`); `data` now always restarts from the image.

A runner image built before this change has no status endpoint. The control plane then makes no decision for that task, so upgrading `ax-server` first is safe. A runner built before `resumedAfterExit` never reports it, so with full snapshots a finished `onCompletion: Suspend` task it runs is suspended again at the first check after it is woken; use a current runner image with `full`.

## Relation to Agent Substrate's worker-initiated suspend

Agent Substrate (v0.3.0 and v0.4.0) has the plumbing for a sandbox to ask for its own suspension (`RequestActorSuspend`, from the guest through atelet to the control plane), with no caller yet. Once the runner can reach it, it could make the same decision locally and drop the polling, with the control plane recording the result. The activity tracking, the busy check and the `spec.idle` API stay the same either way; only who pulls the trigger changes. Polling from `ax-server` works on every Substrate version AX supports today, which is why it comes first.
