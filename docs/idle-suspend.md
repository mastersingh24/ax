# Automatic suspension

A running task holds a whole Agent Substrate worker, whether it is doing anything or not. Without help, AX only suspends a task right after creating it and when someone calls `ax suspend`, so a request-driven agent that answered its last request an hour ago, or a batch task whose command finished, keeps its worker until a person notices. On a small worker pool that is how tasks end up `Failed` for lack of a place to run.

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

Only a `200` whose JSON body has `"busy": true` means busy. Any other answer, including `{"busy": false}`, another status code, an unreadable body, a refused connection, or no answer within two seconds, means not busy. The reason is reported, but the task can still be suspended: an agent whose server has died should not hold a worker forever. Keep the handler cheap; it is called on every check of an otherwise idle task.

## The runner's status endpoint

The runner reports what it knows at `GET /metadata/v1alpha1/ax/status`:

```json
{"idleSeconds": 412, "inFlight": 0, "busy": false, "exited": false, "exitCode": 0}
```

`idleSeconds` is zero while a request is open. `busyError` appears when the busy check failed. `exited` and `exitCode` describe `spec.command`; the exit code is `-1` when the command was killed by a signal.

A suspended task resumes from a snapshot, and the runner's memory in that snapshot remembers the last request before the suspend, possibly hours ago. The runner notices that it was frozen (its once-a-second clock tick jumps) and restarts the idle clock. The control plane also never treats a task as idle for longer than it has seen it running, so a task that was just resumed is never suspended straight away.

## How the control plane applies it

`ax-server` checks every task that sets `spec.idle` or `spec.onCompletion: Suspend` and is `Running` or `Suspended`, every `--idle-check-interval` (30 seconds by default; `0` turns it off). For each one, under the task's lock:

1. It asks the Substrate control API for the actor's state. That call never wakes an actor.
2. If the actor is suspended, the runner is not contacted. Asking a suspended task anything through the router would resume it, so this is the rule that keeps suspended tasks suspended. If AX had the task as `Running`, it is recorded as `Suspended` with reason `ActorSuspended`.
3. If the actor is running but AX had the task as `Suspended`, the router resumed it for a request. The task is recorded as `Running` with reason `ResumedByRequest`, so `ax get` matches reality.
4. For a running actor it reads the status endpoint, directly from the worker's address, or through the router with `ate-target-actor` if that address can't be reached. The actor was running a moment ago, so the router has nothing to wake.
5. It suspends the actor when the command has exited and `onCompletion` is `Suspend` (reason `CompletedSuspended`, with the exit code in the message), or when the task has been idle for `suspendAfter`, has no open requests, and isn't busy (reason `IdleSuspended`).

The task's `Ready` condition carries the reason. `ax resume`, or simply the next request through the router, brings it back.

On resume Agent Substrate restores the runner from its golden snapshot, so the command starts again. For `onCompletion: Suspend` that means a request that wakes a finished task runs the command once more and the task is suspended again when it exits.

A runner image built before this change has no status endpoint. The control plane then makes no decision for that task, so upgrading `ax-server` first is safe.

## Relation to Agent Substrate's worker-initiated suspend

Agent Substrate v0.3.0 has the plumbing for a sandbox to ask for its own suspension (`RequestActorSuspend`, from the guest through atelet to the control plane), with no caller yet. Once the runner can reach it, it could make the same decision locally and drop the polling, with the control plane recording the result. The activity tracking, the busy check and the `spec.idle` API stay the same either way; only who pulls the trigger changes. Polling from `ax-server` works on every Substrate version AX supports today, which is why it comes first.
