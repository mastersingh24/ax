# Agent Substrate v0.5

This branch of AX builds against `github.com/agent-substrate/substrate` v0.5.0 and needs a v0.5 control plane. v0.5 renumbered fields in the ActorTemplate's `SnapshotConfig` and turned `SandboxConfig.sandbox_class` from an enum into a string, so templates built by an older `ax-server` don't create on v0.5 and templates built by this one don't create on v0.4. Upgrade `ax-server` and Substrate together. Substrate itself doesn't upgrade in place between minor versions: it's a fresh install, and suspended tasks don't carry over.

## What changed for AX in v0.5

| Substrate v0.5 change | What AX does now |
|---|---|
| `ContainerWakeupProbe` renamed `WakeupProbe` ([substrate#2575](https://github.com/agent-substrate/substrate/pull/2575)); acronyms in message names are TitleCase (`HttpGetAction`, `HttpsRule`; [substrate#2520](https://github.com/agent-substrate/substrate/pull/2520)). | Renamed in the client. The wire format of these messages didn't change. |
| `SandboxConfig.sandbox_class` is a string (`"gvisor"`, `"microvm"`) instead of an enum ([substrate#2586](https://github.com/agent-substrate/substrate/pull/2586)). | Templates set `sandboxClass: gvisor`; `configName` stays `gvisor-default`. |
| The snapshot fidelity ladder is `VOLUMES` < `ROOTFS` < `MEMORY`. `ROOTFS` (volumes plus root filesystem writes, cold boot) needs the micro-VM sandbox class; a gVisor template that asks for it is rejected ([substrate#2448](https://github.com/agent-substrate/substrate/pull/2448)). | `--snapshot-scope=full` maps to `MEMORY` and `data` to `VOLUMES`, as on v0.4. AX templates are gVisor, so there is no `rootfs` scope. |
| Templates can turn off their golden snapshot (`goldenSnapshotConfig.mode: DISABLED`, [substrate#2328](https://github.com/agent-substrate/substrate/pull/2328)). | Per-task templates disable it. An actor's boot source is fixed when the actor is created, and AX creates the task's actor right after its template, before a golden snapshot could exist, so the golden actor only took a worker, ran the task's command once more, and uploaded a snapshot nothing restored. |
| New actor state `REVERTING`, held while a `RevertActor` runs ([Actor Lifecycle v2](https://github.com/agent-substrate/substrate/pull/2449)). A revert that is interrupted leaves the actor there. | When a task is reconciled and its actor is `REVERTING`, AX calls `RevertActor` again, which picks up where it stopped. Unlike a crashed actor, a reverting one is never deleted and recreated if that fails; the reconcile returns the error and is retried. The idle check leaves a reverting actor alone. |
| `PAUSED` and `PAUSING` removed; a paused actor is `SUSPENDED` with a node-local snapshot ([substrate#2620](https://github.com/agent-substrate/substrate/pull/2620)). `ResumeActor` accepts only `SUSPENDED` actors. | Nothing: AX never pauses actors, and it resumes only suspended ones. |
| List requests take their page size and token in `options` ([substrate#2593](https://github.com/agent-substrate/substrate/pull/2593)). | Nothing: AX lists templates without paging. |
| The `ate-system` namespace denies ingress by default; a client namespace needs the label `ate.dev/api-access=true` to reach the API ([substrate#2522](https://github.com/agent-substrate/substrate/pull/2522)). | `deploy/ax-server.yaml` and `deploy/redis.yaml` label `ax-system`. Without the label, calls from `ax-server` time out rather than fail. |

### Upgrading from v0.4

- Install v0.5 fresh, then deploy this `ax-server` with the same `AX_SNAPSHOT_SCOPE`. Recreate tasks: their actors and templates don't survive the reinstall.
- A task's runner image needs nothing new from v0.5. Runners built from this branch serve the task and workspace metadata on loopback (`127.0.0.1:8099`, passed in `AX_METADATA_URL`), following upstream AX; `/metadata/v1alpha1/ax/status` stays on port 80 because the control plane reads it from outside the task. A command that builds its metadata URL from `AX_METADATA_URL` keeps working; one that hard-codes port 80 for `/metadata/v1alpha1/ax/task` or `/workspaces` doesn't.
- `MEMORY` is a best-effort ceiling: under host pressure Substrate may package a lower fidelity, and the task then wakes as with `data`. Snapshots record the fidelity they actually hold (`status.snapshots[].fidelity`).

### Other v0.5 changes that matter to AX

- **DNS inside actors (opt-in).** With `atecontroller --dns-egress-policy`, an actor resolves only the hosts its egress policy allows ([substrate#2524](https://github.com/agent-substrate/substrate/pull/2524)). AX creates an egress policy only for tasks with `spec.egress`, so with that flag a task without one can't resolve any name.
- **Router ingress auth.** The default `--ingress-auth-mode=deprecated-insecure` keeps the plaintext port 80 that `ax-server` uses for readiness and runner status. Switching the router to `static-mtls` would break that path.
- **Durable directories count against the actor's memory limit**, which matters for tasks with `spec.resources` limits and a large `/workspace`.

## Agent Substrate v0.4

| Substrate v0.4 change | What AX does now |
|---|---|
| `SnapshotConfig.on_pause` removed ([substrate#2309](https://github.com/agent-substrate/substrate/pull/2309)), and the content scopes replaced by a fidelity ladder ([substrate#2364](https://github.com/agent-substrate/substrate/pull/2364)). `preferred_fidelity` now sets the ceiling for every snapshot, both the node-local checkpoint a pause takes and the snapshot a suspend uploads. | Templates set only `preferredFidelity` and `storageLocation`. |
| `SnapshotConfig.on_resume`, `OnResumeConfig` and `RESUME_SOURCE_GOLDEN` removed. A template that sets `onResume` is rejected ([substrate#2105](https://github.com/agent-substrate/substrate/pull/2105)). A data-only (VOLUMES) snapshot now always resumes by starting the containers from the image with the durable directories restored. | AX used DATA snapshots restored on top of the golden snapshot. That mode no longer exists, so `ax-server` takes `--snapshot-scope=full\|data` (env `AX_SNAPSHOT_SCOPE`), default `full`. See [Automatic suspension](idle-suspend.md#what-a-woken-task-looks-like) for the trade-off. |
| `TrustBundleDataSource.name` replaced by `names` (a list, unified into one file), and a new built-in bundle `system-roots.ate.dev` with public CA roots ([substrate#2048](https://github.com/agent-substrate/substrate/pull/2048), [#2049](https://github.com/agent-substrate/substrate/pull/2049)). | With `AX_EGRESS_MITM_TRUST_BUNDLE=true`, `/run/ate/trust-bundle.pem` holds both `egress-mitm.ate.dev` and `system-roots.ate.dev`. |
| Substrate's trust store guidance no longer sets `SSL_CERT_DIR`, since it replaces the image's trust store instead of adding to it ([substrate#2044](https://github.com/agent-substrate/substrate/pull/2044)). | AX stops setting `SSL_CERT_DIR`. `SSL_CERT_FILE`, `REQUESTS_CA_BUNDLE`, `CURL_CA_BUNDLE`, `GIT_SSL_CAINFO` and `NODE_EXTRA_CA_CERTS` still point at the bundle, and a task's own env still wins. |
| `WorkerAssignment.worker_pod_ip` replaced by `worker_pod_ips`, at most one per IP family, primary family first ([substrate#2052](https://github.com/agent-substrate/substrate/pull/2052)). | AX uses the first non-empty address, records no address when there is none, and puts brackets around IPv6 addresses in the URLs it builds. |

### Upgrading from v0.3

- Templates stored by a v0.3 `ax-server` kept `onCommit: DATA`. v0.4 ignored their `onPause` and `onResume`, so those tasks restarted from their image with `/workspace` restored when they woke, instead of restoring from the golden snapshot.
- The scope is fixed when a task's template is created. Changing `--snapshot-scope` only affects tasks created afterwards.
- Substrate's own notes say to upgrade ateapi and atelet together (atelet rejects the removed restore scope).

### Other v0.4 changes that matter to AX

- **Single, intercepting egress gateway.** The plain egress gateway and `--experimental-use-sdsmint` are gone ([substrate#2015](https://github.com/agent-substrate/substrate/pull/2015)). All HTTPS egress goes through the intercepting gateway, so tasks that make HTTPS calls need its CA. Set `AX_EGRESS_MITM_TRUST_BUNDLE=true` on `ax-server` for v0.4 clusters.
- **Credential injection is no longer experimental** ([substrate#2101](https://github.com/agent-substrate/substrate/pull/2101)). The API fields AX uses (`HttpRuleEffects.replace_headers`, `CredentialHeader.header`, `prefix`, `credential_uri`) did not change. The install did: Substrate has to be deployed with `--credential-provider '{"name":"k8s.io"}'` (or another provider) for `spec.egress[].credentials` to work. Without a provider the gateway fails closed with HTTP 500 for a rule that needs a credential. `CredentialHeader.actor_jwt`, which is new, is not exposed by AX.
- **`tls_passthrough` rules are enforced now** ([substrate#2019](https://github.com/agent-substrate/substrate/pull/2019)); v0.3 ignored them. AX still turns every `spec.egress` rule into an `https` (intercepted) rule, because that is the only kind credentials can be injected into.
- **Authorization (opt-in).** With `--experimental-enable-authz`, ateapi checks Actor and ActorTemplate calls against access policies ([substrate#1980](https://github.com/agent-substrate/substrate/pull/1980)). `ax-server`'s identity then needs, on every atespace it serves: create, get, list and delete for ActorTemplates and Actors, and `can_use_template` on the per-task templates and the default template (`ax-system/default-template`). Without the flag nothing changes.
- **Error codes.** ateapi now builds its errors with an internal `apierror` package ([substrate#2072](https://github.com/agent-substrate/substrate/pull/2072), [#2129](https://github.com/agent-substrate/substrate/pull/2129)). The gRPC codes AX checks (`AlreadyExists` on create, `NotFound` on get and delete) are unchanged.
- **`MintActorJWT` no longer takes the actor UID** ([substrate#2207](https://github.com/agent-substrate/substrate/pull/2207)). AX doesn't call it.
