# Agent Substrate v0.4

This branch of AX builds against `github.com/agent-substrate/substrate` v0.4.0 and needs a v0.4 control plane. The ActorTemplates it creates leave out fields that v0.3 requires, and v0.4 rejects templates built for v0.3, so `ax-server` and Substrate have to be upgraded together.

## What changed for AX

| Substrate v0.4 change | What AX does now |
|---|---|
| `SnapshotConfig.on_pause` removed ([substrate#2309](https://github.com/agent-substrate/substrate/pull/2309)). `on_commit` now sets the scope of every snapshot, both the node-local checkpoint a pause takes and the snapshot a suspend uploads. | Templates set only `onCommit` and `storageLocation`. |
| `SnapshotConfig.on_resume`, `OnResumeConfig` and `RESUME_SOURCE_GOLDEN` removed. A template that sets `onResume` is rejected ([substrate#2105](https://github.com/agent-substrate/substrate/pull/2105)). A DATA snapshot now always resumes by starting the containers from the image with the durable directories restored. | AX used DATA snapshots restored on top of the golden snapshot. That mode no longer exists, so `ax-server` takes `--snapshot-scope=full\|data` (env `AX_SNAPSHOT_SCOPE`), default `full`. See [Automatic suspension](idle-suspend.md#what-a-woken-task-looks-like) for the trade-off. |
| `TrustBundleDataSource.name` replaced by `names` (a list, unified into one file), and a new built-in bundle `system-roots.ate.dev` with public CA roots ([substrate#2048](https://github.com/agent-substrate/substrate/pull/2048), [#2049](https://github.com/agent-substrate/substrate/pull/2049)). | With `AX_EGRESS_MITM_TRUST_BUNDLE=true`, `/run/ate/trust-bundle.pem` holds both `egress-mitm.ate.dev` and `system-roots.ate.dev`. |
| Substrate's trust store guidance no longer sets `SSL_CERT_DIR`, since it replaces the image's trust store instead of adding to it ([substrate#2044](https://github.com/agent-substrate/substrate/pull/2044)). | AX stops setting `SSL_CERT_DIR`. `SSL_CERT_FILE`, `REQUESTS_CA_BUNDLE`, `CURL_CA_BUNDLE`, `GIT_SSL_CAINFO` and `NODE_EXTRA_CA_CERTS` still point at the bundle, and a task's own env still wins. |
| `WorkerAssignment.worker_pod_ip` replaced by `worker_pod_ips`, at most one per IP family, primary family first ([substrate#2052](https://github.com/agent-substrate/substrate/pull/2052)). | AX uses the first non-empty address, records no address when there is none, and puts brackets around IPv6 addresses in the URLs it builds. |

## Upgrading a cluster with existing tasks

- Templates stored by an older `ax-server` keep `onCommit: DATA`. v0.4 ignores their `onPause` and `onResume`, so those tasks now restart from their image with `/workspace` restored when they wake, instead of restoring from the golden snapshot. Recreate a task to get the new default scope.
- The scope is fixed when a task's template is created. Changing `--snapshot-scope` only affects tasks created afterwards.
- Substrate's own notes say to upgrade ateapi and atelet together (atelet rejects the removed restore scope).

## Other v0.4 changes that matter to AX

- **Single, intercepting egress gateway.** The plain egress gateway and `--experimental-use-sdsmint` are gone ([substrate#2015](https://github.com/agent-substrate/substrate/pull/2015)). All HTTPS egress goes through the intercepting gateway, so tasks that make HTTPS calls need its CA. Set `AX_EGRESS_MITM_TRUST_BUNDLE=true` on `ax-server` for v0.4 clusters.
- **Credential injection is no longer experimental** ([substrate#2101](https://github.com/agent-substrate/substrate/pull/2101)). The API fields AX uses (`HttpRuleEffects.replace_headers`, `CredentialHeader.header`, `prefix`, `credential_uri`) did not change. The install did: Substrate has to be deployed with `--credential-provider '{"name":"k8s.io"}'` (or another provider) for `spec.egress[].credentials` to work. Without a provider the gateway fails closed with HTTP 500 for a rule that needs a credential. `CredentialHeader.actor_jwt`, which is new, is not exposed by AX.
- **`tls_passthrough` rules are enforced now** ([substrate#2019](https://github.com/agent-substrate/substrate/pull/2019)); v0.3 ignored them. AX still turns every `spec.egress` rule into an `https` (intercepted) rule, because that is the only kind credentials can be injected into.
- **Authorization (opt-in).** With `--experimental-enable-authz`, ateapi checks Actor and ActorTemplate calls against access policies ([substrate#1980](https://github.com/agent-substrate/substrate/pull/1980)). `ax-server`'s identity then needs, on every atespace it serves: create, get, list and delete for ActorTemplates and Actors, and `can_use_template` on the per-task templates and the default template (`ax-system/default-template`). Without the flag nothing changes.
- **Error codes.** ateapi now builds its errors with an internal `apierror` package ([substrate#2072](https://github.com/agent-substrate/substrate/pull/2072), [#2129](https://github.com/agent-substrate/substrate/pull/2129)). The gRPC codes AX checks (`AlreadyExists` on create, `NotFound` on get and delete) are unchanged.
- **`MintActorJWT` no longer takes the actor UID** ([substrate#2207](https://github.com/agent-substrate/substrate/pull/2207)). AX doesn't call it.
