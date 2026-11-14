# Controller recovery and capacity

Managed mode supports one controller per installation. Keep its private `/controller-data` volume across container replacement and keep `BROWSER_INSTALLATION` stable. The Helm chart creates a retained ReadWriteOnce PVC by default, uses `Recreate`, and mounts the volume only into the controller. `controllerData.existingClaim` selects an existing volume. The execution journal lives under `/controller-data/executions`. The optional Profile store retains its separate volume at `/var/lib/browser-sandbox/profiles`, configured through `profileStore.existingClaim`. Journal-volume access includes execution capabilities; Profile-volume access includes archive encryption metadata.

Do not run multiple controllers against the same installation or journal. Shared storage does not provide controller HA or fencing between replicas. Managed endpoints remain internal-only while authentication is deferred.

## Configuration

| Controller environment | Helm value | Default | Meaning |
|---|---|---|---|
| `BROWSER_CONTROLLER_STATE` | Fixed mount path | `/controller-data/executions` | Private durable execution records |
| `BROWSER_MAX_WORKERS` | `capacity.maxWorkers` | `8` | Maximum live or incompletely cleaned worker allocations |
| `BROWSER_MAX_PENDING` | `capacity.maxPending` | `2` | Maximum concurrent worker creations |
| `BROWSER_MAX_QUEUED` | `capacity.maxQueued` | `32` | Maximum queued execution creations |
| `BROWSER_WORKER_CREATION_TIMEOUT` | `capacity.creationTimeoutMs` | `90000` | End-to-end creation budget, including queue, scheduling, image pull and readiness |
| `BROWSER_MAX_EXECUTION_MS` | `capacity.maxExecutionMs` | `3600000` | Maximum registration lifetime before normal finalization |
| `BROWSER_IDLE_REGISTRATION_MS` | `capacity.idleRegistrationMs` | `900000` | Inactivity before an unused registration or idle worker becomes eligible for finalization |

The trusted host operation `controller_status` reports capacity, configured limits, and an optional execution's lifecycle phase, last idle protection/error and reclamation reason. It contains no worker addresses or access grants.

Creation is shared by clients waiting for the same execution. Disconnecting the last waiter cancels queued or incomplete creation. A cancelled partial allocation continues to consume capacity until owned cleanup or a complete discovery confirms its outcome. Queue overflow returns `browser_worker_queue_full`; status and cleanup errors never create a replacement worker or replay browser commands.

## Reclamation and failure recovery

The controller reads the worker's session, connected-client count, takeover state and pending sign-in count. Clients, unexpired retention, human/uncertain takeover and pending sign-in keep a worker alive. Failed or unsupported status is unknown state and does not authorize idle deletion. Before finalization, the worker rechecks idle eligibility and the expected browser generation atomically. A renewal between the controller's preflight and this check prevents reclamation. Maximum execution lifetime uses normal finalization even when the execution remains retained; it never bypasses required save confirmation.

Normal controller shutdown preserves published workers and closes proxy connections. Clients must reconnect; requests and website writes are never automatically replayed. Startup discovers only the installation's exact worker incarnations, recovers bindings, grants and Profile leases from durable records, and reads retention from the existing worker. Failed discovery blocks admission. Worker or generation loss rejects renewal explicitly and never redirects the old endpoint into a newly created browser.

Finalization revokes access, confirms browser shutdown and any required Profile save, records that confirmation durably, and only then deletes infrastructure. Failed deletion remains pending and can be retried without repeating a confirmed save. Unconfirmed save or cleanup retains ownership and recoverable data. A host using remote storage must reconnect its storage adapter to the same execution after controller replacement before a save can complete. A Profile lease is released only after confirmed finalization and cleanup. Compatible owned orphans use the same finalization path; incompatible or unreadable workers remain available for operator investigation.

Recovery performs at most three automatic attempts. Confirmed worker absence completes the execution and releases its lease; an unreachable temporary worker can be removed after those attempts because it holds no persistent Profile. An existing worker with an unconfirmed persistent save enters `recovery-blocked`, retains its capacity allocation and Profile lease, and emits a `recovery-blocked` worker event. `controller_status` and capacity report the blocked count; execution status includes the attempt count and sanitized error. Automatic reconciliation stops retrying that state. The trusted host can reconnect its storage adapter and explicitly finish the execution through the normal save path; operators must preserve its data when investigating backend resources. Protocol incompatibility also reports a blocked state while preserving the original durable binding for a compatible-controller rollback.

The journal stores finalization confirmations without Profile encryption keys or archive transfer grants.

The journal retains recent finished identities to reject delayed execution-ID reuse across restart. Its current 65,536-record budget includes active registrations and completed fences; when new registrations need space, it durably removes the oldest confirmed finished fences, including after restart. Active or unknown records are never evicted to admit new work, and an active-only capacity limit rejects new registrations explicitly. Hosts must generate fresh execution IDs: replay protection is bounded by the retained history, not permanent. Retain and back up this directory. Replacing or deleting it removes replay history; do so only after draining the installation and ensuring old execution IDs and endpoints cannot be reused. Corrupt or foreign-installation records block recovery and remain intact. Abandoned `.pending-*` files from interrupted writes are removed on single-controller startup without replacing confirmed records.

## Deadlines when the controller is unavailable

Kubernetes workers are bare Pods with `restartPolicy: Never` and an `activeDeadlineSeconds` backstop covering creation, the longer of the browser hard timeout and configured maximum execution lifetime, the 180-second finalization/Profile save budget, and graceful termination. A rollout keeps the release-owned worker ownerReference intact. A Pod that is killed or evicted loses its live pages; restoration from a confirmed Profile archive requires an explicit new execution. This hard deadline still terminates a Pod when the controller is unavailable or its Profile save is unconfirmed, so unsaved Profile data can be lost at that point. Retaining an unconfirmed Profile in the controller cannot override the backend hard deadline.

Docker has no equivalent independently enforced maximum container lifetime in this implementation. Browserless still enforces the browser generation's `TIMEOUT`, but the worker container and its data can remain while the controller is unavailable. Restore the controller and let it reconcile, or perform an explicit owned drain that preserves required Profile data. Do not treat controller timers as a Docker daemon lifetime guarantee.

## Worker upgrades

The controller-to-worker protocol is `browser-sandbox.worker/v1`, independently of image version. A new controller can serve existing workers that report this protocol and retain the recorded generation. An incompatible worker is rejected with `browser_worker_protocol_unsupported` and retained for an explicit compatible-controller drain; it is not deleted or replaced as part of an upgrade. Drain persistent workers through their normal save path before an incompatible protocol change. Do not roll back a controller to a version that cannot read the journal's `browser-sandbox.execution/v1` format.

Enable the controller volume Profile store with `profileStore.enabled=true`; its separate ClusterIP service exposes only port 8083, and NetworkPolicy permits that port only from worker Pods. Agent access still uses the controller access port. See [Profile storage](profile-store.md) and [Kubernetes deployment](kubernetes.md) for storage and platform configuration.
