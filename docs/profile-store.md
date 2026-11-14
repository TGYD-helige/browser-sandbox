# Controller Profile store

`createProfileStore` provides a volume-backed `StorageAdapter` for a single controller process. It does not change local runtime persistence or the worker archive format. Managed registration selects a host-attached adapter first, otherwise this configured volume store; a Profile with neither fails with `browser_profile_storage_unavailable`. Local mode retains its existing behavior.

```ts
import { createProfileStore } from "@amaster.ai/browser-sandbox";

const store = await createProfileStore({
  root: "/var/lib/browser-sandbox/profiles",
  baseUrl: "http://controller.internal:8083",
  maxProfiles: 10000,
  maxBytes: 10 * 1024 ** 3,
  maxStagedBytes: 1024 ** 3,
  maxTransfers: 4,
  maxGrants: 1024,
  // idleTtlMs: 30 * 24 * 60 * 60 * 1000,
});
```

Mount one private volume into the controller only. The directory must belong to the controller UID and have mode `0700`. Only one controller process may write it; use Kubernetes `Recreate` or a single StatefulSet replica and retain the PVC on uninstall. The store does not provide multi-controller fencing or authorize profile identifiers.

The host supplies a reusable `profileId` for a login identity. Call `store.open(profileId)` after host authorization and controller lease admission. It returns the unchanged `load`/`save`/`commit` adapter interface plus asynchronous `close()`. A second open of the same Profile returns `browser_profile_busy`. Close the adapter after confirmed worker finalization; closing revokes its transfer grants, cancels its active transfers, and removes uncommitted or superseded objects. The controller holds a cross-worker lease before allocating any worker. On restart it reads exact worker incarnations and profile identifiers from backend metadata; failed discovery or unknown metadata rejects persistent admission. Existing stopped workers also retain occupancy until confirmed cleanup. This recovers Profile exclusivity; it does not by itself recover live browser bindings.

Route `store.handle(request, response)` on a private worker-facing HTTP listener at `baseUrl`. It returns `false` for unrelated paths and handles `/profile-store/<token>` itself. Do not expose this listener to browser pages or agents; the private network and worker runtime privilege separation must prevent their access. Download and upload grants are method scoped, expire after five minutes, and are revoked on adapter close or controller restart. Transfers have a two-minute deadline, stream with backpressure, reject concurrency beyond `maxTransfers`, and enforce the worker's 256 MiB archive limit. Host remote storage adapters issue their own URLs; their archive bytes do not pass through this store.

An upload never publishes a Profile. `commit` verifies the uploaded digest and length, checks `previousSha256` against the confirmed archive, and publishes metadata with a temporary file, file synchronization, atomic rename, and directory synchronization. Repeated confirmation of the same digest is idempotent. A conflicting predecessor returns `browser_profile_commit_conflict`; exceeded count, confirmed-size, staging-size, or grant limits return `browser_profile_capacity_exceeded`. Confirmed archives are bounded by `maxBytes`; confirmed plus staged bytes are bounded by `maxBytes + maxStagedBytes`, excluding bounded metadata. Reserve volume headroom for metadata and filesystem overhead.

Each archive has its own random encryption key in controller-owned metadata. Anyone with volume access can decrypt the archives. Back up the entire store, including `index.json`; copying only the archive files loses their keys. Missing referenced objects, unsupported metadata versions, and malformed metadata reject startup without replacing the index. A restart keeps every confirmed object and discards unreferenced staging whose keys were never committed. A failed metadata durability barrier stops further operations until restart. An upload or unconfirmed commit is not evidence that the worker's recovery copy may be deleted.

`store.delete(profileId, signal?)` deletes a confirmed Profile only while no adapter is open. `store.expire(signal?, isLeased?)` removes idle Profiles when `idleTtlMs` is configured, using the last confirmed commit time and rechecking ownership before deletion. The controller supplies its external lease check so recovered workers also block expiry. Invoke expiry from the controller's maintenance loop after recovering worker leases; it has no independent timer. Execution finish and `store.close()` retain confirmed archives. `store.close()` aborts and drains transfers and waits for pending metadata work before the volume can be released.

## Controller configuration and host operations

For the image's controller command, configure `BROWSER_PROFILE_STORE_ROOT` to enable the volume store and `BROWSER_PROFILE_STORE_ORIGIN` to its private worker-reachable origin on TCP 8083. Limits are `BROWSER_PROFILE_MAX_COUNT`, `BROWSER_PROFILE_MAX_BYTES`, `BROWSER_PROFILE_MAX_STAGED_BYTES` and `BROWSER_PROFILE_MAX_TRANSFERS`; `BROWSER_PROFILE_IDLE_TTL_MS` enables optional idle expiry. The controller checks expiry once per minute after recovering Profile occupancy. `maxGrants` also bounds concurrently open store sessions and queued metadata work. See the [Docker](./managed-docker.md) and [Kubernetes](./kubernetes.md) deployment instructions for volume ownership and networking.

Hosts continue to use `createBrowserSessions({ provider })` with `profileId` and, optionally, `storage`. An optional `runtime.attachStorage(registration, adapter, signal)` signal bounds setup readiness only; close the returned attachment to revoke an established channel. A host adapter takes precedence for its registration and a dropped adapter fails required saves rather than falling back to another store. A save or worker cleanup failure preserves its binding and Profile lease for an explicit finalization retry. Once workers are finalized, a trusted host can call `runtime.request({ op: "profile_delete", profileId })` to delete a volume-backed Profile. This operation needs no execution identity; deletion of host-owned archives remains the host adapter's responsibility.

For a live managed execution, `runtime.request({ op: "worker_metrics", executionId })` returns the existing preparation timings, including `profileRestoreMs` when persistence was used. Finalization returns `profileFinalizeMs`, covering worker shutdown, archive packing/upload and confirmation. Measure controller-side finalization separately because it also includes infrastructure removal. The managed CI reports these timings alongside actual archive byte sizes and median/P95 for three samples of each controlled Cookie workload; these are controlled fixtures, not real-site performance claims.

## Storage networking and upgrades

Private host storage endpoints require exact TCP destinations. Set `BROWSER_STORAGE_EGRESS` to a JSON array such as `[{"address":"10.42.1.2","port":9083}]` for Docker, or set `networkPolicy.storageEgress` to the same array in Helm values. The worker allows only these destination IPs and ports for its trusted runtime UID; Chrome and execution processes remain denied. The built-in store destination is derived from `BROWSER_PROFILE_STORE_ORIGIN` and resolved at worker startup. Keep its address stable for the worker lifetime. Public host storage continues to use the existing public egress policy.

Before upgrading workers created without Profile metadata, finalize them through the old controller and confirm their removal. Missing metadata is unknown occupancy and prevents persistent admission; the new controller does not infer that these workers are temporary. After restart, recovered occupancy is retained while a resource exists, including stopped containers and Pending Pods, and released only after successful backend discovery confirms absence. Discovery failures continue to fence reuse, deletion and expiry.

Older store metadata remains compatible. Previously recorded load timestamps are retained conservatively until the next confirmed commit; new loads do not refresh idle TTL or rewrite the index. Back up the complete store before upgrading.
