import { createHash } from "node:crypto";
import { performance } from "node:perf_hooks";
import { setTimeout as delay } from "node:timers/promises";
import { type ExecutionRecord, isExecutionRecord } from "./execution-journal.js";
import {
  createKubernetesClient,
  executionLeaseSelector,
  hasKubernetesOwner,
  type KubernetesAccess,
  KubernetesError,
  kubernetesOwnerReference,
  listKubernetesPages,
  readKubernetesOwner,
} from "./kubernetes-api.js";
import { validProfileId } from "./local-profiles.js";
import type { ExecutionOwner } from "./managed-backend.js";
import { createSerialWork } from "./serial-work.js";

const prefix = "browser-sandbox.io/";
// Kubernetes MicroTime requires six fractional digits.
const renewTime = () => new Date().toISOString().replace("Z", "000Z");
const validId = (id: string) =>
  /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(id);
const absent = (error: unknown) =>
  error instanceof KubernetesError && error.status === 404;
const conflict = (error: unknown) =>
  error instanceof KubernetesError && error.status === 409;

const validPeerOrigin = (value: string) => {
  try {
    const url = new URL(value);
    return url.protocol === "http:" && url.origin === value;
  } catch {
    return false;
  }
};

/** Kubernetes conditional writes own arbitration; cached observations only determine expiry. */
export async function createKubernetesOwnership(
  options: KubernetesAccess & {
    installation: string;
    ownerName: string;
    replica: string;
    peerOrigin: string;
    leaseMs?: number;
    maxFinished?: number;
    now?: () => number;
  },
) {
  const leaseMs = options.leaseMs ?? 15000;
  const maxFinished = options.maxFinished ?? 65536;
  if (!Number.isSafeInteger(maxFinished) || maxFinished < 1 || maxFinished > 65536)
    throw Error("browser_controller_configuration_invalid");
  const now = options.now ?? (() => performance.now());
  if (
    !options.installation ||
    !options.replica ||
    !Number.isSafeInteger(leaseMs) ||
    leaseMs < 3000 ||
    leaseMs > 60000
  )
    throw Error("browser_controller_configuration_invalid");
  if (!validPeerOrigin(options.peerOrigin))
    throw Error("browser_controller_configuration_invalid");
  const api = createKubernetesClient(options);
  const parent = await readKubernetesOwner(api, options, AbortSignal.timeout(15000));
  const leases = `/apis/coordination.k8s.io/v1/namespaces/${options.namespace}/leases`;
  const secrets = `/api/v1/namespaces/${options.namespace}/secrets`;
  const salt = createHash("sha256")
    .update(options.installation)
    .digest("hex")
    .slice(0, 12);
  const name = (id: string) => `bs-${salt}-e-${id}`;
  const profileName = (id: string) =>
    `bs-${salt}-p-${createHash("sha256").update(options.installation).update("\0").update(id).digest("hex").slice(0, 32)}`;
  const selector = executionLeaseSelector(options.installation);
  const markerName = `bs-${salt}-journal-import`;
  const markerProtocol = Buffer.from("browser-sandbox.journal-import/v1").toString(
    "base64",
  );
  const labels = {
    [prefix + "installation"]: options.installation,
    [prefix + "state"]: "execution",
  };
  const observed = new Map<
    string,
    { lease: any; changedAt: number; hashes: Set<string> }
  >();
  const owned = new Map<
    string,
    { epoch: number; uid: string; deadline: number; ready: boolean }
  >();
  const legacyCapabilities = new Map<string, string>();
  let listing: Set<string> | undefined;
  const ownerOf = (lease: any): ExecutionOwner => ({
    replica: lease.spec.holderIdentity,
    origin: lease.metadata.annotations[prefix + "peer"],
    epoch: Number(lease.metadata.annotations[prefix + "epoch"]),
    uid: lease.metadata.uid,
  });
  const clearLegacy = (id: string) => {
    for (const hash of observed.get(id)?.hashes ?? []) legacyCapabilities.delete(hash);
  };
  const life = new AbortController();
  let watching: Promise<void>;
  let leaseVersion = "";
  let lost: (id: string) => void = () => {};
  const signal = () => AbortSignal.any([life.signal, AbortSignal.timeout(15000)]);
  const verify = (object: any, expectedName: string, state = "execution") => {
    const expected =
      object?.kind === "Secret" && state === "execution"
        ? kubernetesOwnerReference(
            "Lease",
            expectedName,
            object.metadata?.ownerReferences?.[0]?.uid,
          )
        : parent;
    if (
      object?.metadata?.name !== expectedName ||
      object.metadata.labels?.[prefix + "installation"] !== options.installation ||
      object.metadata.labels?.[prefix + "state"] !== state ||
      typeof object.metadata.resourceVersion !== "string" ||
      typeof object.metadata.uid !== "string" ||
      !object.metadata.uid ||
      !hasKubernetesOwner(object, expected)
    )
      throw Error("browser_controller_state_corrupt");
  };
  const forget = (id: string) => {
    if (owned.delete(id)) lost(id);
  };
  const evict = (id: string, uid?: string) => {
    if (uid && observed.has(id) && observed.get(id).lease.metadata.uid !== uid) return;
    listing?.add(id);
    clearLegacy(id);
    observed.delete(id);
    forget(id);
  };
  const expiry = setInterval(() => {
    for (const [id, local] of owned) if (now() >= local.deadline) forget(id);
  }, 100);
  expiry.unref();
  const observe = (lease: any) => {
    const id = lease.metadata?.labels?.[prefix + "execution"];
    if (!validId(id)) throw Error("browser_controller_state_corrupt");
    verify(lease, name(id));
    const epoch = Number(lease.metadata.annotations?.[prefix + "epoch"]);
    if (!Number.isSafeInteger(epoch) || epoch < 1)
      throw Error("browser_controller_state_corrupt");
    const previous = observed.get(id);
    if (
      !validPeerOrigin(lease.metadata.annotations?.[prefix + "peer"]) ||
      !Number.isSafeInteger(lease.spec?.leaseDurationSeconds) ||
      lease.spec.leaseDurationSeconds < 1
    )
      throw Error("browser_controller_state_corrupt");
    if (
      previous?.lease.metadata.uid === lease.metadata.uid &&
      epoch < ownerOf(previous.lease).epoch
    )
      return ownerOf(previous.lease);
    if (previous?.lease.metadata.resourceVersion === lease.metadata.resourceVersion)
      return ownerOf(previous.lease);
    const legacy = lease.metadata.annotations[prefix + "legacy-capabilities"];
    let hashes: unknown = [];
    try {
      if (legacy !== undefined) hashes = JSON.parse(legacy);
    } catch {
      throw Error("browser_controller_state_corrupt");
    }
    if (
      !Array.isArray(hashes) ||
      hashes.length > 2000 ||
      hashes.some(
        (hash) =>
          typeof hash !== "string" ||
          !/^[a-f0-9]{64}$/.test(hash) ||
          (legacyCapabilities.has(hash) && legacyCapabilities.get(hash) !== id),
      )
    )
      throw Error("browser_controller_state_corrupt");
    const next = new Set<string>(hashes);
    if (legacyCapabilities.size - (previous?.hashes.size ?? 0) + next.size > 200000)
      throw Error("browser_execution_capacity_exceeded");
    clearLegacy(id);
    for (const hash of next) legacyCapabilities.set(hash, id);
    listing?.add(id);
    observed.set(id, { lease, changedAt: now(), hashes: next });
    if (
      owned.has(id) &&
      (lease.spec.holderIdentity !== options.replica ||
        owned.get(id).epoch !== epoch ||
        owned.get(id).uid !== lease.metadata.uid)
    )
      forget(id);
    return ownerOf(lease);
  };
  const get = (path: string) =>
    api(path, "GET", undefined, signal()).catch((error) => {
      if (!absent(error)) throw error;
    });
  const readOwnedLease = async (id: string) => {
    const local = owned.get(id);
    const lease =
      local && now() < local.deadline && (await get(`${leases}/${name(id)}`));
    if (
      !lease ||
      lease.spec?.holderIdentity !== options.replica ||
      lease.metadata?.uid !== local.uid ||
      Number(lease.metadata?.annotations?.[prefix + "epoch"]) !== local.epoch ||
      owned.get(id) !== local ||
      now() >= local.deadline
    ) {
      forget(id);
      throw Error("browser_controller_fenced");
    }
    observe(lease);
    return lease;
  };
  const assertOwned = async (id: string) => {
    const lease = await readOwnedLease(id);
    return { ...ownerOf(lease), name: lease.metadata.name };
  };
  const decode = (secret: any, id: string): ExecutionRecord | undefined => {
    verify(secret, name(id));
    let body: any;
    try {
      body = JSON.parse(Buffer.from(secret.data?.record ?? "", "base64").toString());
    } catch {
      throw Error("browser_controller_state_corrupt");
    }
    if (
      body?.protocol !== "browser-sandbox.shared-execution/v1" ||
      body.executionId !== id ||
      (body.record !== undefined && !isExecutionRecord(body.record, id))
    )
      throw Error("browser_controller_state_corrupt");
    return body.record;
  };
  const encode = (id: string, record?: ExecutionRecord) => {
    const json = JSON.stringify({
      protocol: "browser-sandbox.shared-execution/v1",
      executionId: id,
      record,
    });
    if (Buffer.byteLength(json) > 65536)
      throw Error("browser_execution_record_too_large");
    return { record: Buffer.from(json).toString("base64") };
  };
  const save = (
    kind: "Secret" | "Lease",
    metadata: any,
    fields: object,
    owner = parent,
  ) => {
    const collection = kind === "Secret" ? secrets : leases;
    return api(
      metadata.resourceVersion ? `${collection}/${metadata.name}` : collection,
      metadata.resourceVersion ? "PUT" : "POST",
      {
        apiVersion: kind === "Secret" ? "v1" : "coordination.k8s.io/v1",
        kind,
        ...(kind === "Secret" ? { type: "Opaque" } : {}),
        ...fields,
        metadata: { ...metadata, ownerReferences: [owner] },
      },
      signal(),
    );
  };
  const saveRecord = (
    id: string,
    previous: any,
    record: ExecutionRecord | undefined,
    fence: Pick<ExecutionOwner, "uid" | "epoch">,
  ) =>
    save(
      "Secret",
      {
        ...previous?.metadata,
        name: name(id),
        labels,
        annotations: {
          ...previous?.metadata.annotations,
          [prefix + "epoch"]: String(fence.epoch),
        },
      },
      { data: encode(id, record) },
      kubernetesOwnerReference("Lease", name(id), fence.uid),
    );
  const saveLease = async (
    previous: any,
    annotations: Record<string, string>,
    spec: object,
  ) => {
    const updated = await save(
      "Lease",
      {
        ...previous.metadata,
        annotations: { ...previous.metadata.annotations, ...annotations },
      },
      { spec: { ...previous.spec, ...spec } },
    );
    if (updated.metadata.labels[prefix + "state"] === "execution") observe(updated);
    return updated;
  };
  const readSecret = async (id: string) => {
    const secret = await get(`${secrets}/${name(id)}`);
    return { secret, record: secret && decode(secret, id) };
  };
  const deleteVersion = (
    path: string,
    { uid, resourceVersion }: { uid: string; resourceVersion: string },
  ) => api(path, "DELETE", { preconditions: { uid, resourceVersion } }, signal());
  const secretOwnedBy = (secret: any, uid: string) =>
    secret?.metadata.ownerReferences[0].uid === uid;
  const handoff = async (id: string) => {
    for (let attempt = 0; attempt < 16; attempt++) {
      const { secret, record } = await readSecret(id);
      const fence = await assertOwned(id);
      try {
        if (secret && !secretOwnedBy(secret, fence.uid)) {
          await deleteVersion(`${secrets}/${name(id)}`, secret.metadata);
          continue;
        }
        // Epoch metadata makes takeover a real mutation; Kubernetes can elide no-op PUTs.
        await saveRecord(id, secret, record, fence);
        const profileId = record?.data.profileId;
        if (profileId !== undefined) await profile(id, profileId, "fence");
        return;
      } catch (error) {
        if (!conflict(error)) throw error;
      }
    }
    throw Error("browser_controller_owner_moved");
  };
  const resolveOnce = async (
    id: string,
    create = false,
  ): Promise<ExecutionOwner | undefined> => {
    if (!validId(id)) throw Error("browser_execution_id_invalid");
    let lease = observed.get(id)?.lease ?? (await get(`${leases}/${name(id)}`));
    if (
      lease &&
      now() - (observed.get(id)?.changedAt ?? now()) >=
        lease.spec.leaseDurationSeconds * 1000
    )
      lease = await get(`${leases}/${name(id)}`);
    let current = lease && observe(lease);
    if (!lease && !create) return;
    const observation = observed.get(id);
    const expired =
      lease &&
      (!current?.replica ||
        now() - observation.changedAt >= lease.spec.leaseDurationSeconds * 1000);
    if (!lease || expired) {
      const epoch = Number(lease?.metadata.annotations[prefix + "epoch"] ?? 0) + 1;
      const started = now();
      try {
        lease = await saveLease(
          lease ?? {
            metadata: {
              name: name(id),
              labels: { ...labels, [prefix + "execution"]: id },
            },
          },
          {
            [prefix + "epoch"]: String(epoch),
            [prefix + "peer"]: options.peerOrigin,
            [prefix + "phase"]: "active",
          },
          {
            holderIdentity: options.replica,
            leaseDurationSeconds: Math.ceil(leaseMs / 1000),
            renewTime: renewTime(),
          },
        );
        owned.set(id, {
          epoch,
          uid: lease.metadata.uid,
          deadline: started + leaseMs,
          ready: false,
        });
        current = ownerOf(lease);
      } catch (error) {
        if (!conflict(error) && !absent(error)) throw error;
        const uid = lease?.metadata.uid;
        lease = await get(`${leases}/${name(id)}`);
        if (!lease) {
          if (uid) evict(id, uid);
          if (!create) return;
          throw Error("browser_controller_owner_moved");
        }
        current = observe(lease);
      }
    }
    if (current?.replica === options.replica) {
      const local = owned.get(id);
      if (!local) throw Error("browser_controller_fenced");
      if (!local.ready) {
        await handoff(id);
        await assertOwned(id);
        local.ready = true;
      }
    }
    return current;
  };
  const serialize = createSerialWork();
  const resolve = (id: string, create = false) =>
    serialize(id, () => resolveOnce(id, create));
  const readOwnedRecord = async (id: string) => {
    const { secret, record } = await readSecret(id);
    const lease = await readOwnedLease(id);
    if (!secretOwnedBy(secret, lease.metadata.uid)) {
      forget(id);
      throw Error("browser_controller_fenced");
    }
    return { lease, secret, record };
  };
  const write = async (id: string, revision: number, data: Record<string, unknown>) => {
    const { lease, secret, record: previous } = await readOwnedRecord(id);
    if ((previous?.revision ?? 0) !== revision)
      throw Error(
        revision ? "browser_execution_record_changed" : "browser_execution_revoked",
      );
    const record = { executionId: id, revision: revision + 1, data };
    try {
      await saveRecord(id, secret, record, ownerOf(lease));
    } catch (error) {
      if (conflict(error)) {
        forget(id);
        throw Error("browser_controller_fenced");
      }
      throw error;
    }
    return record;
  };
  const profile = async (
    id: string,
    profileId: unknown,
    action: "acquire" | "release" | "fence",
  ) => {
    if (!validProfileId(profileId)) throw Error("browser_profile_id_invalid");
    const key = profileName(profileId),
      path = `${leases}/${key}`;
    for (let attempt = 0; attempt < (action === "fence" ? 16 : 1); attempt++) {
      const previous = await get(path);
      const { lease, record } = await readOwnedRecord(id);
      if (!record || record.data.profileId !== profileId)
        throw Error("browser_profile_binding_invalid");
      const finished = record.data.phase === "finished";
      if (action !== "fence" && (action === "release" ? !finished : finished))
        throw Error("browser_execution_revoked");
      if (previous) {
        verify(previous, key, "profile");
        if (previous.spec?.holderIdentity && previous.spec.holderIdentity !== id) {
          if (
            action === "release" ||
            (action === "fence" && (finished || record.data.profileAcquired === false))
          )
            return;
          throw Error("browser_profile_busy");
        }
      }
      const holder =
        action === "fence"
          ? (previous?.spec?.holderIdentity ?? "")
          : action === "release"
            ? ""
            : id;
      // Keep a free tombstone: an old absent-resource POST must not resurrect ownership.
      if (
        action !== "fence" &&
        previous &&
        previous.spec.holderIdentity === holder &&
        previous.metadata.annotations?.[prefix + "execution-uid"] === lease.metadata.uid
      )
        return;
      try {
        await saveLease(
          previous ?? {
            metadata: {
              name: key,
              labels: { ...labels, [prefix + "state"]: "profile" },
            },
          },
          {
            [prefix + "execution-uid"]: lease.metadata.uid,
            [prefix + "epoch"]: String(ownerOf(lease).epoch),
          },
          { holderIdentity: holder },
        );
        return;
      } catch (error) {
        if (!conflict(error)) throw error;
        if (action !== "fence") throw Error("browser_profile_busy");
      }
    }
    throw Error("browser_controller_owner_moved");
  };
  const indexLegacy = async (record: ExecutionRecord) => {
    const id = record.executionId;
    const lease = await readOwnedLease(id);
    const grants = Array.isArray(record.data.grants) ? record.data.grants : [];
    const hashes = grants.map((grant: any) => {
      if (!/^[a-f0-9]{64}$/.test(grant.token))
        throw Error("browser_controller_state_corrupt");
      return createHash("sha256").update(grant.token).digest("hex");
    });
    await saveLease(
      lease,
      { [prefix + "legacy-capabilities"]: JSON.stringify(hashes) },
      {},
    );
  };
  const complete = async (id: string) => {
    const { lease, record } = await readOwnedRecord(id);
    if (record?.data.phase !== "finished")
      throw Error("browser_execution_record_changed");
    if (record.data.profileId)
      await profile(id, String(record.data.profileId), "release");
    await saveLease(
      lease,
      {
        [prefix + "phase"]: "finished",
        [prefix + "finished-at"]:
          lease.metadata.annotations[prefix + "finished-at"] ?? String(Date.now()),
      },
      { holderIdentity: "" },
    );
  };
  const compact = async () => {
    const finished = [...observed.entries()]
      .filter(
        ([, entry]) =>
          entry.lease.metadata.annotations[prefix + "phase"] === "finished",
      )
      .sort(
        (a, b) =>
          Number(a[1].lease.metadata.annotations[prefix + "finished-at"]) -
          Number(b[1].lease.metadata.annotations[prefix + "finished-at"]),
      );
    for (const [id, { lease }] of finished.slice(0, -maxFinished)) {
      if (lease.spec.holderIdentity) continue;
      try {
        // Recovery must change this Lease before changing its record.
        await deleteVersion(`${leases}/${name(id)}`, lease.metadata);
        evict(id, lease.metadata.uid);
      } catch (error) {
        if (absent(error)) evict(id, lease.metadata.uid);
      }
    }
  };

  let renewing: Promise<void>;
  const renew = () =>
    (renewing ??= (async () => {
      const entries = [...owned];
      for (let start = 0; start < entries.length; start += 8)
        await Promise.all(
          entries.slice(start, start + 8).map(async ([id, local]) => {
            try {
              if (!local.ready) return;
              const lease = await readOwnedLease(id);
              const started = now();
              await saveLease(lease, {}, { renewTime: renewTime() });
              if (owned.get(id) === local) local.deadline = started + leaseMs;
            } catch (error) {
              if (
                now() >= local.deadline ||
                (error as Error).message === "browser_controller_fenced"
              )
                forget(id);
            }
          }),
        );
    })().finally(() => {
      renewing = undefined;
    }));
  const renewal = setInterval(() => {
    void renew().catch(() => {});
  }, leaseMs / 3);
  renewal.unref();
  return {
    lookup(id: string): ExecutionOwner | undefined {
      const lease = observed.get(id)?.lease;
      return lease && ownerOf(lease);
    },
    resolve,
    assertOwned,
    complete,
    compact,
    findCapability(token: string) {
      if (!/^[a-f0-9]{64}$/.test(token)) return undefined;
      return legacyCapabilities.get(createHash("sha256").update(token).digest("hex"));
    },
    indexLegacy,
    async migrationComplete() {
      const marker = await get(`${secrets}/${markerName}`);
      if (!marker) return false;
      verify(marker, markerName, "migration");
      if (marker.data?.protocol !== markerProtocol)
        throw Error("browser_controller_state_corrupt");
      return true;
    },
    async finishMigration() {
      try {
        await save(
          "Secret",
          {
            name: markerName,
            labels: { ...labels, [prefix + "state"]: "migration" },
          },
          { data: { protocol: markerProtocol } },
        );
      } catch (error) {
        if (!conflict(error)) throw error;
      }
    },
    async importRecord(record: ExecutionRecord) {
      const id = record.executionId;
      const { lease, secret, record: previous } = await readOwnedRecord(id);
      if (previous) throw Error("browser_execution_migration_conflict");
      await saveRecord(id, secret, record, ownerOf(lease));
    },
    epoch: (id: string) => owned.get(id)?.epoch,
    owns: (id: string) =>
      Boolean(owned.get(id)?.ready && now() < owned.get(id).deadline),
    onLost(callback: (id: string) => void) {
      lost = callback;
    },
    acquireProfile: (id: string, profileId: string) =>
      profile(id, profileId, "acquire"),
    releaseProfile: (id: string, profileId: string) =>
      profile(id, profileId, "release"),
    journal: {
      async read(id: string) {
        const lease = await get(`${leases}/${name(id)}`);
        if (!lease) return undefined;
        verify(lease, name(id));
        const { secret, record } = await readSecret(id);
        return secretOwnedBy(secret, lease.metadata.uid) ? record : undefined;
      },
      create: (id: string, data: Record<string, unknown>) => write(id, 0, data),
      update: write,
    },
    async refresh() {
      const missing = new Set(observed.keys());
      listing = new Set<string>();
      try {
        const version = leaseVersion;
        let listedVersion = version;
        for await (const page of listKubernetesPages(
          api,
          leases,
          selector,
          AbortSignal.any([life.signal, AbortSignal.timeout(30000)]),
          131072,
          "browser_execution_capacity_exceeded",
        )) {
          for (const lease of page.items) {
            const id = lease.metadata?.labels?.[prefix + "execution"];
            missing.delete(id);
            if (!listing.has(id)) observe(lease);
          }
          listedVersion = page.metadata?.resourceVersion ?? listedVersion;
        }
        for (const id of missing) if (!listing.has(id)) evict(id);
        if (leaseVersion === version) leaseVersion = listedVersion;
      } finally {
        listing = undefined;
      }
      await compact();
      return [...observed]
        .filter(
          ([, entry]) =>
            entry.lease.metadata.annotations[prefix + "phase"] !== "finished",
        )
        .map(([id]) => id);
    },
    renew,
    watch() {
      return (watching ??= (async () => {
        while (!life.signal.aborted) {
          try {
            await api(
              `${leases}?watch=true&allowWatchBookmarks=true&timeoutSeconds=10&labelSelector=${selector}&resourceVersion=${encodeURIComponent(leaseVersion)}`,
              "GET",
              undefined,
              signal(),
              (event) => {
                if (event.type === "ERROR") {
                  leaseVersion = "";
                  throw Error("browser_kubernetes_watch_expired");
                }
                leaseVersion = event.object?.metadata?.resourceVersion ?? leaseVersion;
                if (["ADDED", "MODIFIED"].includes(event.type)) observe(event.object);
                if (event.type === "DELETED") {
                  const id = event.object?.metadata?.labels?.[prefix + "execution"];
                  const uid = event.object?.metadata?.uid;
                  if (validId(id) && typeof uid === "string" && uid) evict(id, uid);
                }
              },
            );
          } catch {
            if (!life.signal.aborted)
              await delay(250, undefined, { signal: life.signal }).catch(() => {});
          }
        }
      })());
    },
    async close() {
      clearInterval(expiry);
      clearInterval(renewal);
      life.abort();
      await watching;
      await renewing;
    },
    async release() {
      clearInterval(renewal);
      await renewing;
      for (const id of [...owned.keys()]) {
        try {
          const lease = await readOwnedLease(id);
          await saveLease(lease, {}, { holderIdentity: "" });
        } catch (error) {
          if (
            (error as Error).message !== "browser_controller_fenced" &&
            !conflict(error)
          )
            throw error;
        } finally {
          forget(id);
        }
      }
    },
  };
}
export type KubernetesOwnership = Awaited<ReturnType<typeof createKubernetesOwnership>>;
