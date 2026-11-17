import { createHash } from "node:crypto";
import { performance } from "node:perf_hooks";
import { setTimeout as delay } from "node:timers/promises";
import type { ExecutionRecord } from "./execution-journal.js";
import {
  createKubernetesClient,
  type KubernetesAccess,
  KubernetesError,
} from "./kubernetes-api.js";
import { validProfileId } from "./local-profiles.js";

import type { ExecutionOwner } from "./managed-backend.js";

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
    return (
      url.protocol === "http:" && url.origin === value && !url.username && !url.password
    );
  } catch {
    return false;
  }
};

/** Kubernetes conditional writes own arbitration; cached observations only determine expiry. */
export function createKubernetesOwnership(
  options: KubernetesAccess & {
    installation: string;
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
  const leases = `/apis/coordination.k8s.io/v1/namespaces/${options.namespace}/leases`;
  const secrets = `/api/v1/namespaces/${options.namespace}/secrets`;
  const salt = createHash("sha256")
    .update(options.installation)
    .digest("hex")
    .slice(0, 12);
  const name = (id: string) => `bs-${salt}-e-${id}`;
  const profileName = (id: string) =>
    `bs-${salt}-p-${createHash("sha256").update(options.installation).update("\0").update(id).digest("hex").slice(0, 32)}`;
  const selector = encodeURIComponent(
    `${prefix}installation=${options.installation},${prefix}state=execution`,
  );
  const labels = {
    [prefix + "installation"]: options.installation,
    [prefix + "state"]: "execution",
  };
  const observed = new Map<string, { lease: any; changedAt: number }>();
  const owned = new Map<string, { epoch: number; deadline: number; ready: boolean }>();
  const records = new Map<string, ExecutionRecord>();
  const legacyCapabilities = new Map<string, string>();
  const legacyByExecution = new Map<string, Set<string>>();
  const clearLegacy = (id: string) => {
    for (const hash of legacyByExecution.get(id) ?? []) legacyCapabilities.delete(hash);
    legacyByExecution.delete(id);
  };
  const life = new AbortController();
  let watching: Promise<void>;
  let leaseVersion = "";
  let lost: (id: string) => void = () => {};
  const signal = () => AbortSignal.any([life.signal, AbortSignal.timeout(15000)]);
  const verify = (object: any, expectedName: string, state = "execution") => {
    if (
      object?.metadata?.name !== expectedName ||
      object.metadata.labels?.[prefix + "installation"] !== options.installation ||
      object.metadata.labels?.[prefix + "state"] !== state ||
      typeof object.metadata.resourceVersion !== "string"
    )
      throw Error("browser_controller_state_corrupt");
  };
  const forget = (id: string) => {
    if (owned.delete(id)) lost(id);
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
    if (
      !Number.isSafeInteger(epoch) ||
      epoch < 1 ||
      !validPeerOrigin(lease.metadata.annotations?.[prefix + "peer"]) ||
      !Number.isSafeInteger(lease.spec?.leaseDurationSeconds) ||
      lease.spec.leaseDurationSeconds < 1
    )
      throw Error("browser_controller_state_corrupt");
    const previous = observed.get(id);
    if (previous) {
      const priorEpoch = Number(previous.lease.metadata.annotations[prefix + "epoch"]);
      if (epoch < priorEpoch)
        return {
          replica: previous.lease.spec.holderIdentity,
          origin: previous.lease.metadata.annotations[prefix + "peer"],
          epoch: priorEpoch,
        } as ExecutionOwner;
    }
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
    if (
      legacyCapabilities.size - (legacyByExecution.get(id)?.size ?? 0) + next.size >
      200000
    )
      throw Error("browser_execution_capacity_exceeded");
    clearLegacy(id);
    legacyByExecution.set(id, next);
    for (const hash of next) legacyCapabilities.set(hash, id);
    if (previous?.lease.metadata.resourceVersion !== lease.metadata.resourceVersion)
      observed.set(id, { lease, changedAt: now() });
    if (
      owned.has(id) &&
      (lease.spec.holderIdentity !== options.replica || owned.get(id).epoch !== epoch)
    )
      forget(id);
    return {
      replica: lease.spec.holderIdentity,
      origin: lease.metadata.annotations[prefix + "peer"],
      epoch,
    } as ExecutionOwner;
  };
  const get = async (path: string) => {
    try {
      return await api(path, "GET", undefined, signal());
    } catch (error) {
      if (absent(error)) return;
      throw error;
    }
  };
  const readOwnedLease = async (id: string) => {
    const local = owned.get(id);
    if (!local || now() >= local.deadline) {
      forget(id);
      throw Error("browser_controller_fenced");
    }
    const lease = await get(`${leases}/${name(id)}`);
    if (
      !lease ||
      lease.spec?.holderIdentity !== options.replica ||
      Number(lease.metadata?.annotations?.[prefix + "epoch"]) !== local.epoch ||
      owned.get(id) !== local ||
      now() >= local.deadline
    ) {
      forget(id);
      throw Error("browser_controller_fenced");
    }
    observe(lease);
    return { epoch: local.epoch, lease };
  };
  const assertOwned = async (id: string) => (await readOwnedLease(id)).epoch;
  const decode = (secret: any, id: string) => {
    verify(secret, name(id));
    let body: any;
    try {
      body = JSON.parse(Buffer.from(secret.data?.record ?? "", "base64").toString());
    } catch {
      throw Error("browser_controller_state_corrupt");
    }
    if (
      body.protocol !== "browser-sandbox.shared-execution/v1" ||
      body.executionId !== id ||
      !Number.isSafeInteger(body.epoch) ||
      body.epoch < 1 ||
      (body.record &&
        (body.record.executionId !== id ||
          !Number.isSafeInteger(body.record.revision) ||
          body.record.revision < 1 ||
          !body.record.data ||
          typeof body.record.data !== "object" ||
          Array.isArray(body.record.data)))
    )
      throw Error("browser_controller_state_corrupt");
    return body as {
      protocol: string;
      executionId: string;
      epoch: number;
      record?: ExecutionRecord;
    };
  };
  const encode = (id: string, epoch: number, record?: ExecutionRecord) => {
    const json = JSON.stringify({
      protocol: "browser-sandbox.shared-execution/v1",
      executionId: id,
      epoch,
      record,
    });
    if (Buffer.byteLength(json) > 65536)
      throw Error("browser_execution_record_too_large");
    return { record: Buffer.from(json).toString("base64") };
  };
  const fenceProfile = async (id: string, epoch: number, record?: ExecutionRecord) => {
    const profileId = record?.data.profileId;
    if (profileId === undefined) return;
    if (typeof profileId !== "string" || !validProfileId(profileId))
      throw Error("browser_profile_id_invalid");
    const key = profileName(profileId),
      path = `${leases}/${key}`;
    for (let attempt = 0; attempt < 16; attempt++) {
      const previous = await get(path);
      if (previous) {
        verify(previous, key, "profile");
        if (previous.spec?.holderIdentity && previous.spec.holderIdentity !== id) {
          if (record.data.phase === "finished" || record.data.profileAcquired === false)
            return;
          throw Error("browser_profile_busy");
        }
        const same = previous.metadata.annotations?.[prefix + "execution"] === id;
        const priorEpoch = Number(previous.metadata.annotations?.[prefix + "epoch"]);
        if (same && priorEpoch > epoch) throw Error("browser_controller_fenced");
        if (same && priorEpoch === epoch) return;
      }
      try {
        await api(
          previous ? path : leases,
          previous ? "PUT" : "POST",
          {
            apiVersion: "coordination.k8s.io/v1",
            kind: "Lease",
            metadata: {
              name: key,
              labels: { ...labels, [prefix + "state"]: "profile" },
              annotations: {
                [prefix + "epoch"]: String(epoch),
                [prefix + "execution"]: id,
              },
              ...(previous
                ? { resourceVersion: previous.metadata.resourceVersion }
                : {}),
            },
            spec: { holderIdentity: previous?.spec?.holderIdentity ?? "" },
          },
          signal(),
        );
        return;
      } catch (error) {
        if (!conflict(error)) throw error;
      }
    }
    throw Error("browser_controller_owner_moved");
  };
  const handoff = async (id: string, epoch: number) => {
    for (let attempt = 0; attempt < 16; attempt++) {
      await assertOwned(id);
      const secret = await get(`${secrets}/${name(id)}`);
      const body = secret && decode(secret, id);
      if (body && body.epoch > epoch) throw Error("browser_controller_fenced");
      if (body?.epoch === epoch) {
        await fenceProfile(id, epoch, body.record);
        return;
      }
      try {
        await api(
          secret ? `${secrets}/${name(id)}` : secrets,
          secret ? "PUT" : "POST",
          {
            apiVersion: "v1",
            kind: "Secret",
            type: "Opaque",
            metadata: {
              name: name(id),
              labels,
              ...(secret ? { resourceVersion: secret.metadata.resourceVersion } : {}),
            },
            data: encode(id, epoch, body?.record),
          },
          signal(),
        );
        await fenceProfile(id, epoch, body?.record);
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
      (!current.replica ||
        now() - observation.changedAt >= lease.spec.leaseDurationSeconds * 1000);
    if (!lease || expired) {
      const epoch = (current?.epoch ?? 0) + 1;
      const started = now();
      try {
        lease = await api(
          lease ? `${leases}/${name(id)}` : leases,
          lease ? "PUT" : "POST",
          {
            apiVersion: "coordination.k8s.io/v1",
            kind: "Lease",
            metadata: {
              name: name(id),
              labels: { ...labels, [prefix + "execution"]: id },
              annotations: {
                ...lease?.metadata.annotations,
                [prefix + "epoch"]: String(epoch),
                [prefix + "peer"]: options.peerOrigin,
              },
              ...(lease ? { resourceVersion: lease.metadata.resourceVersion } : {}),
            },
            spec: {
              holderIdentity: options.replica,
              leaseDurationSeconds: Math.ceil(leaseMs / 1000),
              renewTime: renewTime(),
            },
          },
          signal(),
        );
        observe(lease);
        owned.set(id, { epoch, deadline: started + leaseMs, ready: false });
        await handoff(id, epoch);
        await assertOwned(id);
        owned.get(id).ready = true;
        current = observe(lease);
      } catch (error) {
        if (!conflict(error)) throw error;
        lease = await get(`${leases}/${name(id)}`);
        if (!lease) throw Error("browser_controller_owner_moved");
        current = observe(lease);
      }
    }
    if (current.replica === options.replica) {
      const local = owned.get(id);
      if (!local) throw Error("browser_controller_fenced");
      if (!local.ready) {
        await handoff(id, local.epoch);
        await assertOwned(id);
        local.ready = true;
      }
    }
    return current;
  };
  const resolving = new Map<string, Promise<ExecutionOwner | undefined>>();
  const resolve = (id: string, create = false) => {
    const work = (resolving.get(id) ?? Promise.resolve())
      .catch(() => {})
      .then(() => resolveOnce(id, create));
    resolving.set(id, work);
    void work
      .finally(() => {
        if (resolving.get(id) === work) resolving.delete(id);
      })
      .catch(() => {});
    return work;
  };
  const read = async (id: string) => {
    const secret = await get(`${secrets}/${name(id)}`);
    if (!secret) return undefined;
    const body = decode(secret, id);
    if (body.record) records.set(id, body.record);
    return body.record;
  };
  const readOwnedRecord = async (id: string, allowMissing = false) => {
    const { epoch, lease } = await readOwnedLease(id);
    const secret = await get(`${secrets}/${name(id)}`);
    const body = secret && decode(secret, id);
    if ((!body && !allowMissing) || (body && body.epoch !== epoch)) {
      forget(id);
      throw Error("browser_controller_fenced");
    }
    return { epoch, lease, secret, body };
  };
  const write = async (id: string, revision: number, data: Record<string, unknown>) => {
    const { epoch, secret, body } = await readOwnedRecord(id);
    if ((body.record?.revision ?? 0) !== revision)
      throw Error(
        revision ? "browser_execution_record_changed" : "browser_execution_revoked",
      );
    const record = { executionId: id, revision: revision + 1, data };
    try {
      await api(
        `${secrets}/${name(id)}`,
        "PUT",
        { ...secret, data: encode(id, epoch, record) },
        signal(),
      );
    } catch (error) {
      if (conflict(error)) {
        forget(id);
        throw Error("browser_controller_fenced");
      }
      throw error;
    }
    records.set(id, record);
    return record;
  };
  const profile = async (id: string, profileId: string, release: boolean) => {
    if (!validProfileId(profileId)) throw Error("browser_profile_id_invalid");
    const path = `${leases}/${profileName(profileId)}`;
    const previous = await get(path);
    const { epoch, body } = await readOwnedRecord(id);
    if (!body.record || body.record.data.profileId !== profileId)
      throw Error("browser_profile_binding_invalid");
    if (
      release
        ? body.record.data.phase !== "finished"
        : body.record.data.phase === "finished"
    )
      throw Error("browser_execution_revoked");
    if (previous) {
      verify(previous, profileName(profileId), "profile");
      const holder = previous.spec?.holderIdentity;
      if (holder && holder !== id) {
        if (release) return;
        throw Error("browser_profile_busy");
      }
      if (
        previous.metadata.annotations?.[prefix + "execution"] === id &&
        Number(previous.metadata.annotations?.[prefix + "epoch"]) > epoch
      )
        throw Error("browser_controller_fenced");
    }
    // Keep a free tombstone: deleting it would let an old absent-resource POST resurrect ownership.
    if (
      previous &&
      previous.spec.holderIdentity === (release ? "" : id) &&
      previous.metadata.annotations?.[prefix + "execution"] === id &&
      Number(previous.metadata.annotations?.[prefix + "epoch"]) === epoch
    )
      return;
    try {
      await api(
        previous ? path : leases,
        previous ? "PUT" : "POST",
        {
          apiVersion: "coordination.k8s.io/v1",
          kind: "Lease",
          metadata: {
            name: profileName(profileId),
            labels: { ...labels, [prefix + "state"]: "profile" },
            annotations: {
              [prefix + "epoch"]: String(epoch),
              [prefix + "execution"]: id,
            },
            ...(previous ? { resourceVersion: previous.metadata.resourceVersion } : {}),
          },
          spec: { holderIdentity: release ? "" : id },
        },
        signal(),
      );
    } catch (error) {
      if (conflict(error)) throw Error("browser_profile_busy");
      throw error;
    }
  };
  const indexLegacy = async (record: ExecutionRecord) => {
    const id = record.executionId;
    const { lease } = await readOwnedLease(id);
    const grants = Array.isArray(record.data.grants) ? record.data.grants : [];
    const hashes = grants.map((grant: any) => {
      if (!/^[a-f0-9]{64}$/.test(grant.token))
        throw Error("browser_controller_state_corrupt");
      return createHash("sha256").update(grant.token).digest("hex");
    });
    const updated = await api(
      `${leases}/${name(id)}`,
      "PUT",
      {
        ...lease,
        metadata: {
          ...lease.metadata,
          annotations: {
            ...lease.metadata.annotations,
            [prefix + "legacy-capabilities"]: JSON.stringify(hashes),
          },
        },
      },
      signal(),
    );
    observe(updated);
  };
  const complete = async (id: string) => {
    const { epoch, lease, body } = await readOwnedRecord(id);
    const record = body.record;
    if (record?.data.phase !== "finished")
      throw Error("browser_execution_record_changed");
    if (record.data.profileId) await profile(id, String(record.data.profileId), true);
    const updated = await api(
      `${leases}/${name(id)}`,
      "PUT",
      {
        ...lease,
        metadata: {
          ...lease.metadata,
          annotations: {
            ...lease.metadata.annotations,
            [prefix + "phase"]: "finished",
            [prefix + "finished-at"]:
              lease.metadata.annotations[prefix + "finished-at"] ?? String(Date.now()),
            [prefix + "epoch"]: String(epoch),
          },
        },
        spec: { ...lease.spec, holderIdentity: "" },
      },
      signal(),
    );
    observe(updated);
    forget(id);
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
    for (const [id] of finished.slice(0, Math.max(0, finished.length - maxFinished))) {
      await resolve(id);
      if (!owned.has(id)) continue;
      const { lease, secret, body } = await readOwnedRecord(id, true);
      if (body?.record && body.record.data.phase !== "finished") continue;
      if (body?.record?.data.profileId)
        await profile(id, String(body.record.data.profileId), true);
      if (secret)
        await api(
          `${secrets}/${name(id)}`,
          "DELETE",
          { preconditions: { resourceVersion: secret.metadata.resourceVersion } },
          signal(),
        );
      await api(
        `${leases}/${name(id)}`,
        "DELETE",
        {
          preconditions: {
            resourceVersion: lease.metadata.resourceVersion,
          },
        },
        signal(),
      );
      observed.delete(id);
      records.delete(id);
      clearLegacy(id);
      forget(id);
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
              const { lease } = await readOwnedLease(id);
              const started = now();
              const updated = await api(
                `${leases}/${name(id)}`,
                "PUT",
                {
                  ...lease,
                  spec: { ...lease.spec, renewTime: renewTime() },
                },
                signal(),
              );
              observe(updated);
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
      if (!lease) return;
      return {
        replica: lease.spec.holderIdentity,
        origin: lease.metadata.annotations[prefix + "peer"],
        epoch: Number(lease.metadata.annotations[prefix + "epoch"]),
      };
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
      const marker = await get(`${secrets}/bs-${salt}-journal-import`);
      if (!marker) return false;
      verify(marker, `bs-${salt}-journal-import`, "migration");
      if (
        marker.data?.protocol !==
        Buffer.from("browser-sandbox.journal-import/v1").toString("base64")
      )
        throw Error("browser_controller_state_corrupt");
      return true;
    },
    async finishMigration() {
      try {
        await api(
          secrets,
          "POST",
          {
            apiVersion: "v1",
            kind: "Secret",
            type: "Opaque",
            metadata: {
              name: `bs-${salt}-journal-import`,
              labels: { ...labels, [prefix + "state"]: "migration" },
            },
            data: {
              protocol: Buffer.from("browser-sandbox.journal-import/v1").toString(
                "base64",
              ),
            },
          },
          signal(),
        );
      } catch (error) {
        if (!conflict(error)) throw error;
      }
    },
    async importRecord(record: ExecutionRecord) {
      const id = record.executionId;
      const { epoch, secret, body } = await readOwnedRecord(id);
      if (body.record) throw Error("browser_execution_migration_conflict");
      await api(
        `${secrets}/${name(id)}`,
        "PUT",
        { ...secret, data: encode(id, epoch, record) },
        signal(),
      );
      records.set(id, record);
    },
    epoch: (id: string) => owned.get(id)?.epoch,
    owns: (id: string) =>
      Boolean(owned.get(id)?.ready && now() < owned.get(id).deadline),
    onLost(callback: (id: string) => void) {
      lost = callback;
    },
    acquireProfile: (id: string, profileId: string) => profile(id, profileId, false),
    releaseProfile: (id: string, profileId: string) => profile(id, profileId, true),
    journal: {
      read,
      has: (id: string) => records.has(id),
      ids: () => [...records.keys()],
      finishedIds: () =>
        [...records.values()]
          .filter((r) => r.data.phase === "finished")
          .map((r) => r.executionId),
      create: (id: string, data: Record<string, unknown>) => write(id, 0, data),
      update: write,
    },
    async refresh() {
      let cursor = "";
      let count = 0;
      do {
        const page = await api(
          `${leases}?labelSelector=${selector}&limit=100&continue=${encodeURIComponent(cursor)}`,
          "GET",
          undefined,
          signal(),
        );
        if (!Array.isArray(page.items) || (count += page.items.length) > 131072)
          throw Error("browser_execution_capacity_exceeded");
        for (const lease of page.items) observe(lease);
        leaseVersion = page.metadata?.resourceVersion ?? leaseVersion;
        cursor = page.metadata?.continue ?? "";
      } while (cursor);
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
                  observed.delete(id);
                  clearLegacy(id);
                  records.delete(id);
                  forget(id);
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
          const { lease } = await readOwnedLease(id);
          await api(
            `${leases}/${name(id)}`,
            "PUT",
            { ...lease, spec: { ...lease.spec, holderIdentity: "" } },
            signal(),
          );
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
export type KubernetesOwnership = ReturnType<typeof createKubernetesOwnership>;
