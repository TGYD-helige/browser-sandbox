import { createHash, randomBytes } from "node:crypto";
import { constants, createWriteStream } from "node:fs";
import fs from "node:fs/promises";
import type { IncomingMessage, ServerResponse } from "node:http";
import path from "node:path";
import { Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { StorageAdapter } from "./host-client.js";
import { validProfileId } from "./local-profiles.js";
import { MAX_ARCHIVE_BYTES } from "./profiles.js";

interface StoredProfile {
  profileId: string;
  archive: Record<string, any>;
  key: string;
  lastUsed: number;
}
interface Grant {
  method: "GET" | "PUT";
  objectKey: string;
  key: string;
  expiresAt: number;
  session: AbortController;
  running?: boolean;
  uploaded?: { sha256: string; byteSize: number };
}
export interface ProfileStoreOptions {
  root: string;
  /** Private worker-reachable origin. Never mount the store into a worker. */
  baseUrl: string;
  maxProfiles?: number;
  maxBytes?: number;
  maxStagedBytes?: number;
  maxTransfers?: number;
  maxGrants?: number;
  idleTtlMs?: number;
  now?: () => number;
}
const hex = (value, length) =>
  typeof value === "string" && new RegExp(`^[a-f0-9]{${length}}$`).test(value);
const profileKey = (profileId: string) =>
  createHash("sha256").update(profileId).digest("hex");
const schema = "browser-sandbox.profile-store/v1";
const invalid = () => Error("browser_profile_store_invalid");
function validArchive(archive) {
  return (
    archive?.schema === "browser-sandbox.profile/v1" &&
    typeof archive.browserVersion === "string" &&
    archive.browserVersion.length < 256 &&
    hex(archive.iv, 24) &&
    hex(archive.tag, 32) &&
    hex(archive.sha256, 64) &&
    hex(archive.objectKey, 64) &&
    Number.isSafeInteger(archive.byteSize) &&
    archive.byteSize > 0 &&
    archive.byteSize <= MAX_ARCHIVE_BYTES
  );
}

/** Single-controller volume adapter; worker archive format and host adapter stay unchanged. */
export async function createProfileStore({
  root,
  baseUrl,
  maxProfiles = 10000,
  maxBytes = 10 * 1024 ** 3,
  maxStagedBytes = MAX_ARCHIVE_BYTES * 4,
  maxTransfers = 4,
  maxGrants = 1024,
  idleTtlMs,
  now = Date.now,
}: ProfileStoreOptions) {
  const origin = new URL(baseUrl);
  if (
    !path.isAbsolute(root) ||
    !["http:", "https:"].includes(origin.protocol) ||
    origin.username ||
    origin.password ||
    origin.pathname !== "/" ||
    origin.search ||
    origin.hash ||
    !Number.isSafeInteger(maxBytes + maxStagedBytes) ||
    [maxProfiles, maxBytes, maxStagedBytes, maxTransfers, maxGrants].some(
      (n) => !Number.isSafeInteger(n) || n < 1,
    ) ||
    (idleTtlMs !== undefined && (!Number.isSafeInteger(idleTtlMs) || idleTtlMs < 1))
  )
    throw Error("browser_profile_store_config_invalid");
  await fs.mkdir(root, { recursive: true, mode: 0o700 });
  const stat = await fs.lstat(root);
  if (!stat.isDirectory() || stat.uid !== process.getuid?.() || stat.mode & 0o077)
    throw Error("browser_profile_store_permissions");
  const indexFile = path.join(root, "index.json");
  let profiles: Record<string, StoredProfile> = {};
  let initialized = false;
  try {
    const handle = await fs.open(indexFile, constants.O_RDONLY | constants.O_NOFOLLOW);
    let data: any;
    try {
      const metadata = await handle.stat();
      if (
        !metadata.isFile() ||
        metadata.nlink !== 1 ||
        metadata.uid !== process.getuid?.() ||
        metadata.mode & 0o077 ||
        metadata.size > 16 * 1024 * 1024
      )
        throw invalid();
      data = JSON.parse(await handle.readFile("utf8"));
    } finally {
      await handle.close();
    }
    if (
      data?.schema !== schema ||
      !data.profiles ||
      typeof data.profiles !== "object" ||
      Array.isArray(data.profiles)
    )
      throw invalid();
    for (const [id, record] of Object.entries(data.profiles) as [
      string,
      StoredProfile,
    ][]) {
      if (
        !record ||
        !validProfileId(record.profileId) ||
        profileKey(record.profileId) !== id ||
        !validArchive(record.archive) ||
        !hex(record.key, 64) ||
        !Number.isSafeInteger(record.lastUsed) ||
        record.lastUsed < 0
      )
        throw invalid();
      const object = await fs.lstat(
        path.join(root, `${record.archive.objectKey}.archive`),
      );
      if (
        !object.isFile() ||
        object.size !== record.archive.byteSize ||
        object.mode & 0o077
      )
        throw invalid();
    }
    profiles = data.profiles;
    initialized = true;
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
    // A missing referenced object is corruption, never an empty store.
    if (
      await fs.lstat(indexFile).catch((failure) => {
        if (failure.code === "ENOENT") return null;
        throw failure;
      })
    )
      throw invalid();
  }
  let diskBytes = 0;
  const referenced = new Set(
    Object.values(profiles).map((record) => `${record.archive.objectKey}.archive`),
  );
  for (const entry of await fs.readdir(root, { withFileTypes: true })) {
    if (!/^[a-f0-9]{64}\.(archive|upload|index)$/.test(entry.name)) continue;
    const filename = path.join(root, entry.name);
    const item = await fs.lstat(filename);
    if (!initialized || !item.isFile() || item.nlink !== 1) throw invalid();
    if (referenced.has(entry.name)) diskBytes += item.size;
    // Unconfirmed upload keys existed only in the old process. Keep every indexed object.
    else await fs.unlink(filename);
  }
  const grants = new Map<string, Grant>();
  const sessions = new Map<string, AbortController>();
  const transfers = new Map<Promise<void>, Grant>();
  const deleting = new Set<string>();
  const life = new AbortController();
  let queue = Promise.resolve(),
    closed = false,
    poisoned = false;
  const check = (signal?: AbortSignal) => {
    signal?.throwIfAborted();
    if (closed || poisoned) throw Error("browser_profile_store_unavailable");
  };
  let pendingMetadata = 0;
  const serial = <T>(work: () => Promise<T>, cleanup = false): Promise<T> => {
    if (!cleanup && pendingMetadata >= maxGrants)
      return Promise.reject(Error("browser_profile_capacity_exceeded"));
    pendingMetadata++;
    const result = queue.then(work);
    queue = result.then(
      () => {},
      () => {},
    );
    return result.finally(() => {
      pendingMetadata--;
    });
  };
  async function removeObject(objectKey: string) {
    const filename = path.join(root, `${objectKey}.archive`);
    const bytes = await fs.stat(filename).catch((error) => {
      if (error.code === "ENOENT") return null;
      throw error;
    });
    if (!bytes) return;
    await fs.unlink(filename);
    diskBytes -= bytes.size;
  }
  async function syncDirectory() {
    const directory = await fs.open(root, "r");
    try {
      await directory.sync();
    } finally {
      await directory.close();
    }
  }
  async function publish(next: Record<string, StoredProfile>, signal?: AbortSignal) {
    check(signal);
    const temporary = path.join(root, `${randomBytes(32).toString("hex")}.index`);
    try {
      const handle = await fs.open(temporary, "wx", 0o600);
      try {
        const body = JSON.stringify({ schema, profiles: next });
        if (Buffer.byteLength(body) > 16 * 1024 * 1024)
          throw Error("browser_profile_capacity_exceeded");
        await handle.writeFile(body);
        await handle.sync();
      } finally {
        await handle.close();
      }
      await syncDirectory();
      check(signal);
      await fs.rename(temporary, indexFile);
      // A failed durability barrier is unknown state; stop admission until restart.
      try {
        await syncDirectory();
      } catch (error) {
        poisoned = true;
        throw error;
      }
      profiles = next;
    } finally {
      await fs.rm(temporary, { force: true });
    }
  }
  const grant = (value: Omit<Grant, "expiresAt">) => {
    check();
    for (const [token, item] of grants)
      if (item.expiresAt <= now() && !item.running && !item.uploaded)
        grants.delete(token);
    if (grants.size >= maxGrants) throw Error("browser_profile_capacity_exceeded");
    const token = randomBytes(32).toString("hex");
    grants.set(token, { ...value, expiresAt: now() + 300000 });
    return `${origin.origin}/profile-store/${token}`;
  };
  if (!initialized) await publish(profiles);
  return {
    open(profileId: string): StorageAdapter & { close(): Promise<void> } {
      check();
      if (!validProfileId(profileId)) throw Error("browser_profile_id_invalid");
      if (sessions.has(profileId) || deleting.has(profileId))
        throw Error("browser_profile_busy");
      if (sessions.size >= maxGrants) throw Error("browser_profile_capacity_exceeded");
      const objects = new Set<string>();
      const session = new AbortController(),
        id = profileKey(profileId);
      sessions.set(profileId, session);
      let closing: Promise<void> | undefined;
      const owned = (signal?: AbortSignal) => {
        check(signal);
        session.signal.throwIfAborted();
        if (sessions.get(profileId) !== session)
          throw Error("browser_profile_lease_lost");
      };
      return {
        async load(signal) {
          signal = AbortSignal.any([signal, session.signal]);
          owned(signal);
          const record = profiles[id];
          if (!record) return { seed: null };
          objects.add(record.archive.objectKey);
          return {
            seed: {
              ...record.archive,
              key: record.key,
              url: grant({
                method: "GET",
                objectKey: record.archive.objectKey,
                key: record.key,
                session,
              }),
            },
          };
        },
        async save(signal) {
          owned(signal);
          const key = randomBytes(32).toString("hex"),
            objectKey = randomBytes(32).toString("hex");
          objects.add(objectKey);
          return {
            target: {
              key,
              objectKey,
              url: grant({ method: "PUT", objectKey, key, session }),
            },
          };
        },
        async commit(result, signal) {
          signal = AbortSignal.any([signal, session.signal]);
          return serial(async () => {
            owned(signal);
            const archive = result?.archive as Record<string, any>;
            if (
              result?.used !== true ||
              !validArchive(archive) ||
              !(result.previousSha256 === null || hex(result.previousSha256, 64))
            )
              throw invalid();
            const current = profiles[id];
            if (current?.archive.sha256 === archive.sha256) {
              await publish(
                { ...profiles, [id]: { ...current, lastUsed: now() } },
                signal,
              );
              return { confirmedSha: archive.sha256 };
            }
            const upload = [...grants.values()].find(
              (item) =>
                item.session === session &&
                item.method === "PUT" &&
                item.objectKey === archive.objectKey &&
                item.uploaded,
            );
            if (
              !upload ||
              upload.uploaded.sha256 !== archive.sha256 ||
              upload.uploaded.byteSize !== archive.byteSize
            )
              throw invalid();
            if ((current?.archive.sha256 ?? null) !== result.previousSha256)
              throw Error("browser_profile_commit_conflict");
            const next = {
              ...profiles,
              [id]: {
                profileId,
                archive: {
                  schema: archive.schema,
                  browserVersion: archive.browserVersion,
                  iv: archive.iv,
                  tag: archive.tag,
                  sha256: archive.sha256,
                  byteSize: archive.byteSize,
                  objectKey: archive.objectKey,
                },
                key: upload.key,
                lastUsed: now(),
              },
            };
            if (
              Object.keys(next).length > maxProfiles ||
              Object.values(next).reduce(
                (sum, record) => sum + record.archive.byteSize,
                0,
              ) > maxBytes
            )
              throw Error("browser_profile_capacity_exceeded");
            await publish(next, signal);
            return { confirmedSha: archive.sha256 };
          });
        },
        close() {
          return (closing ??= (async () => {
            if (sessions.get(profileId) !== session) return;
            session.abort();
            for (const [token, item] of grants)
              if (item.session === session) {
                objects.add(item.objectKey);
                grants.delete(token);
              }
            await Promise.allSettled(
              [...transfers]
                .filter(([, item]) => item.session === session)
                .map(([work]) => work),
            );
            await serial(async () => {
              if (poisoned) return;
              for (const objectKey of objects) {
                if (
                  !Object.values(profiles).some(
                    (record) => record.archive.objectKey === objectKey,
                  )
                )
                  await removeObject(objectKey);
              }
            }, true);
            if (sessions.get(profileId) === session) sessions.delete(profileId);
          })().catch((error) => {
            closing = undefined;
            throw error;
          }));
        },
      };
    },
    async handle(req: IncomingMessage, res: ServerResponse): Promise<boolean> {
      const token = /^\/profile-store\/([a-f0-9]{64})$/.exec(req.url ?? "")?.[1];
      if (!token) return false;
      const item = grants.get(token);
      if (closed || poisoned || !item || item.expiresAt <= now()) {
        res.writeHead(403).end();
        return true;
      }
      if (req.method !== item.method) {
        res.writeHead(405).end();
        return true;
      }
      if (item.running || (item.method === "PUT" && item.uploaded)) {
        res.writeHead(409).end();
        return true;
      }
      if (transfers.size >= maxTransfers) {
        res.writeHead(429).end();
        return true;
      }
      const transferLife = new AbortController();
      const signal = AbortSignal.any([
        life.signal,
        item.session.signal,
        transferLife.signal,
        AbortSignal.timeout(120000),
      ]);
      const cancelled = () => {
        if (!res.writableFinished) transferLife.abort();
      };
      res.once("close", cancelled);
      item.running = true;
      const target = path.join(root, `${item.objectKey}.archive`);
      const temporary = path.join(root, `${item.objectKey}.upload`);
      const work = (async () => {
        let reservedBytes = 0;
        try {
          if (item.method === "GET") {
            const handle = await fs.open(
              target,
              constants.O_RDONLY | constants.O_NOFOLLOW,
            );
            try {
              res.writeHead(200, {
                "content-type": "application/octet-stream",
                "cache-control": "no-store",
              });
              await pipeline(handle.createReadStream(), res, { signal });
            } finally {
              await handle.close();
            }
          } else {
            const length = req.headers["content-length"];
            if (
              length !== undefined &&
              (!/^\d+$/.test(length) ||
                Number(length) < 1 ||
                Number(length) > MAX_ARCHIVE_BYTES)
            ) {
              res.writeHead(413).end();
              return;
            }
            let byteSize = 0;
            const hash = createHash("sha256");
            const bound = new Transform({
              transform(chunk, _, callback) {
                byteSize += chunk.length;
                if (
                  byteSize > MAX_ARCHIVE_BYTES ||
                  diskBytes + chunk.length > maxBytes + maxStagedBytes
                )
                  return callback(Error("browser_profile_capacity_exceeded"));
                diskBytes += chunk.length;
                reservedBytes += chunk.length;
                hash.update(chunk);
                callback(null, chunk);
              },
            });
            await pipeline(
              req,
              bound,
              createWriteStream(temporary, { flags: "wx", mode: 0o600 }),
              { signal },
            );
            if (!byteSize || (length !== undefined && byteSize !== Number(length)))
              throw invalid();
            const handle = await fs.open(temporary, "r+");
            try {
              await handle.sync();
            } finally {
              await handle.close();
            }
            signal.throwIfAborted();
            if (grants.get(token) !== item) throw Error("browser_profile_lease_lost");
            await fs.rename(temporary, target);
            item.uploaded = { byteSize, sha256: hash.digest("hex") };
            reservedBytes = 0;
            res.writeHead(200).end();
          }
        } catch {
          if (item.method === "GET" && !res.headersSent && !res.destroyed)
            res.writeHead(503).end();
          else res.destroy();
        } finally {
          res.off("close", cancelled);
          await fs.rm(temporary, { force: true });
          diskBytes -= reservedBytes;
          item.running = false;
        }
      })();
      transfers.set(work, item);
      try {
        await work;
      } finally {
        transfers.delete(work);
      }
      return true;
    },
    async delete(profileId: string, signal?: AbortSignal) {
      return serial(async () => {
        check(signal);
        if (!validProfileId(profileId)) throw Error("browser_profile_id_invalid");
        if (sessions.has(profileId) || deleting.has(profileId))
          throw Error("browser_profile_busy");
        const id = profileKey(profileId),
          record = profiles[id];
        if (!record) return false;
        deleting.add(profileId);
        try {
          const next = { ...profiles };
          delete next[id];
          await publish(next, signal);
          await removeObject(record.archive.objectKey);
          return true;
        } finally {
          deleting.delete(profileId);
        }
      });
    },
    async expire(
      signal?: AbortSignal,
      isLeased: (profileId: string) => boolean = () => false,
    ) {
      if (!idleTtlMs) return 0;
      let count = 0;
      for (const record of Object.values(profiles)) {
        if (
          !sessions.has(record.profileId) &&
          !isLeased(record.profileId) &&
          record.lastUsed + idleTtlMs <= now()
        ) {
          // Recheck activity under the same metadata queue as commit/delete.
          await serial(async () => {
            check(signal);
            const id = profileKey(record.profileId),
              current = profiles[id];
            if (
              !current ||
              sessions.has(record.profileId) ||
              isLeased(record.profileId) ||
              current.lastUsed + idleTtlMs > now()
            )
              return;
            deleting.add(record.profileId);
            try {
              const next = { ...profiles };
              delete next[id];
              await publish(next, signal);
              await removeObject(current.archive.objectKey);
              count++;
            } finally {
              deleting.delete(record.profileId);
            }
          });
        }
      }
      return count;
    },
    async close() {
      closed = true;
      life.abort();
      grants.clear();
      sessions.clear();
      await Promise.allSettled([...transfers.keys()]);
      await queue;
    },
  };
}
