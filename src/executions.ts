import { createHash, randomBytes } from "node:crypto";
import { channel } from "node:diagnostics_channel";
import { chmod, lstat, mkdir, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import type WebSocket from "ws";
import { requestArchive } from "./archive-channel.js";
import {
  BrowserCleanupUnconfirmed,
  VerifiedBrowserCheckpointFailure,
} from "./lifecycle-errors.js";
import {
  clearWindowState,
  createLocalProfiles,
  validProfileId,
} from "./local-profiles.js";
import { normalizeAllowedDomains } from "./network-policy.js";
import { createProfileArchive } from "./profiles.js";
import { BrowserSession } from "./session.js";
import { Takeover } from "./takeover.js";

interface BrowserHandle {
  endpoint: string;
  close(): Promise<void>;
  closed?: Promise<unknown>;
}
export interface ExecutionPeer {
  client: WebSocket;
  upstream: WebSocket | null;
  inflight: Set<number>;
  starting?: Promise<BrowserHandle>;
  cleanup(): void;
}
export interface Execution {
  executionId: string;
  uid: number;
  root: string;
  profileDir: string;
  filesDir: string;
  debugPort: number;
  profileId?: string;
  allowedDomains: string[];
  storageRequired: boolean;
  capability: string;
  peers: Set<ExecutionPeer>;
  session: BrowserSession;
  lifetime: AbortController;
  fileTransfers: Set<Promise<void>>;
  revoked: boolean;
  finishRequested?: boolean;
  browserRequested?: boolean;
  starting?: Promise<BrowserHandle>;
  closing?: Promise<void>;
  reusable?: Promise<void>;
  abort?: AbortController;
  idle?: ReturnType<typeof setTimeout>;
  idleAt?: number;
  journalDir?: string;
  profileLease?: Awaited<ReturnType<ReturnType<typeof createLocalProfiles>["acquire"]>>;
  profileUsed?: boolean;
  profileRestored?: boolean;
  finalizing?: boolean;
  archive?: any;
  finalized?: any;
  takeover?: Takeover;
}

const preparationTiming = channel("browser-sandbox.preparation");

const MAX_REVOKED_EXECUTIONS = 65536;
const uuid = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/;
const fail = (code) => {
  throw Error(code);
};

export async function createExecutions({
  launch,
  dataRoot,
  maxBrowsers,
  getPort,
  profileStore = createLocalProfiles({ root: join(dataRoot, ".profiles") }),
  profileArchive = createProfileArchive(),
  startupTimeoutMs,
  maxSessionMs,
}) {
  if (!Number.isSafeInteger(maxBrowsers) || maxBrowsers < 1)
    fail("browser_capacity_invalid");
  await mkdir(dataRoot, { recursive: true, mode: 0o755 });
  await chmod(dataRoot, 0o755);
  const executions = new Map<string, Execution>(),
    revoked = new Set(),
    reservedPorts = new Set();
  const mutations = new Map<string, Promise<void>>();
  let closing = false;
  if (
    !Number.isSafeInteger(maxSessionMs) ||
    maxSessionMs < 1 ||
    maxSessionMs > 86_400_000
  )
    fail("browser_session_duration_invalid");
  async function register({
    executionId,
    executorUid = undefined,
    profileId = undefined,
    archive = undefined,
    storage = undefined,
    allowedDomains = undefined,
  }) {
    const uid = executorUid ?? 20001;
    const domains = normalizeAllowedDomains(allowedDomains);
    if (
      (executorUid !== undefined || profileId !== undefined) &&
      (!Number.isInteger(uid) || uid < 100000 || uid >= 2000000000)
    )
      fail("browser_executor_identity_invalid");
    if (!uuid.test(executionId ?? "")) fail("browser_identity_invalid");
    if (profileId !== undefined && !validProfileId(profileId))
      fail("browser_profile_binding_invalid");
    if (
      storage !== undefined &&
      (storage !== true || !profileId || archive !== undefined)
    )
      fail("browser_profile_binding_invalid");
    if (revoked.has(executionId)) fail("browser_execution_revoked");
    let entry = executions.get(executionId);
    if (entry?.finishRequested) fail("browser_execution_revoked");
    if (!entry && revoked.size >= MAX_REVOKED_EXECUTIONS)
      fail("browser_execution_capacity_exceeded");
    if (entry && entry.profileId !== profileId) fail("browser_profile_binding_changed");
    if (entry && storage !== undefined && !entry.storageRequired)
      fail("browser_profile_binding_changed");
    if (entry && JSON.stringify(entry.allowedDomains) !== JSON.stringify(domains))
      fail("browser_domains_changed");
    if (entry && entry.uid !== uid) fail("browser_identity_mismatch");
    if (entry && !entry.profileUsed && !entry.starting && archive !== undefined)
      entry.archive = archive;
    if (entry?.finalizing) fail("browser_profile_finalizing");
    if (entry?.revoked) {
      // Explicit registration can resume a failed quiesce only after retirement.
      // A finish or shutdown arriving during this wait remains authoritative.
      await entry.reusable;
      if (
        closing ||
        entry.finishRequested ||
        revoked.has(executionId) ||
        executions.get(executionId) !== entry
      )
        fail("browser_execution_revoked");
      entry.revoked = false;
      entry.lifetime = new AbortController();
    }
    if (!entry) {
      const root = join(dataRoot, executionId);
      if (
        await lstat(join(root, "owner.json")).catch((error) => {
          if (error.code === "ENOENT") return null;
          throw error;
        })
      )
        fail("browser_execution_recovery_required");
      let debugPort: number;
      for (let port = 42000; port < 43000; port++)
        if (
          !reservedPorts.has(port) &&
          ![...executions.values()].some((item) => item.debugPort === port)
        ) {
          debugPort = port;
          break;
        }
      if (!debugPort) fail("browser_capacity_invalid");
      reservedPorts.add(debugPort);
      try {
        await mkdir(root, { recursive: true, mode: 0o711 });
        const profileDir = join(root, "profile");
        await mkdir(join(root, "files"), { mode: 0o700 });
        await writeFile(
          join(root, "owner.json"),
          JSON.stringify({ uid, profileDir, debugPort }),
          { mode: 0o600 },
        );
        entry = {
          executionId,
          allowedDomains: domains,
          uid,
          root,
          profileDir,
          filesDir: join(root, "files"),
          debugPort,
          profileId,
          archive,
          storageRequired: storage === true,
          capability: randomBytes(32).toString("hex"),
          peers: new Set(),
          session: new BrowserSession(() => {
            if (
              !entry.peers.size &&
              (!entry.takeover || entry.takeover.state === "automation")
            )
              retire(entry);
          }, maxSessionMs),
          lifetime: new AbortController(),
          fileTransfers: new Set(),
          revoked: false,
        };
        executions.set(executionId, entry);
      } finally {
        reservedPorts.delete(debugPort);
      }
    }
    return {
      protocol: "browser-sandbox.host/v1",
      executorUid: entry.uid,
      ...(entry.profileId ? { profileId: entry.profileId } : {}),
      ...(entry.storageRequired ? { storage: true } : {}),
      wsEndpoint: `ws://127.0.0.1:${getPort()}/execution/${entry.capability}`,
    };
  }
  // Invalidate by launch identity: late events from an old browser must never
  // clear a newer launch, and profile reuse must wait for verified shutdown.
  function retire(entry: Execution, starting = entry.starting) {
    if (!starting || entry.starting !== starting) return entry.closing;
    clearTimeout(entry.idle);
    entry.idle = null;
    entry.session.ended();
    entry.starting = null;
    entry.takeover = null;
    entry.abort?.abort();
    // A cancelled successor has not replaced the previous failed checkpoint.
    const previousClosing = entry.closing;
    entry.closing = Promise.resolve(entry.reusable)
      .then(() =>
        starting.catch((error) => {
          if (
            error instanceof BrowserCleanupUnconfirmed ||
            error instanceof VerifiedBrowserCheckpointFailure
          )
            throw error;
          return null; // Ordinary launch rejection guarantees no surviving process.
        }),
      )
      .then((browser) => (browser ? browser.close() : previousClosing));
    entry.closing.catch(() => {});
    // Keep the original failed confirmation for quiesce/finish, independently
    // of the proof needed by a new launch or capacity reclamation.
    entry.reusable = entry.closing.catch((error) => {
      if (!(error instanceof VerifiedBrowserCheckpointFailure)) throw error;
    });
    entry.reusable.catch(() => {});
    for (const pair of entry.peers) if (pair.starting === starting) pair.cleanup();
    return entry.closing;
  }
  function reclaimIdle(exclude: Execution = undefined) {
    // Count pending launches too. This also handles an active request already
    // queued when the last client of a slot-holding browser disconnects.
    const claimed = [...executions.values()].filter((entry) => entry.starting);
    let excess = claimed.length - maxBrowsers;
    const idle = claimed
      .filter(
        (entry) =>
          entry !== exclude &&
          !entry.peers.size &&
          !entry.session.retained &&
          !entry.revoked &&
          (!entry.takeover || entry.takeover.state === "automation"),
      )
      .sort((a, b) => a.idleAt - b.idleAt);
    const released = [];
    for (const entry of idle) {
      if (excess-- <= 0) break;
      retire(entry);
      released.push(entry.reusable);
    }
    return Promise.all(released);
  }
  async function close(entry: Execution, removeProfile: boolean) {
    entry.revoked = true;
    entry.lifetime?.abort();
    clearTimeout(entry.idle);
    retire(entry);
    for (const pair of entry.peers) pair.cleanup();
    await entry.closing;
    await Promise.allSettled(entry.fileTransfers);
    entry.lifetime?.abort();
    if (removeProfile) {
      await entry.profileLease?.release();
      entry.profileLease = null;
      await rm(entry.root, { recursive: true, force: true });
    }
  }
  async function command(request, host = false, fromSocket = false, signal) {
    if (closing) fail("browser_stopping");
    if (signal?.aborted) fail("browser_request_cancelled");
    if (request.op === "session") {
      if (!host) fail("browser_host_authority_required");
      const entry = executions.get(request.executionId);
      if (!entry || entry.revoked) fail("browser_execution_revoked");
      if (request.action === "stop") {
        if (entry.takeover && entry.takeover.state !== "automation")
          fail("browser_control_busy");
        retire(entry);
        await entry.closing;
        entry.session.ended("stopped");
      } else {
        entry.session.control(request.action, request.durationMs);
        if (entry.session.retained) {
          clearTimeout(entry.idle);
          entry.idle = null;
        } else if (request.action === "release") disconnected(entry);
      }
      return entry.session.status(entry.peers.size);
    }
    if (request.op === "takeover") {
      if (!host) fail("browser_host_authority_required");
      const entry = executions.get(request.executionId);
      if (!entry?.starting || entry.revoked) fail("browser_page_unavailable");
      if (!entry.takeover) {
        const browser = await entry.starting;
        entry.takeover = new Takeover(browser.endpoint, async () => {
          const deadline = Date.now() + 10_000;
          while ([...entry.peers].some((peer: any) => peer.inflight.size)) {
            if (Date.now() > deadline) fail("browser_stop_uncertain");
            await delay(10);
          }
          for (const peer of entry.peers) peer.cleanup();
        });
      }
      clearTimeout(entry.idle);
      entry.idle = null;
      try {
        return await entry.takeover.command(request.action, signal);
      } finally {
        if (entry.takeover?.state === "automation") disconnected(entry);
      }
    }
    if (request.op === "storage_attach") {
      if (!host || !uuid.test(request.executionId ?? ""))
        fail("browser_host_authority_required");
      const entry = executions.get(request.executionId);
      if (entry) {
        if (entry.uid !== request.executorUid) fail("browser_profile_binding_changed");
        if (request.existingOnly && !entry.profileId && !entry.storageRequired)
          return { status: "temporary" };
        if (!entry.storageRequired || entry.profileId !== request.profileId)
          fail("browser_profile_binding_changed");
      } else if (request.existingOnly) return { status: "absent" };
      else await register({ ...request, storage: true });
      return { attached: true };
    }
    if (request.op === "finalize") {
      if (!host) fail("browser_host_authority_required");
      const entry = executions.get(request.executionId);
      if (!entry) return { status: "absent" };
      let result: any = { used: !!entry.browserRequested };
      if (entry.profileId && entry.profileUsed && entry.storageRequired) {
        result = await command(
          { ...request, op: "profile_finalize" },
          host,
          fromSocket,
          signal,
        );
        const confirmation: any = await requestArchive(entry, "commit", signal, result);
        if (confirmation.confirmedSha !== result.archive.sha256)
          fail("browser_profile_save_unconfirmed");
        request = { ...request, confirmedSha: confirmation.confirmedSha };
      }
      await command({ ...request, op: "finish" }, host, fromSocket, signal);
      return { ...result, stopped: true };
    }
    if (request.op === "profile_finalize") {
      if (!host || !uuid.test(request.executionId ?? ""))
        fail("browser_host_authority_required");
      const entry = executions.get(request.executionId);
      if (!entry) return { status: "absent" };
      if (!entry.profileId || !entry.profileUsed) return { used: false };
      entry.finalizing = true;
      await close(entry, false);
      if (signal?.aborted) fail("browser_request_cancelled");
      if (!entry.finalized) {
        const archive = entry.storageRequired
          ? await requestArchive(entry, "save", signal)
          : (request.archive ?? entry.archive);
        entry.finalized = await profileArchive.save(entry, archive, signal);
      }
      return entry.finalized;
    }
    if (request.op === "endpoint") {
      if (!host) fail("browser_host_authority_required");
      const entry = executions.get(request.executionId);
      if (!entry) return { wsEndpoint: null };
      if (entry.revoked) fail("browser_execution_revoked");
      if (entry.profileId !== request.profileId)
        fail("browser_profile_binding_changed");
      if (!entry.profileUsed && !entry.starting && request.archive !== undefined)
        entry.archive = request.archive;
      return {
        protocol: "browser-sandbox.host/v1",
        executorUid: entry.uid,
        ...(entry.profileId ? { profileId: entry.profileId } : {}),
        ...(entry.storageRequired ? { storage: true } : {}),
        wsEndpoint: `ws://127.0.0.1:${getPort()}/execution/${entry.capability}`,
      };
    }
    if (request.op === "register") {
      if (request.profileId !== undefined && !host)
        fail("browser_host_authority_required");
      if (
        fromSocket &&
        !host &&
        (!Number.isInteger(request.executorUid) ||
          request.executorUid < 100000 ||
          request.executorUid >= 2000000000)
      )
        fail("browser_executor_identity_invalid");
      return register(request);
    }
    if (
      !uuid.test(request.executionId ?? "") ||
      !["finish", "quiesce"].includes(request.op)
    )
      fail("browser_request_invalid");
    const entry = executions.get(request.executionId);
    if (entry?.profileId && !host) fail("browser_host_authority_required");
    if (
      request.op === "finish" &&
      (entry?.archive || entry?.storageRequired) &&
      entry.profileUsed &&
      (!entry.finalized ||
        typeof request.confirmedSha !== "string" ||
        request.confirmedSha !== entry.finalized.archive.sha256)
    )
      fail("browser_profile_save_unconfirmed");
    if (request.op === "quiesce" && entry?.finishRequested)
      fail("browser_execution_revoked");
    if (request.op === "finish") {
      if (revoked.size < MAX_REVOKED_EXECUTIONS) revoked.add(request.executionId);
      // At capacity, a failed cleanup still needs a permanent fence on its live entry.
      if (entry) entry.finishRequested = true;
    }
    if (entry) {
      await close(entry, request.op === "finish");
      if (request.op === "finish") executions.delete(request.executionId);
      else {
        if (entry.finishRequested) fail("browser_execution_revoked");
        entry.starting = null;
        entry.revoked = false;
        entry.lifetime = new AbortController();
      }
    }
    return {
      stopped: true,
      ...(request.op === "finish" ? { credentialsScrubbed: !entry?.profileId } : {}),
    };
  }
  function ensureStarted(entry: Execution) {
    if (!entry.starting) {
      entry.session.starting();
      let hardExpiresAt: number;
      const phases = { profileAcquireMs: 0, profileRestoreMs: 0, browserLaunchMs: 0 };
      const abort = (entry.abort = new AbortController());
      let acquiredLease: Awaited<ReturnType<typeof profileStore.acquire>>;
      const starting = Promise.resolve(entry.reusable)
        .then(() => reclaimIdle(entry))
        .then(async () => {
          if (abort.signal.aborted) fail("browser_launch_cancelled");
          if (entry.profileId) {
            if (!entry.profileLease) {
              const start = performance.now();
              entry.profileLease = await profileStore.acquire(
                entry.profileId,
                entry.executionId,
                entry.uid,
              );
              phases.profileAcquireMs = performance.now() - start;
              acquiredLease = entry.profileLease;
            }
            entry.profileDir = entry.profileLease.profileDir;
            entry.journalDir = entry.profileLease.journalDir;
            if ((entry.archive || entry.storageRequired) && !entry.profileRestored) {
              const start = performance.now();
              const archive = entry.storageRequired
                ? await requestArchive(entry, "load", abort.signal)
                : entry.archive;
              await profileArchive.restore(entry, archive, abort.signal);
              phases.profileRestoreMs = performance.now() - start;
              entry.profileRestored = true;
            }
            entry.profileUsed = true;
            if (abort.signal.aborted || entry.revoked) fail("browser_launch_cancelled");
            await clearWindowState(entry.profileDir, abort.signal);
            await writeFile(
              join(entry.root, "owner.json"),
              JSON.stringify({
                uid: entry.uid,
                profileDir: entry.profileDir,
                debugPort: entry.debugPort,
              }),
              { mode: 0o600 },
            );
          }
          if (abort.signal.aborted || entry.revoked) fail("browser_launch_cancelled");
          hardExpiresAt = Date.now() + maxSessionMs;
          const start = performance.now();
          const browser = await launch({
            ...entry,
            signal: abort.signal,
            sessionBoundary: !!entry.profileId,
            fingerprint: String(
              parseInt(
                createHash("sha256")
                  .update(entry.profileId ?? entry.executionId)
                  .digest("hex")
                  .slice(0, 8),
                16,
              ),
            ),
          });
          phases.browserLaunchMs = performance.now() - start;
          return browser;
        })
        .then((browser) => {
          if (entry.starting === starting && !entry.revoked && !abort.signal.aborted) {
            entry.session.ready(hardExpiresAt);
            if (preparationTiming.hasSubscribers)
              preparationTiming.publish({ executionId: entry.executionId, ...phases });
          }
          // Watch Browserless/Chrome even while no MCP client is attached.
          browser.closed?.then(() => retire(entry, starting)).catch(() => {});
          return browser;
        })
        .catch(async (error) => {
          if (
            acquiredLease &&
            !(error instanceof BrowserCleanupUnconfirmed) &&
            !(error instanceof VerifiedBrowserCheckpointFailure)
          ) {
            try {
              await acquiredLease.release();
              if (entry.profileLease === acquiredLease) {
                entry.profileLease = null;
                entry.profileUsed = false;
                entry.profileRestored = false;
              }
            } catch (cleanupError) {
              error = new BrowserCleanupUnconfirmed(cleanupError);
            }
          }
          if (entry.starting === starting) {
            entry.starting = null;
            entry.session.ended();
            if (
              error instanceof BrowserCleanupUnconfirmed ||
              error instanceof VerifiedBrowserCheckpointFailure
            ) {
              // The initial handshake can fail before a browser handle/watch is
              // installed. Preserve its checkpoint failure just like retirement.
              entry.closing = Promise.reject(error);
              entry.closing.catch(() => {});
              entry.reusable = entry.closing.catch((error) => {
                if (!(error instanceof VerifiedBrowserCheckpointFailure)) throw error;
              });
              entry.reusable.catch(() => {});
            }
          }
          throw error;
        });
      entry.starting = starting;
      const startupTimer = setTimeout(() => {
        abort.abort();
        for (const peer of entry.peers)
          if (peer.starting === starting)
            peer.client.close(1011, "browser_profile_startup_timeout");
      }, startupTimeoutMs);
      starting.then(
        () => clearTimeout(startupTimer),
        () => clearTimeout(startupTimer),
      );
    }
    return entry.starting;
  }
  function disconnected(entry: Execution) {
    if (
      entry.peers.size ||
      entry.revoked ||
      !entry.starting ||
      entry.session.retained ||
      (entry.takeover && entry.takeover.state !== "automation")
    )
      return;
    entry.idleAt = Date.now();
    entry.idle ??= setTimeout(() => {
      entry.idle = null;
      if (
        !entry.peers.size &&
        !entry.revoked &&
        !entry.session.retained &&
        (!entry.takeover || entry.takeover.state === "automation")
      )
        retire(entry);
    }, 1000);
    reclaimIdle().catch(() => {});
  }
  return {
    command,
    disconnected,
    ensureStarted,
    dispatch(request, signal) {
      const id = request.executionId;
      const work = (mutations.get(id) ?? Promise.resolve()).then(() =>
        command(request, true, true, signal),
      );
      const settled = work
        .catch(() => {})
        .finally(() => {
          if (mutations.get(id) === settled) mutations.delete(id);
        });
      mutations.set(id, settled);
      return work;
    },
    async drain() {
      await Promise.all(mutations.values());
    },
    retire,
    reclaimIdle,
    get: (id) => executions.get(id),
    resolve: (capability) =>
      [...executions.values()].find(
        (entry) => entry.capability === capability && !entry.revoked,
      ),
    get closing() {
      return closing;
    },
    beginShutdown() {
      closing = true;
    },
    async stop() {
      const cleanup = await Promise.allSettled(
        [...executions.values()].map(async (entry) => {
          try {
            if ((entry.archive || entry.storageRequired) && entry.profileUsed) {
              await close(entry, false);
              fail("browser_profile_save_unconfirmed");
            }
            await close(entry, true);
          } catch (error) {
            await entry.profileLease?.abandon();
            throw error;
          }
        }),
      );
      const failures = cleanup.filter((result) => result.status === "rejected");
      if (failures.length)
        throw new AggregateError(
          failures.map((result) => result.reason),
          "browser_lifecycle_failed",
        );
    },
  };
}
