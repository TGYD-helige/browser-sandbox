import { randomBytes, randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createServer, request as httpRequest } from "node:http";
import type { AddressInfo } from "node:net";
import { pipeline, Transform } from "node:stream";
import WebSocket, { createWebSocketStream, WebSocketServer } from "ws";
import { waitForControl } from "./automation-control.js";
import { createExecutionJournal } from "./execution-journal.js";
import { MAX_FILE_BYTES } from "./files.js";
import type { StorageAdapter } from "./host-client.js";
import { WorkerCleanupFailure } from "./lifecycle-errors.js";
import { validProfileId } from "./local-profiles.js";
import type { ManagedBackend, ManagedWorker } from "./managed-backend.js";
import {
  CONTROL_TIMEOUT_MS,
  type ControlPeer,
  createNetworkHostControl,
} from "./network-control.js";
import { normalizeAllowedDomains } from "./network-policy.js";
import type { createProfileStore } from "./profile-store.js";
import type { SessionStatus } from "./session.js";
import { createWorkerAdmission, type WorkerSlot } from "./worker-admission.js";
import { WORKER_PROTOCOL, workerIdleProtection } from "./worker-protocol.js";

export interface ExecutionBinding {
  executionId: string;
  incarnation: string;
  allowedDomains: string[];
  profileId?: string;
  storageMode?: "host" | "volume";
  storage?: StorageAdapter;
  storagePeer?: ControlPeer;
  storageDisconnect?: () => void;
  storageConnection?: { close(): void };
  volumeAdapter?: StorageAdapter & { close(): Promise<void> };
  worker?: ManagedWorker;
  starting?: Promise<ManagedWorker>;
  endpoint?: { cdpUrl: string; viewerToken: string };
  life: AbortController;
  revoked: boolean;
  host?: ControlPeer;
  registeredAt: number;
  lastActivityAt: number;
  phase: string;
  revision: number;
  persisted?: Promise<void>;
  slot?: WorkerSlot;
  creation?: { abort: AbortController; waiters: number };
  creationFailure?: unknown;
  stopConfirmed?: boolean;
  finalResult?: any;
  recoveryError?: string;
  recoveryAttempts?: number;
  recovered?: boolean;
  generation?: string;
  cleanupUnconfirmed?: boolean;
  infrastructureOnly?: boolean;
  checkedAt?: number;
  idleProtection?: string;
  statusError?: string;
  reclaimReason?: "idle" | "maximum-lifetime" | "orphan" | "host-finish";
}
interface Access {
  token: string;
  binding: ExecutionBinding;
  kind: "cdp" | "view";
  control: boolean;
  resize: boolean;
  expiresAt?: number;
  life: AbortController;
  timer?: ReturnType<typeof setTimeout>;
  connections: Set<{ destroy(): void }>;
}
export interface ControllerOptions {
  backend: ManagedBackend;
  /** Retained, private storage; exactly one controller may own this directory. */
  profileStore?: Awaited<ReturnType<typeof createProfileStore>>;
  stateDirectory?: string;
  installationId?: string;
  port?: number;
  host?: string;
  controlPort?: number;
  controlHost?: string;
  publicOrigin?: string;
  viewerOrigins?: string[];
  creationTimeoutMs?: number;
  maxWorkers?: number;
  maxPending?: number;
  maxQueued?: number;
  maxExecutionMs?: number;
  idleRegistrationMs?: number;
  reconciliationMs?: number;
  onWorkerEvent?: (event: {
    type: "creating" | "ready" | "failed" | "removed" | "recovery-blocked";
    executionId: string;
    incarnation: string;
    elapsedMs?: number;
    error?: string;
  }) => void;
  authenticateHost?: Parameters<typeof createNetworkHostControl>[0]["authenticateHost"];
  /** Reserved for future agent identity validation; every protected route crosses this seam. */
  authorizeAccess?: (
    executionId: string,
    request: import("node:http").IncomingMessage,
  ) => Promise<void>;
}

/** Single-controller temporary executions; infrastructure and worker addresses stay behind this interface. */
export async function createController(options: ControllerOptions) {
  const bindings = new Map<string, ExecutionBinding>();
  const grants = new Map<string, Access>();
  const storageAttachments = new Set<ExecutionBinding>();
  const finished = new Set<string>();
  const rememberFinished = (id: string) => {
    finished.add(id);
    if (finished.size > 65536) finished.delete(finished.values().next().value);
  };
  let journal: Awaited<ReturnType<typeof createExecutionJournal>> | undefined;
  const capacityLimits = {
    maxWorkers: options.maxWorkers ?? 8,
    maxPending: options.maxPending ?? 2,
    maxQueued: options.maxQueued ?? 32,
    creationTimeoutMs: options.creationTimeoutMs ?? 90000,
  };
  const admission = createWorkerAdmission(capacityLimits);
  const maxExecutionMs = options.maxExecutionMs ?? 3600000;
  const idleRegistrationMs = options.idleRegistrationMs ?? 900000;
  const reconciliationMs = options.reconciliationMs ?? 1000;
  for (const duration of [maxExecutionMs, idleRegistrationMs, reconciliationMs])
    if (!Number.isSafeInteger(duration) || duration < 1 || duration > 2147483647)
      throw Error("browser_controller_deadline_invalid");
  const profileLeases = new Map<string, { executionId: string; incarnation: string }>();
  let recoveredProfiles = false;
  let recovering: Promise<void> | undefined;
  const localLease = (lease: { executionId: string; incarnation: string }) => {
    const binding = bindings.get(lease.executionId);
    return (
      binding?.incarnation === lease.incarnation &&
      (!binding.recovered || binding.phase === "registered")
    );
  };
  const recoverProfiles = (profileId?: string) => {
    const leases = profileId
      ? [profileLeases.get(profileId)].filter(Boolean)
      : [...profileLeases.values()];
    if (recoveredProfiles && !leases.some((lease) => !localLease(lease)))
      return Promise.resolve();
    return (recovering ??= (async () => {
      if (!options.backend.list) throw Error("browser_profile_leases_unavailable");
      const workers = await options.backend.list(AbortSignal.timeout(30000));
      const recovered = new Map<string, ManagedWorker>();
      for (const worker of workers) {
        if (
          !worker.profileId ||
          worker.infrastructureOnly ||
          finished.has(worker.executionId)
        )
          continue;
        if (!validProfileId(worker.profileId) || recovered.has(worker.profileId))
          throw Error("browser_profile_lease_recovery_failed");
        recovered.set(worker.profileId, worker);
      }
      for (const [id, worker] of recovered) {
        const lease = profileLeases.get(id);
        if (lease && localLease(lease) && lease.incarnation !== worker.incarnation)
          throw Error("browser_profile_lease_recovery_failed");
      }
      for (const [id, lease] of profileLeases) {
        const binding = bindings.get(lease.executionId);
        if (!localLease(lease) && binding?.recovered && !recovered.has(id)) {
          adoptRecoveredWorker(binding, undefined);
          binding.stopConfirmed = true;
          binding.cleanupUnconfirmed = false;
          binding.finalResult ??= { stopped: true, used: true, graceful: false };
          binding.phase = "cleanup-pending";
          await command({ op: "finalize", executionId: binding.executionId });
        }
      }
      // Only a successful authoritative read can release recovered occupancy.
      for (const [id, lease] of profileLeases)
        if (!localLease(lease)) profileLeases.delete(id);
      for (const [id, worker] of recovered) {
        const lease = profileLeases.get(id);
        if (!lease) profileLeases.set(id, worker);
      }
      recoveredProfiles = true;
    })().finally(() => {
      recovering = undefined;
    }));
  };
  let closing = false;
  let stopped = false;
  let origin = options.publicOrigin;
  if (
    origin &&
    (!/^https?:$/.test(new URL(origin).protocol) || new URL(origin).origin !== origin)
  )
    throw Error("browser_access_origin_invalid");
  if (
    !origin &&
    options.host &&
    !["127.0.0.1", "localhost", "::1"].includes(options.host)
  )
    throw Error("browser_access_origin_required");
  if ((options.viewerOrigins?.length ?? 0) > 8)
    throw Error("browser_access_origin_invalid");
  for (const value of options.viewerOrigins ?? []) {
    const url = new URL(value);
    if (url.origin !== value || !["http:", "https:"].includes(url.protocol))
      throw Error("browser_access_origin_invalid");
  }
  function persist(
    binding: ExecutionBinding,
    mode: "lifecycle" | "recovery-attempt" = "lifecycle",
  ) {
    if (!journal) return Promise.resolve();
    const data = {
      profileId: binding.profileId,
      storageMode: binding.storageMode,
      infrastructureOnly: binding.infrastructureOnly,
      incarnation: binding.incarnation,
      allowedDomains: binding.allowedDomains,
      registeredAt: binding.registeredAt,
      lastActivityAt: binding.lastActivityAt,
      phase: binding.phase,
      stopConfirmed: binding.stopConfirmed,
      finalResult: binding.finalResult
        ? {
            stopped: binding.finalResult.stopped,
            ...(typeof binding.finalResult.used === "boolean"
              ? { used: binding.finalResult.used }
              : {}),
            ...(typeof binding.finalResult.graceful === "boolean"
              ? { graceful: binding.finalResult.graceful }
              : {}),
            ...(binding.finalResult.archive
              ? {
                  archive: {
                    sha256: binding.finalResult.archive.sha256,
                    byteSize: binding.finalResult.archive.byteSize,
                  },
                }
              : {}),
          }
        : undefined,
      generation: binding.generation,
      cleanupUnconfirmed: binding.cleanupUnconfirmed,
      recoveryAttempts: binding.recoveryAttempts,
      recoveryError: binding.recoveryError,
      reclaimReason: binding.reclaimReason,
      endpoint: binding.endpoint,
      workerId: binding.worker?.id,
      grants: [...grants.values()]
        .filter((grant) => grant.binding === binding)
        .map((grant) => ({
          token: grant.token,
          kind: grant.kind,
          control: grant.control,
          resize: grant.resize,
          expiresAt: grant.expiresAt,
        })),
    };
    const write = async () => {
      // Unknown compatibility must retain the last published binding for rollback.
      const recordData =
        mode === "recovery-attempt" && binding.revision
          ? {
              ...(await journal.read(binding.executionId)).data,
              recoveryAttempts: data.recoveryAttempts,
            }
          : data;
      const record = binding.revision
        ? await journal.update(binding.executionId, binding.revision, recordData)
        : await journal.create(binding.executionId, recordData);
      binding.revision = record.revision;
    };
    binding.persisted = (binding.persisted ?? Promise.resolve()).then(write);
    return binding.persisted;
  }
  const assets = new Map([
    ["/view", ["index.html", "text/html"]],
    ["/viewer.js", ["browser-viewer.js", "text/javascript"]],
    ["/viewer.css", ["viewer.css", "text/css"]],
  ]);
  const server = createServer(async (req, res) => {
    const life = new AbortController();
    res.once("close", () => {
      if (!res.writableEnded) life.abort();
    });
    try {
      res.setHeader("cache-control", "no-store");
      res.setHeader("referrer-policy", "no-referrer");
      res.setHeader("x-content-type-options", "nosniff");
      res.setHeader(
        "content-security-policy",
        `default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' blob: data:; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'self' ${(options.viewerOrigins ?? []).join(" ")}`,
      );
      if (req.url === "/health") {
        res.writeHead(closing ? 503 : 204).end();
        return;
      }
      if (!allowedOrigin(req)) {
        res.writeHead(403).end();
        return;
      }
      const asset = assets.get(req.url);
      if (asset && req.method === "GET") {
        res.setHeader("content-type", asset[1]);
        res.end(await readFile(new URL(`./viewer/${asset[0]}`, import.meta.url)));
        return;
      }
      const match =
        /^\/execution\/([a-f0-9]{64})(\/(?:session|auth|files\/[^/?]+))$/.exec(
          req.url ?? "",
        );
      const grant = match && (await access(match[1], "cdp", req));
      if (!grant) {
        res.writeHead(403).end();
        return;
      }
      const signal = AbortSignal.any([
        life.signal,
        grant.life.signal,
        AbortSignal.timeout(match[2] === "/auth" ? 620000 : 180000),
      ]);
      if (match[2] === "/session") {
        if (!["GET", "POST"].includes(req.method)) {
          res.writeHead(405).end();
          return;
        }
        let input = "";
        for await (const chunk of req) {
          input += chunk;
          if (Buffer.byteLength(input) > 8192) {
            res.writeHead(413).end();
            return;
          }
        }
        let action: { action?: string; durationMs?: number };
        try {
          action = req.method === "POST" ? JSON.parse(input) : { action: "status" };
        } catch {
          res.writeHead(400).end(JSON.stringify({ error: "browser_request_invalid" }));
          return;
        }
        if (!action || typeof action !== "object" || Array.isArray(action)) {
          res.writeHead(400).end(JSON.stringify({ error: "browser_request_invalid" }));
          return;
        }
        const result = await command(
          {
            op: "session",
            executionId: grant.binding.executionId,
            action: action.action,
            durationMs: action.durationMs,
          },
          undefined,
          signal,
        );
        res
          .writeHead(200, { "content-type": "application/json" })
          .end(JSON.stringify(result));
        return;
      }
      const fileRoute = match[2].startsWith("/files/");
      if (fileRoute && Number(req.headers["content-length"]) > MAX_FILE_BYTES) {
        res.writeHead(413, { connection: "close" }).end();
        return;
      }
      await ensureWorker(grant.binding, signal);
      signal.throwIfAborted();
      const target = new URL(grant.binding.endpoint.cdpUrl);
      target.protocol = "http:";
      target.pathname += match[2];
      const headers = { ...req.headers, host: target.host };
      delete headers.authorization;
      delete headers.cookie;
      delete headers.origin;
      const upstream = httpRequest(
        target,
        { method: req.method, headers, signal },
        (response) => {
          if (res.writableEnded || res.destroyed) {
            response.destroy();
            return;
          }
          res.writeHead(response.statusCode, response.headers);
          pipeline(response, res, () => {});
        },
      );
      upstream.on("error", () => {
        if (res.writableEnded) return;
        if (!res.headersSent)
          res
            .writeHead(502)
            .end(JSON.stringify({ error: "browser_worker_unavailable" }));
        else res.destroy();
      });
      if (fileRoute) {
        let bytes = 0,
          exceeded = false;
        const limit = new Transform({
          transform(chunk, _encoding, done) {
            bytes += chunk.length;
            if (bytes > MAX_FILE_BYTES) {
              exceeded = true;
              req.unpipe(limit);
              req.resume();
              if (!res.headersSent) res.writeHead(413, { connection: "close" }).end();
              else res.destroy();
              done(Error("browser_file_too_large"));
              return;
            }
            done(undefined, chunk);
          },
        });
        req.once("error", (error) => limit.destroy(error));
        res.once("close", () => {
          req.unpipe(limit);
          limit.destroy();
        });
        req.pipe(limit);
        pipeline(limit, upstream, (error) => {
          if (error && !exceeded) req.destroy();
        });
      } else pipeline(req, upstream, () => {});
    } catch (error) {
      if (!res.headersSent)
        res.writeHead(502).end(JSON.stringify({ error: errorCode(error) }));
      else res.destroy();
    }
  });
  const allowedOrigin = (req) =>
    !req.headers.origin ||
    req.headers.origin === origin ||
    (options.viewerOrigins ?? []).includes(req.headers.origin);
  function revoke(grant: Access) {
    grants.delete(grant.token);
    clearTimeout(grant.timer);
    grant.life.abort();
    for (const connection of grant.connections) connection.destroy();
    grant.connections.clear();
  }
  function revokeExecution(binding: ExecutionBinding) {
    binding.revoked = true;
    binding.life.abort();
    for (const grant of [...grants.values()])
      if (grant.binding === binding) revoke(grant);
  }
  function issue(
    binding: ExecutionBinding,
    kind: Access["kind"],
    durationMs?: number,
    control = false,
    resize = false,
    restored?: { token: string; expiresAt?: number },
  ) {
    if (grants.size >= 2000) throw Error("browser_access_capacity_exceeded");
    const token = restored?.token ?? randomBytes(32).toString("hex");
    const grant: Access = {
      token,
      binding,
      kind,
      control,
      resize,
      life: new AbortController(),
      connections: new Set(),
      expiresAt:
        restored?.expiresAt ?? (durationMs ? Date.now() + durationMs : undefined),
    };
    grants.set(token, grant);
    if (durationMs) {
      grant.timer = setTimeout(
        () => revoke(grant),
        Math.max(1, grant.expiresAt - Date.now()),
      );
      grant.timer.unref();
    }
    return grant;
  }
  async function access(
    token: string,
    kind: Access["kind"],
    request,
  ): Promise<Access | undefined> {
    const grant = grants.get(token);
    if (
      !grant ||
      closing ||
      grant.kind !== kind ||
      grant.binding.revoked ||
      grant.life.signal.aborted ||
      (grant.expiresAt && grant.expiresAt <= Date.now()) ||
      !allowedOrigin(request)
    )
      return;
    await options.authorizeAccess?.(grant.binding.executionId, request);
    return grant.life.signal.aborted || grant.binding.revoked ? undefined : grant;
  }
  const endpoint = (binding: ExecutionBinding) => {
    let grant = [...grants.values()].find(
      (grant) => grant.binding === binding && grant.kind === "cdp" && !grant.expiresAt,
    );
    grant ??= issue(binding, "cdp");
    return {
      protocol: "browser-sandbox.host/v1",
      wsEndpoint: `${origin.replace(/^http/, "ws")}/execution/${grant.token}`,
    };
  };
  const event = (
    binding: ExecutionBinding,
    type: Parameters<NonNullable<ControllerOptions["onWorkerEvent"]>>[0]["type"],
    extra = {},
  ) => {
    try {
      options.onWorkerEvent?.({
        type,
        executionId: binding.executionId,
        incarnation: binding.incarnation,
        ...extra,
      });
    } catch {}
  };
  function automationCallbacks(binding: ExecutionBinding) {
    const invoke = async (
      operation: string,
      executionId: string,
      signal?: AbortSignal,
    ) => {
      if (!binding.host || binding.host.signal.aborted)
        throw Error("browser_automation_unavailable");
      await binding.host.callback(operation, { executionId }, signal);
    };
    return {
      pause: (id: string, signal?: AbortSignal) => invoke("pause", id, signal),
      resume: (id: string, signal?: AbortSignal) => invoke("resume", id, signal),
    };
  }
  function bindStorage(binding: ExecutionBinding) {
    if (!binding.profileId && binding.storageMode !== "host") return;
    if (binding.storageMode === "host") {
      binding.storage = {
        load: (signal) => {
          if (!binding.storagePeer || binding.storagePeer.signal.aborted)
            throw Error("browser_storage_disconnected");
          return binding.storagePeer.callback(
            "load",
            { executionId: binding.executionId },
            signal,
          );
        },
        save: (signal) => {
          if (!binding.storagePeer || binding.storagePeer.signal.aborted)
            throw Error("browser_storage_disconnected");
          return binding.storagePeer.callback(
            "save",
            { executionId: binding.executionId },
            signal,
          );
        },
        commit: (payload, signal) => {
          if (!binding.storagePeer || binding.storagePeer.signal.aborted)
            throw Error("browser_storage_disconnected");
          return binding.storagePeer.callback(
            "commit",
            { executionId: binding.executionId, payload },
            signal,
          );
        },
      };
    } else {
      if (!options.profileStore) throw Error("browser_profile_storage_unavailable");
      binding.storage = binding.volumeAdapter = options.profileStore.open(
        binding.profileId,
      );
    }
  }
  function checkHostStorageCapacity(binding?: ExecutionBinding) {
    if (
      (!binding || !storageAttachments.has(binding)) &&
      storageAttachments.size >= 2000
    )
      throw Error("browser_access_capacity_exceeded");
  }
  function bindHostStorage(binding: ExecutionBinding, peer: ControlPeer) {
    if (peer.signal.aborted) throw Error("browser_storage_disconnected");
    if (binding.storagePeer === peer) return;
    binding.storageDisconnect?.();
    binding.storagePeer = peer;
    storageAttachments.add(binding);
    const disconnect = () => {
      peer.signal.removeEventListener("abort", disconnect);
      if (binding.storagePeer !== peer) return;
      binding.storageConnection?.close();
      binding.storageConnection = undefined;
      binding.storagePeer = undefined;
      binding.storageDisconnect = undefined;
      storageAttachments.delete(binding);
    };
    binding.storageDisconnect = disconnect;
    peer.signal.addEventListener("abort", disconnect, { once: true });
  }
  async function attachWorkerStorage(binding: ExecutionBinding, signal?: AbortSignal) {
    if (!binding.storage) return;
    binding.storageConnection?.close();
    binding.storageConnection = await binding.worker.runtime.attachStorage(
      {
        executionId: binding.executionId,
        profileId: binding.profileId,
        allowedDomains: binding.allowedDomains,
        storage: binding.profileId ? true : undefined,
      },
      binding.storage,
      {
        signal:
          binding.storageMode === "host" && binding.storagePeer
            ? AbortSignal.any([binding.storagePeer.signal, ...(signal ? [signal] : [])])
            : signal,
      },
    );
  }
  function closeWorkerChannel(worker?: ManagedWorker) {
    let runtime: ManagedWorker["runtime"];
    try {
      runtime = worker?.runtime;
    } catch (error) {
      if (error.message !== "browser_worker_not_ready") throw error;
      return;
    }
    runtime?.close?.();
  }
  function adoptRecoveredWorker(binding: ExecutionBinding, worker?: ManagedWorker) {
    if (
      worker &&
      (worker.profileId !== binding.profileId ||
        (binding.worker && binding.worker.id !== worker.id))
    )
      throw Error("browser_worker_ownership_mismatch");
    if (binding.worker === worker) return;
    binding.storageConnection?.close();
    closeWorkerChannel(binding.worker);
    binding.worker = worker;
    binding.starting = worker ? Promise.resolve(worker) : undefined;
  }
  function closeUnusedDiscovery(workers: ManagedWorker[]) {
    const live = new Set([...bindings.values()].map((binding) => binding.worker));
    for (const worker of workers) if (!live.has(worker)) closeWorkerChannel(worker);
  }
  async function checkGeneration(binding: ExecutionBinding, signal?: AbortSignal) {
    const status = await binding.worker.runtime.request<any>(
      { op: "worker_status", executionId: binding.executionId },
      { signal, timeoutMs: 10000 },
    );
    if (status?.protocol !== WORKER_PROTOCOL)
      throw Error("browser_worker_protocol_unsupported");
    if (!status.session || typeof status.session.state !== "string")
      throw Error("browser_worker_status_invalid");
    if (
      binding.generation &&
      (status.session.generation !== binding.generation ||
        ["lost", "stopped", "expired"].includes(status.session.state))
    ) {
      binding.recoveryError = "browser_session_lost";
      revokeExecution(binding);
      binding.phase = "recovery-required";
      await persist(binding);
      throw Error("browser_session_lost");
    }
    if (
      !binding.generation &&
      ["live", "retained"].includes(status.session.state) &&
      typeof status.session.generation === "string"
    ) {
      binding.generation = status.session.generation;
      await persist(binding);
    }
    return status;
  }
  async function removeWorker(worker: ManagedWorker, signal: AbortSignal) {
    let failure: unknown;
    try {
      await options.backend.remove(worker, signal);
    } catch (error) {
      failure = error;
    }
    try {
      closeWorkerChannel(worker);
    } catch (error) {
      failure = failure ? new WorkerCleanupFailure(failure, error) : error;
    }
    if (failure) throw failure;
  }
  async function ensureWorker(binding: ExecutionBinding, signal: AbortSignal) {
    signal.throwIfAborted();
    if (binding.revoked || closing) throw Error("browser_execution_revoked");
    if (
      binding.cleanupUnconfirmed ||
      binding.creationFailure instanceof WorkerCleanupFailure
    )
      throw binding.creationFailure ?? Error("browser_worker_cleanup_unconfirmed");
    if (binding.worker && !binding.endpoint)
      throw binding.creationFailure ?? Error("browser_worker_cleanup_unconfirmed");
    if ((binding.creation?.waiters ?? 0) >= 64)
      throw Error("browser_worker_waiters_full");
    binding.lastActivityAt = Date.now();
    if (binding.worker && binding.generation) await checkGeneration(binding, signal);
    if (!binding.starting) {
      const attempt = { abort: new AbortController(), waiters: 0 };
      binding.creation = attempt;
      binding.creationFailure = undefined;
      binding.incarnation = randomUUID();
      const queuedSignal = AbortSignal.any([binding.life.signal, attempt.abort.signal]);
      const startedAt = performance.now();
      event(binding, "creating");
      const starting = (async () => {
        try {
          binding.slot = await admission.acquire(queuedSignal);
          const creationSignal = AbortSignal.any([queuedSignal, binding.slot.signal]);
          creationSignal.throwIfAborted();
          binding.phase = "starting";
          await persist(binding);
          creationSignal.throwIfAborted();
          const worker = await options.backend.create(
            {
              executionId: binding.executionId,
              incarnation: binding.incarnation,
              profileId: binding.profileId,
              allowedDomains: binding.allowedDomains,
              automation: automationCallbacks(binding),
            },
            creationSignal,
          );
          binding.worker = worker;
          creationSignal.throwIfAborted();
          const result = await worker.runtime.request<{
            cdpUrl: string;
            viewerToken: string;
          }>(
            {
              op: "register",
              executionId: binding.executionId,
              profileId: binding.profileId,
              storage: binding.profileId ? true : undefined,
              allowedDomains: binding.allowedDomains,
            },
            { signal: creationSignal },
          );
          creationSignal.throwIfAborted();
          await attachWorkerStorage(binding, creationSignal);
          creationSignal.throwIfAborted();
          binding.endpoint = result;
          binding.phase = "active";
          binding.lastActivityAt = Date.now();
          await persist(binding);
          creationSignal.throwIfAborted();
          binding.slot.ready();
          binding.creation = undefined;
          event(binding, "ready", { elapsedMs: performance.now() - startedAt });
          return worker;
        } catch (operation) {
          let failure = operation;
          binding.cleanupUnconfirmed =
            operation instanceof WorkerCleanupFailure ||
            operation?.message?.startsWith("browser_worker_cleanup_unconfirmed");
          if (binding.worker) {
            try {
              await removeWorker(binding.worker, AbortSignal.timeout(30000));
              binding.worker = undefined;
            } catch (cleanup) {
              binding.cleanupUnconfirmed = true;
              failure = new WorkerCleanupFailure(operation, cleanup);
            }
          }
          binding.endpoint = undefined;
          binding.generation = undefined;
          binding.phase = binding.cleanupUnconfirmed
            ? "recovery-required"
            : "registered";
          if (binding.cleanupUnconfirmed) binding.slot?.retain();
          else {
            binding.slot?.release();
            binding.slot = undefined;
          }
          await persist(binding);
          throw failure;
        }
      })();
      binding.starting = starting.catch((error) => {
        if (binding.creation === attempt) {
          binding.starting = undefined;
          binding.creation = undefined;
          binding.creationFailure = error;
        }
        event(binding, "failed", {
          elapsedMs: performance.now() - startedAt,
          error: errorCode(error),
        });
        throw error;
      });
    }
    const attempt = binding.creation;
    if (!attempt) return binding.starting;
    attempt.waiters++;
    try {
      return await waitForControl(binding.starting, signal);
    } finally {
      attempt.waiters--;
      if (!attempt.waiters && !binding.endpoint)
        attempt.abort.abort(Error("browser_worker_creation_cancelled"));
    }
  }
  async function dispatch(
    request: any,
    peer?: ControlPeer,
    signal = AbortSignal.timeout(CONTROL_TIMEOUT_MS),
  ): Promise<any> {
    signal.throwIfAborted();
    if (request?.op === "controller_status") {
      const id = request.executionId;
      if (id !== undefined && !/^[a-f0-9-]{36}$/.test(id))
        throw Error("browser_request_invalid");
      const binding = bindings.get(id);
      const record =
        !binding && journal?.has(id) && finished.has(id)
          ? await journal.read(id).catch((error) => {
              if (error.code !== "ENOENT") throw error;
              return undefined;
            })
          : undefined;
      return {
        ...admission.status(),
        limits: { ...capacityLimits, maxExecutionMs, idleRegistrationMs },
        executions: bindings.size,
        recoveryBlocked: [...bindings.values()].filter(
          (item) => item.phase === "recovery-blocked",
        ).length,
        ...(id
          ? {
              execution: {
                phase: binding?.phase ?? (finished.has(id) ? "finished" : "absent"),
                checkedAt: binding?.checkedAt,
                idleProtection: binding?.idleProtection,
                statusError: binding?.statusError,
                recoveryAttempts: binding?.recoveryAttempts,
                reclaimReason: binding?.reclaimReason ?? record?.data.reclaimReason,
              },
            }
          : {}),
      };
    }
    if (request?.op === "profile_delete") {
      if (!validProfileId(request.profileId) || !options.profileStore)
        throw Error("browser_profile_storage_unavailable");
      await recoverProfiles(request.profileId);
      if (profileLeases.has(request.profileId)) throw Error("browser_profile_busy");
      return { deleted: await options.profileStore.delete(request.profileId, signal) };
    }
    if (
      !request ||
      typeof request.op !== "string" ||
      !/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(
        request.executionId ?? "",
      )
    )
      throw Error("browser_request_invalid");
    const id = request.executionId;
    let binding = bindings.get(id);
    if (request.op === "storage_detach") {
      if (binding?.storagePeer && binding.storagePeer !== peer)
        throw Error("browser_host_authority_required");
      binding?.storageDisconnect?.();
      return { detached: true };
    }
    if (request.op === "register" || request.op === "storage_attach") {
      const storageReconnect =
        request.op === "storage_attach" &&
        binding?.storageMode === "host" &&
        ["finalizing", "recovery-blocked"].includes(binding.phase);
      if (closing || finished.has(id) || (binding?.revoked && !storageReconnect))
        throw Error("browser_execution_revoked");
      if (request.tenantId !== undefined)
        throw Error("browser_tenant_isolation_unavailable");
      const hostStorage = request.op === "storage_attach" || request.storage === true;
      if (request.profileId !== undefined && !validProfileId(request.profileId))
        throw Error("browser_profile_id_invalid");
      if (
        request.storage !== undefined &&
        (request.storage !== true || !request.profileId)
      )
        throw Error("browser_profile_binding_invalid");
      if (hostStorage && !peer) throw Error("browser_profile_storage_unavailable");
      if (hostStorage && peer.signal.aborted)
        throw Error("browser_storage_disconnected");
      if (hostStorage) checkHostStorageCapacity(binding);
      if (request.profileId) {
        if (!hostStorage && !options.profileStore)
          throw Error("browser_profile_storage_unavailable");
        await recoverProfiles(request.profileId);
        signal.throwIfAborted();
        binding = bindings.get(id);
        if (closing || finished.has(id) || (binding?.revoked && !storageReconnect))
          throw Error("browser_execution_revoked");
        if (hostStorage) checkHostStorageCapacity(binding);
      }
      const domains = normalizeAllowedDomains(request.allowedDomains);
      if (binding && JSON.stringify(binding.allowedDomains) !== JSON.stringify(domains))
        throw Error("browser_domains_changed");
      if (
        binding &&
        (binding.profileId !== request.profileId ||
          (hostStorage && binding.storageMode !== "host"))
      )
        throw Error("browser_profile_binding_changed");
      if (!binding) {
        if (bindings.size >= 65536) throw Error("browser_execution_capacity_exceeded");
        if (request.profileId && profileLeases.has(request.profileId))
          throw Error("browser_profile_busy");
        binding = {
          executionId: id,
          incarnation: randomUUID(),
          allowedDomains: domains,
          profileId: request.profileId,
          storageMode: hostStorage ? "host" : request.profileId ? "volume" : undefined,
          life: new AbortController(),
          revoked: false,
          host: request.automation ? peer : undefined,
          registeredAt: Date.now(),
          lastActivityAt: Date.now(),
          phase: "registered",
          revision: 0,
        };
        bindStorage(binding);
        if (request.profileId) profileLeases.set(request.profileId, binding);
        bindings.set(id, binding);
      }
      if (
        hostStorage &&
        binding.storagePeer &&
        (request.op === "storage_attach" || binding.storagePeer !== peer) &&
        !binding.storagePeer.signal.aborted
      )
        throw Error("browser_storage_already_attached");
      if (hostStorage) bindHostStorage(binding, peer);
      if (request.automation && peer) binding.host = peer;
      if (request.op === "storage_attach" && binding.worker) {
        try {
          await attachWorkerStorage(binding, signal);
        } catch (error) {
          binding.storageDisconnect?.();
          throw error;
        }
      }
      const result =
        request.op === "storage_attach" ? { attached: true } : endpoint(binding);
      await persist(binding);
      return result;
    }
    if (["finalize", "finish"].includes(request.op)) {
      if (!binding) {
        if (journal && !finished.has(id)) {
          await journal.create(id, { phase: "finished" });
          rememberFinished(id);
        }
        return { status: "absent" };
      }
      if (binding.phase === "recovery-blocked" && options.backend.list) {
        const workers = await options.backend.list(signal);
        try {
          const worker = workers.find(
            (item) =>
              item.executionId === id && item.incarnation === binding.incarnation,
          );
          adoptRecoveredWorker(binding, worker);
          if (!worker) {
            binding.stopConfirmed = true;
            binding.finalResult ??= { stopped: true, used: true, graceful: false };
            binding.cleanupUnconfirmed = false;
          } else if (!binding.stopConfirmed && !binding.cleanupUnconfirmed) {
            worker.runtime.setAutomation?.(automationCallbacks(binding));
            await attachWorkerStorage(binding, signal);
          }
        } finally {
          closeUnusedDiscovery(workers);
        }
      }
      if (request.ifIdle && binding.worker && !binding.stopConfirmed) {
        binding.finalResult = await binding.worker.runtime.request(
          {
            op: "finalize",
            executionId: id,
            ifIdle: true,
            expectedGeneration: request.expectedGeneration,
          },
          { signal },
        );
        if (binding.finalResult.stopped !== true)
          throw Error("browser_worker_stop_unconfirmed");
        binding.stopConfirmed = true;
      }
      binding.reclaimReason ??= "host-finish";
      revokeExecution(binding);
      binding.phase = "finalizing";
      await binding.starting?.catch(() => {});
      if (binding.cleanupUnconfirmed) {
        if (!binding.worker) {
          if (!options.backend.list)
            throw (
              binding.creationFailure ?? Error("browser_worker_discovery_unavailable")
            );
          const workers = await options.backend.list(signal);
          binding.worker = workers.find(
            (worker) =>
              worker.executionId === id && worker.incarnation === binding.incarnation,
          );
        }
        if (binding.worker) {
          // Creation never reached a client: this incarnation received no browser commands.
          await removeWorker(binding.worker, signal);
          binding.worker = undefined;
        }
        binding.cleanupUnconfirmed = false;
      }
      await persist(binding);
      let finalizationError: Error | undefined;
      let result = binding.finalResult ?? { used: false, stopped: true };
      if (binding.worker) {
        if (!binding.stopConfirmed) {
          try {
            result = await binding.worker.runtime.request(
              { op: "finalize", executionId: id },
              { signal },
            );
          } catch (error) {
            if (
              binding.profileId ||
              ![
                "browser_control_disconnected",
                "browser_control_unavailable",
                "browser_control_timeout",
              ].includes(error.message)
            )
              throw error;
            try {
              await removeWorker(binding.worker, AbortSignal.timeout(30000));
            } catch (cleanup) {
              throw new WorkerCleanupFailure(error, cleanup);
            }
            binding.worker = undefined;
            result = { used: true, stopped: true, graceful: false };
            finalizationError = Error("browser_worker_finalization_failed");
          }
          if (result.stopped !== true) throw Error("browser_worker_stop_unconfirmed");
          binding.stopConfirmed = true;
          binding.finalResult = result;
        }
        binding.phase = "cleanup-pending";
        await persist(binding);
        if (binding.worker) {
          await removeWorker(binding.worker, signal);
        }
      }
      binding.phase = "finished";
      await persist(binding);
      binding.storageDisconnect?.();
      binding.storageConnection?.close();
      await binding.volumeAdapter?.close();
      if (
        binding.profileId &&
        profileLeases.get(binding.profileId)?.incarnation === binding.incarnation
      )
        profileLeases.delete(binding.profileId);
      event(binding, "removed", { reason: binding.reclaimReason });
      binding.slot?.release();
      binding.slot = undefined;
      bindings.delete(id);
      rememberFinished(id);
      if (finalizationError) throw finalizationError;
      return result;
    }
    if (!binding || binding.revoked || closing)
      throw Error(binding?.recoveryError ?? "browser_execution_revoked");
    if (request.op === "endpoint") {
      const result = endpoint(binding);
      await persist(binding);
      return result;
    }
    if (request.op === "access_create") {
      if (binding.worker && binding.generation) await checkGeneration(binding, signal);
      const durationMs = request.durationMs ?? 600000;
      if (
        !Number.isInteger(durationMs) ||
        durationMs < 1 ||
        durationMs > 1800000 ||
        (request.control !== undefined && typeof request.control !== "boolean") ||
        (request.resize !== undefined && typeof request.resize !== "boolean")
      )
        throw Error("browser_access_request_invalid");
      const agent = issue(binding, "cdp", durationMs);
      const viewer = issue(
        binding,
        "view",
        durationMs,
        request.control === true,
        request.resize === true,
      );
      await persist(binding);
      return {
        executionId: id,
        cdpUrl: `${origin.replace(/^http/, "ws")}/execution/${agent.token}`,
        viewerUrl: `${origin}/view#${viewer.token}`,
        expiresAt: agent.expiresAt,
      };
    }
    if (request.op === "access_revoke") {
      for (const grant of [...grants.values()])
        if (grant.binding === binding) revoke(grant);
      await persist(binding);
      return { revoked: true };
    }
    if (request.op === "session" && !binding.starting) {
      if ((request.action ?? "status") === "status")
        return { state: "unused", connectedClients: 0 } satisfies SessionStatus;
      throw Error("browser_session_not_live");
    }
    if (!binding.starting) throw Error("browser_session_not_live");
    if (
      request.op === "session" &&
      ["renew", "retain"].includes(request.action) &&
      binding.worker &&
      binding.generation
    )
      await checkGeneration(binding, signal);
    const worker = await binding.starting;
    const result = await worker.runtime.request<any>(request, { signal });
    if (request.op === "session" && result.generation && !binding.generation) {
      binding.generation = result.generation;
      await persist(binding);
    }
    return result;
  }
  const mutations = new Map<string, Promise<unknown>>();
  function command(request: any, peer?: ControlPeer, signal?: AbortSignal) {
    if (request?.op === "controller_status") return dispatch(request, peer, signal);
    const id = request?.executionId;
    const work = (mutations.get(id) ?? Promise.resolve()).then(() =>
      dispatch(request, peer, signal),
    );
    const settled = work
      .catch(() => {})
      .finally(() => {
        if (mutations.get(id) === settled) mutations.delete(id);
      });
    mutations.set(id, settled);
    return work;
  }
  const controlServer = createServer((_req, res) => res.writeHead(404).end());
  controlServer.on("upgrade", (req, socket) => {
    if (req.url !== "/host")
      socket.end("HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n");
  });
  const control = createNetworkHostControl({
    server: controlServer,
    dispatch: command,
    authenticateHost: options.authenticateHost,
  });
  const viewers = new WebSocketServer({ noServer: true, maxPayload: 16384 });
  const rawConnections = new Set<import("node:net").Socket>();
  server.on("connection", (socket) => {
    rawConnections.add(socket);
    socket.once("close", () => rawConnections.delete(socket));
  });
  function tunnel(client: WebSocket, target: string, grant: Access, auth?: any) {
    const upstream = new WebSocket(target, {
      maxPayload: 32 * 1024 * 1024,
      handshakeTimeout: 30000,
      ...(auth ? { origin: grant.binding.worker.origin } : {}),
    });
    const connection = {
      destroy() {
        client.terminate();
        upstream.terminate();
      },
    };
    grant.connections.add(connection);
    const downstream = createWebSocketStream(client, {
      readableObjectMode: true,
      decodeStrings: false,
      highWaterMark: 1,
    });
    const incoming = createWebSocketStream(upstream, {
      readableObjectMode: true,
      decodeStrings: false,
      highWaterMark: 1,
    });
    const close = () => {
      grant.connections.delete(connection);
      downstream.destroy();
      incoming.destroy();
      connection.destroy();
    };
    downstream.on("error", close);
    incoming.on("error", close);
    client.once("close", close);
    upstream.once("close", close);
    upstream.on("open", async () => {
      try {
        if (grant.life.signal.aborted) return close();
        if (!auth) await checkGeneration(grant.binding, grant.life.signal);
        if (grant.life.signal.aborted) return close();
        if (auth) upstream.send(JSON.stringify(auth));
        downstream.pipe(incoming).pipe(downstream);
      } catch {
        close();
      }
    });
    upstream.on("error", close);
  }
  const cdp = new WebSocketServer({ noServer: true, maxPayload: 32 * 1024 * 1024 });
  server.on("upgrade", async (req, socket, head) => {
    const life = new AbortController();
    socket.once("close", () => life.abort());
    try {
      if (req.url === "/view/socket" && allowedOrigin(req)) {
        viewers.handleUpgrade(req, socket, head, (client) => {
          const timer = setTimeout(() => client.terminate(), 5000);
          client.once("close", () => clearTimeout(timer));
          client.on("error", () => client.terminate());
          client.once("message", async (raw) => {
            clearTimeout(timer);
            client.pause();
            try {
              const auth = JSON.parse(String(raw));
              const grant =
                auth.type === "authenticate" && (await access(auth.token, "view", req));
              if (!grant) throw Error("browser_access_denied");
              await ensureWorker(
                grant.binding,
                AbortSignal.any([life.signal, grant.life.signal]),
              );
              tunnel(
                client,
                `${grant.binding.worker.origin.replace(/^http/, "ws")}/view/socket`,
                grant,
                {
                  type: "authenticate",
                  token: grant.binding.endpoint.viewerToken,
                  control: grant.control,
                  resize: grant.resize,
                },
              );
              client.resume();
            } catch {
              client.close(1008, "browser_access_denied");
            }
          });
        });
        return;
      }
      const match = /^\/execution\/([a-f0-9]{64})$/.exec(req.url ?? "");
      const grant = match && (await access(match[1], "cdp", req));
      if (!grant) throw Error("browser_access_denied");
      await ensureWorker(
        grant.binding,
        AbortSignal.any([life.signal, grant.life.signal]),
      );
      if (socket.destroyed || grant.life.signal.aborted)
        throw Error("browser_execution_revoked");
      cdp.handleUpgrade(req, socket, head, (client) =>
        tunnel(client, grant.binding.endpoint.cdpUrl, grant),
      );
    } catch {
      socket.end("HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n");
    }
  });
  let discoveredWorkers: ManagedWorker[] = [];
  async function constructionFailed(error: unknown): Promise<never> {
    closing = true;
    control.close();
    for (const grant of [...grants.values()]) revoke(grant);
    viewers.close();
    cdp.close();
    for (const socket of rawConnections) socket.destroy();
    for (const worker of discoveredWorkers) closeWorkerChannel(worker);
    await Promise.allSettled(
      [...bindings.values()].map((binding) => binding.volumeAdapter?.close()),
    );
    await Promise.all(
      [server, controlServer]
        .filter((listener) => listener.listening)
        .map(
          (listener) => new Promise<void>((resolve) => listener.close(() => resolve())),
        ),
    );
    throw error;
  }
  try {
    if (options.stateDirectory) {
      if (!options.backend.list) throw Error("browser_worker_discovery_unavailable");
      const discovered = await options.backend.list(AbortSignal.timeout(30000));
      discoveredWorkers = discovered;
      const owned = new Map(discovered.map((worker) => [worker.id, worker]));
      const finishedRevisions = new Map<string, number>();
      journal = await createExecutionJournal({
        directory: options.stateDirectory,
        installationId: options.installationId,
        onRecord: async (record) => {
          const id = record.executionId;
          const data = record.data as any;
          validateControllerRecord(data);
          if (data.phase === "finished") {
            finishedRevisions.set(id, record.revision);
            return;
          }
          if (
            typeof data.incarnation !== "string" ||
            !Array.isArray(data.allowedDomains) ||
            !Number.isSafeInteger(data.registeredAt) ||
            !Number.isSafeInteger(data.lastActivityAt) ||
            !Array.isArray(data.grants)
          )
            throw Error("browser_execution_journal_corrupt");
          const worker =
            owned.get(data.workerId) ??
            discovered.find(
              (item) =>
                item.executionId === id && item.incarnation === data.incarnation,
            );
          if (
            worker &&
            (worker.executionId !== id ||
              worker.incarnation !== data.incarnation ||
              worker.profileId !== data.profileId)
          )
            throw Error("browser_worker_ownership_mismatch");
          const binding: ExecutionBinding = {
            executionId: id,
            incarnation: data.incarnation,
            profileId: data.profileId,
            storageMode: data.storageMode,
            infrastructureOnly: data.infrastructureOnly,
            allowedDomains: data.allowedDomains,
            registeredAt: data.registeredAt,
            lastActivityAt: data.lastActivityAt,
            phase: data.phase,
            revision: record.revision,
            recovered: true,
            endpoint: data.endpoint,
            life: new AbortController(),
            revoked: false,
            worker,
            stopConfirmed: data.stopConfirmed,
            finalResult: data.finalResult,
            generation: data.generation,
            cleanupUnconfirmed: data.cleanupUnconfirmed,
            recoveryAttempts: data.recoveryAttempts,
            recoveryError: data.recoveryError,
            statusError: data.recoveryError,
            reclaimReason: data.reclaimReason,
          };
          if (worker?.infrastructureOnly && data.cleanupUnconfirmed) {
            // Creation never published an endpoint; discovery proves only its network remains.
            binding.infrastructureOnly = true;
            binding.stopConfirmed = true;
            binding.finalResult = { stopped: true, used: false };
            binding.phase = "cleanup-pending";
            binding.revoked = true;
          }
          bindings.set(id, binding);
          if (binding.profileId && !binding.infrastructureOnly) {
            if (
              !validProfileId(binding.profileId) ||
              profileLeases.has(binding.profileId)
            )
              throw Error("browser_profile_lease_recovery_failed");
            profileLeases.set(binding.profileId, binding);
            bindStorage(binding);
          }
          if (worker) {
            owned.delete(worker.id);
            binding.starting = Promise.resolve(worker);
            binding.slot = admission.restore();
            try {
              worker.runtime.setAutomation?.(automationCallbacks(binding));
              if (!binding.stopConfirmed && data.phase !== "recovery-blocked")
                await attachWorkerStorage(binding, AbortSignal.timeout(10000));
              const status =
                binding.stopConfirmed ||
                ["finalizing", "recovery-blocked"].includes(data.phase)
                  ? { protocol: WORKER_PROTOCOL }
                  : await worker.runtime.request<any>(
                      { op: "worker_status", executionId: id },
                      { timeoutMs: 10000 },
                    );
              if (status.protocol !== WORKER_PROTOCOL)
                throw Error("browser_worker_protocol_unsupported");
              if (
                !binding.stopConfirmed &&
                !["finalizing", "recovery-blocked"].includes(data.phase)
              ) {
                if (
                  !status.session ||
                  (binding.generation &&
                    (status.session.generation !== binding.generation ||
                      ["lost", "stopped", "expired"].includes(status.session.state)))
                )
                  throw Error("browser_session_lost");
                binding.generation ??= status.session.generation;
              }
            } catch (error) {
              binding.revoked = true;
              binding.phase = "recovery-required";
              binding.recoveryError = errorCode(error);
            }
          } else if (data.phase !== "registered") {
            binding.revoked = true;
            binding.phase = "recovery-required";
          }
          if (
            [
              "finalizing",
              "cleanup-pending",
              "recovery-required",
              "recovery-blocked",
            ].includes(data.phase)
          )
            binding.revoked = true;
          bindings.set(id, binding);
          if (binding.revoked) return;
          for (const grant of data.grants) {
            if (
              !/^[a-f0-9]{64}$/.test(grant.token) ||
              !["cdp", "view"].includes(grant.kind)
            )
              throw Error("browser_execution_journal_corrupt");
            if (!grant.expiresAt || grant.expiresAt > Date.now())
              issue(
                binding,
                grant.kind,
                grant.expiresAt ? grant.expiresAt - Date.now() : undefined,
                grant.control,
                grant.resize,
                grant,
              );
          }
        },
      });
      for (const id of journal.finishedIds()) rememberFinished(id);
      for (const worker of owned.values()) {
        if (bindings.has(worker.executionId))
          throw Error("browser_worker_ownership_mismatch");
        const revision = journal.has(worker.executionId)
          ? (finishedRevisions.get(worker.executionId) ?? 0)
          : 0;
        const binding: ExecutionBinding = {
          executionId: worker.executionId,
          incarnation: worker.incarnation,
          profileId: worker.profileId,
          storageMode: worker.profileId ? "volume" : undefined,
          infrastructureOnly: worker.infrastructureOnly,
          allowedDomains: [],
          worker,
          starting: Promise.resolve(worker),
          slot: admission.restore(),
          registeredAt: Date.now(),
          lastActivityAt: Date.now(),
          phase: worker.infrastructureOnly ? "cleanup-pending" : "recovery-required",
          stopConfirmed: worker.infrastructureOnly ? true : undefined,
          finalResult: worker.infrastructureOnly
            ? { stopped: true, used: false }
            : undefined,
          reclaimReason: "orphan",
          revision,
          life: new AbortController(),
          revoked: true,
        };
        if (binding.profileId && !binding.infrastructureOnly) {
          if (profileLeases.has(binding.profileId))
            throw Error("browser_profile_lease_recovery_failed");
          profileLeases.set(binding.profileId, binding);
          try {
            bindStorage(binding);
          } catch (error) {
            binding.recoveryError = errorCode(error);
          }
        }
        try {
          if (worker.infrastructureOnly) {
            bindings.set(binding.executionId, binding);
            await persist(binding);
            continue;
          }
          await attachWorkerStorage(binding);
          const status = await worker.runtime.request<any>(
            { op: "worker_status", executionId: worker.executionId },
            { timeoutMs: 10000 },
          );
          if (status.protocol !== WORKER_PROTOCOL)
            throw Error("browser_worker_protocol_unsupported");
          binding.phase = "finalizing";
        } catch (error) {
          binding.recoveryError = errorCode(error);
        }
        bindings.set(binding.executionId, binding);
        await persist(binding);
      }
    }
  } catch (error) {
    await constructionFailed(error);
  }
  let reconciliation: Promise<void> = Promise.resolve();
  function reconcile(): Promise<void> {
    const run = async () => {
      if (closing) return;
      let discovered: Promise<ManagedWorker[]> | undefined;
      const recoveryWorkers = () =>
        (discovered ??= options.backend.list(AbortSignal.timeout(30000)));
      for (const binding of [...bindings.values()]) {
        if (closing) break;
        try {
          if (binding.revoked || binding.cleanupUnconfirmed) {
            if (binding.phase === "recovery-blocked") continue;
            if (binding.recoveryError === "browser_worker_protocol_unsupported") {
              // Preserve the durable descriptor and grants so a compatible controller can roll back.
              binding.phase = "recovery-blocked";
              event(binding, "recovery-blocked", { error: binding.recoveryError });
              continue;
            }
            binding.recoveryAttempts = Math.min(3, (binding.recoveryAttempts ?? 0) + 1);
            await persist(binding, "recovery-attempt");
            if (options.backend.list) {
              const worker = (await recoveryWorkers()).find(
                (worker) =>
                  worker.executionId === binding.executionId &&
                  worker.incarnation === binding.incarnation,
              );
              if (!worker) {
                adoptRecoveredWorker(binding, undefined);
                binding.cleanupUnconfirmed = false;
                binding.stopConfirmed = true;
                binding.finalResult ??= { stopped: true, used: true, graceful: false };
                binding.phase = "cleanup-pending";
              } else if (
                !binding.cleanupUnconfirmed &&
                !binding.stopConfirmed &&
                binding.recoveryError !== "browser_worker_protocol_unsupported"
              ) {
                adoptRecoveredWorker(binding, worker);
                try {
                  worker.runtime.setAutomation?.(automationCallbacks(binding));
                  const status = await worker.runtime.request<any>(
                    { op: "worker_status", executionId: binding.executionId },
                    { timeoutMs: 10000 },
                  );
                  if (status.protocol !== WORKER_PROTOCOL)
                    throw Error("browser_worker_protocol_unsupported");
                  await attachWorkerStorage(binding, AbortSignal.timeout(10000));
                  await command({ op: "finalize", executionId: binding.executionId });
                } catch (error) {
                  if (
                    !binding.profileId &&
                    binding.recoveryAttempts >= 3 &&
                    errorCode(error) !== "browser_worker_protocol_unsupported"
                  ) {
                    try {
                      await removeWorker(worker, AbortSignal.timeout(30000));
                    } catch (cleanup) {
                      throw new WorkerCleanupFailure(error, cleanup);
                    }
                    binding.worker = undefined;
                    binding.starting = undefined;
                    binding.cleanupUnconfirmed = false;
                    binding.stopConfirmed = true;
                    binding.finalResult = {
                      stopped: true,
                      used: true,
                      graceful: false,
                    };
                    binding.phase = "cleanup-pending";
                    await command({ op: "finalize", executionId: binding.executionId });
                  } else throw error;
                }
                continue;
              }
            }
            if (
              binding.cleanupUnconfirmed ||
              ["cleanup-pending", "finalizing"].includes(binding.phase) ||
              binding.stopConfirmed
            )
              await command({ op: "finalize", executionId: binding.executionId });
            continue;
          }
          const now = Date.now();
          if (now - binding.registeredAt >= maxExecutionMs) {
            binding.reclaimReason = "maximum-lifetime";
            await command({ op: "finalize", executionId: binding.executionId });
            continue;
          }
          if (now - binding.lastActivityAt < idleRegistrationMs || binding.creation)
            continue;
          if (!binding.worker) {
            binding.reclaimReason = "idle";
            await command({ op: "finalize", executionId: binding.executionId });
            continue;
          }
          const status = await binding.worker.runtime.request<any>(
            { op: "worker_status", executionId: binding.executionId },
            { timeoutMs: 10000 },
          );
          binding.checkedAt = Date.now();
          binding.statusError = undefined;
          binding.idleProtection = workerIdleProtection(status, now);
          if (binding.idleProtection) continue;
          binding.reclaimReason = "idle";
          await command({
            op: "finalize",
            executionId: binding.executionId,
            ifIdle: true,
            expectedGeneration: status.session.generation,
          });
        } catch (error) {
          binding.checkedAt = Date.now();
          binding.statusError = errorCode(error);
          binding.idleProtection = "unknown";
          if (
            binding.revoked &&
            binding.statusError === "browser_worker_protocol_unsupported"
          ) {
            binding.phase = "recovery-blocked";
            binding.recoveryError = binding.statusError;
            event(binding, "recovery-blocked", { error: binding.recoveryError });
          } else if (
            binding.revoked &&
            binding.recoveryAttempts >= 3 &&
            bindings.has(binding.executionId)
          ) {
            binding.phase = "recovery-blocked";
            binding.recoveryError = binding.statusError;
            try {
              await persist(binding);
            } catch {
              binding.statusError = binding.recoveryError =
                "browser_execution_journal_unavailable";
            }
            event(binding, "recovery-blocked", { error: binding.recoveryError });
          }
          /* Unknown status or failed finalization retains resources and ownership. */
        }
      }
      if (discovered) closeUnusedDiscovery(await discovered.catch(() => []));
    };
    reconciliation = reconciliation.then(run, run);
    return reconciliation;
  }
  let reconciliationTimer: ReturnType<typeof setTimeout>;
  const schedule = () => {
    reconciliationTimer = setTimeout(() => {
      void reconcile()
        .catch(() => {})
        .finally(() => {
          if (!closing) schedule();
        });
    }, reconciliationMs);
    reconciliationTimer.unref();
  };
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(options.port ?? 8080, options.host ?? "127.0.0.1", resolve);
  }).catch(constructionFailed);
  await new Promise<void>((resolve, reject) => {
    controlServer.once("error", reject);
    controlServer.listen(
      options.controlPort ?? 0,
      options.controlHost ?? "127.0.0.1",
      resolve,
    );
  }).catch(constructionFailed);
  schedule();
  origin ??= `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  return {
    controlOrigin: `http://127.0.0.1:${(controlServer.address() as AddressInfo).port}`,
    origin,
    command,
    bindings,
    async expireProfiles(signal?: AbortSignal) {
      if (!options.profileStore) return 0;
      await recoverProfiles();
      return options.profileStore.expire(signal, (profileId) =>
        profileLeases.has(profileId),
      );
    },
    capacity: () => ({
      ...admission.status(),
      recoveryBlocked: [...bindings.values()].filter(
        (binding) => binding.phase === "recovery-blocked",
      ).length,
      waitingClients: [...bindings.values()].reduce(
        (sum, binding) => sum + (binding.creation?.waiters ?? 0),
        0,
      ),
    }),
    reconcile,
    async stop(settings: { finalize?: boolean; preserveWorkers?: boolean } = {}) {
      const finalize =
        settings.finalize ??
        !(
          settings.preserveWorkers ??
          (Boolean(journal) || options.backend.preserveWorkersOnShutdown === true)
        );
      if (stopped) return;
      stopped = true;
      closing = true;
      clearTimeout(reconciliationTimer);
      let failure: unknown;
      try {
        await reconciliation;
      } catch (error) {
        failure = error;
      }
      if (!finalize) for (const binding of bindings.values()) binding.life.abort();
      for (const binding of bindings.values()) {
        try {
          if (!finalize) {
            if (
              binding.creationFailure instanceof WorkerCleanupFailure ||
              binding.cleanupUnconfirmed
            )
              throw (
                binding.creationFailure ?? Error("browser_worker_cleanup_unconfirmed")
              );
            binding.life.abort();
            if (!binding.endpoint)
              await binding.starting?.catch((error) => {
                if (
                  error?.name !== "AbortError" &&
                  error?.message !== "browser_worker_creation_cancelled"
                )
                  throw error;
              });
            await binding.persisted;
          } else await command({ op: "finalize", executionId: binding.executionId });
        } catch (error) {
          failure ??= error;
        } finally {
          if (!finalize) {
            binding.storageDisconnect?.();
            binding.storageConnection?.close();
            closeWorkerChannel(binding.worker);
            try {
              await binding.volumeAdapter?.close();
            } catch (error) {
              failure ??= error;
            }
          }
        }
      }
      control.close();
      for (const grant of [...grants.values()]) revoke(grant);
      for (const client of [...viewers.clients, ...cdp.clients]) client.terminate();
      viewers.close();
      cdp.close();
      for (const socket of rawConnections) socket.destroy();
      controlServer.closeAllConnections();
      await Promise.all(
        [server, controlServer].map(
          (listener) => new Promise<void>((resolve) => listener.close(() => resolve())),
        ),
      );
      if (failure) throw failure;
    },
  };
}
function errorCode(error: any) {
  return /^browser_[a-z_]+$/.test(error?.message)
    ? error.message
    : "browser_worker_unavailable";
}

function validateControllerRecord(data: any) {
  const invalid = () => {
    throw Error("browser_execution_journal_corrupt");
  };
  if (!data || typeof data !== "object" || Array.isArray(data)) invalid();
  if (
    data.recoveryError !== undefined &&
    (typeof data.recoveryError !== "string" ||
      !/^browser_[a-z_]+$/.test(data.recoveryError))
  )
    invalid();
  if (
    data.recoveryAttempts !== undefined &&
    (!Number.isInteger(data.recoveryAttempts) ||
      data.recoveryAttempts < 0 ||
      data.recoveryAttempts > 3)
  )
    invalid();
  if (
    ![
      "registered",
      "starting",
      "active",
      "finalizing",
      "cleanup-pending",
      "recovery-required",
      "recovery-blocked",
      "finished",
    ].includes(data.phase)
  )
    invalid();
  for (const key of ["stopConfirmed", "cleanupUnconfirmed", "infrastructureOnly"])
    if (data[key] !== undefined && typeof data[key] !== "boolean") invalid();
  if (
    data.stopConfirmed &&
    (!["finalizing", "cleanup-pending", "recovery-blocked", "finished"].includes(
      data.phase,
    ) ||
      data.finalResult?.stopped !== true)
  )
    invalid();
  if (
    data.finalResult !== undefined &&
    (!data.finalResult ||
      typeof data.finalResult !== "object" ||
      Array.isArray(data.finalResult) ||
      data.finalResult.stopped !== true)
  )
    invalid();
  if (
    data.cleanupUnconfirmed &&
    (data.generation !== undefined ||
      ![
        "recovery-required",
        "recovery-blocked",
        "finalizing",
        "cleanup-pending",
      ].includes(data.phase))
  )
    invalid();
  if (data.infrastructureOnly && !data.stopConfirmed) invalid();
  if (data.profileId !== undefined && !validProfileId(data.profileId)) invalid();
  if (data.storageMode !== undefined && !["host", "volume"].includes(data.storageMode))
    invalid();
  if (
    (data.profileId !== undefined && data.storageMode === undefined) ||
    (data.storageMode === "volume" && data.profileId === undefined)
  )
    invalid();
  if (data.phase === "finished") return;
  if (
    !/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(
      data.incarnation ?? "",
    ) ||
    !Number.isSafeInteger(data.registeredAt) ||
    !Number.isSafeInteger(data.lastActivityAt) ||
    !Array.isArray(data.grants)
  )
    invalid();
  try {
    if (
      JSON.stringify(normalizeAllowedDomains(data.allowedDomains)) !==
      JSON.stringify(data.allowedDomains)
    )
      invalid();
  } catch {
    invalid();
  }
  if (
    data.generation !== undefined &&
    (typeof data.generation !== "string" ||
      !data.generation ||
      data.generation.length > 128)
  )
    invalid();
  if (
    data.workerId !== undefined &&
    (typeof data.workerId !== "string" || !data.workerId || data.workerId.length > 256)
  )
    invalid();
  if (data.endpoint !== undefined) {
    if (
      !data.endpoint ||
      typeof data.endpoint !== "object" ||
      !/^[a-f0-9]{64}$/.test(data.endpoint.viewerToken ?? "")
    )
      invalid();
    try {
      const url = new URL(data.endpoint.cdpUrl);
      if (
        url.protocol !== "ws:" ||
        url.username ||
        url.password ||
        url.search ||
        url.hash ||
        !/^\/execution\/[a-f0-9]{64}$/.test(url.pathname)
      )
        invalid();
    } catch {
      invalid();
    }
  }
  const tokens = new Set<string>();
  for (const grant of data.grants) {
    if (
      !grant ||
      !/^[a-f0-9]{64}$/.test(grant.token ?? "") ||
      tokens.has(grant.token) ||
      !["cdp", "view"].includes(grant.kind) ||
      typeof grant.control !== "boolean" ||
      typeof grant.resize !== "boolean" ||
      (grant.expiresAt !== undefined &&
        (!Number.isSafeInteger(grant.expiresAt) || grant.expiresAt < 1))
    )
      invalid();
    tokens.add(grant.token);
  }
}
