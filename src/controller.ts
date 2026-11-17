import { randomBytes, randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createServer, request as httpRequest } from "node:http";
import type { AddressInfo } from "node:net";
import { pipeline, Transform } from "node:stream";
import { isDeepStrictEqual } from "node:util";
import WebSocket, { createWebSocketStream, WebSocketServer } from "ws";
import { waitForControl } from "./automation-control.js";
import {
  createHostForwarding,
  forwardControllerRequest,
  forwardControllerViewer,
} from "./controller-forwarding.js";
import { createExecutionJournal } from "./execution-journal.js";
import { MAX_FILE_BYTES } from "./files.js";
import type { StorageAdapter } from "./host-client.js";
import type { KubernetesOwnership } from "./kubernetes-ownership.js";
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
import { MAX_ARCHIVE_BYTES } from "./profiles.js";
import type { SessionStatus } from "./session.js";
import { createWorkerAdmission, type WorkerSlot } from "./worker-admission.js";
import { WORKER_PROTOCOL, workerIdleProtection } from "./worker-protocol.js";

export interface ExecutionBinding {
  executionId: string;
  incarnation: string;
  allowedDomains: string[];
  profileId?: string;
  profileAcquired?: boolean;
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
  confirmedCommit?: { incarnation: string; result: Record<string, unknown> };
  host?: ControlPeer;
  registeredAt: number;
  lastActivityAt: number;
  phase: string;
  revision: number;
  persisted?: Promise<void>;
  pendingRecordData?: Record<string, unknown>;
  slot?: WorkerSlot;
  creation?: { abort: AbortController; waiters: number };
  creationFailure?: unknown;
  stopConfirmed?: boolean;
  finalResult?: any;
  recoveryError?: string;
  recoveryAttempts?: number;
  recoveryRetryAt?: number;
  recovered?: boolean;
  generation?: string;
  workerProtocol?: string;
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
  ownership?: KubernetesOwnership;
  replicas?: number;
  peerPort?: number;
  peerHost?: string;
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
  let journal:
    | Awaited<ReturnType<typeof createExecutionJournal>>
    | KubernetesOwnership["journal"]
    | undefined;
  const ownership = options.ownership;
  const supportedProtocol = (protocol: unknown) =>
    protocol === WORKER_PROTOCOL ||
    (!ownership && protocol === "browser-sandbox.worker/v1");
  const forwarding = createHostForwarding();
  if ((options.replicas ?? 1) > 1 && options.profileStore)
    throw Error("browser_profile_store_replicas_unsupported");
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
  let recovering: Promise<ManagedWorker[]> | undefined;
  const localLease = (lease: { executionId: string; incarnation: string }) => {
    const binding = bindings.get(lease.executionId);
    return (
      binding?.incarnation === lease.incarnation &&
      (!binding.recovered || binding.phase === "registered")
    );
  };
  const recoverProfiles = async (profileId?: string, owningExecutionId?: string) => {
    if (ownership) return;
    const leases = profileId
      ? [profileLeases.get(profileId)].filter(Boolean)
      : [...profileLeases.values()];
    if (recoveredProfiles && !leases.some((lease) => !localLease(lease)))
      return Promise.resolve();
    if (!options.backend.list) throw Error("browser_profile_leases_unavailable");
    // Share discovery, while finalization stays in each execution's own operation.
    const workers = await (recovering ??= options.backend
      .list(AbortSignal.timeout(30000))
      .finally(() => {
        recovering = undefined;
      }));
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
      if (profileId && id !== profileId) continue;
      const binding = bindings.get(lease.executionId);
      if (!localLease(lease) && binding?.recovered && !recovered.has(id)) {
        adoptRecoveredWorker(binding, undefined);
        binding.stopConfirmed = true;
        binding.cleanupUnconfirmed = false;
        binding.finalResult ??= { stopped: true, used: true, graceful: false };
        binding.phase = "cleanup-pending";
        const finish = { op: "finalize", executionId: binding.executionId };
        if (binding.executionId === owningExecutionId) await dispatch(finish);
        else await command(finish);
      }
    }
    // Only a successful authoritative read can release recovered occupancy.
    for (const [id, lease] of profileLeases)
      if ((!profileId || id === profileId) && !localLease(lease))
        profileLeases.delete(id);
    for (const [id, worker] of recovered) {
      const lease = profileLeases.get(id);
      if (!lease) profileLeases.set(id, worker);
    }
    recoveredProfiles = true;
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
      profileAcquired: binding.profileAcquired,
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
            ...finalizationDetails(binding.finalResult),
          }
        : undefined,
      confirmedCommit: binding.confirmedCommit,
      generation: binding.generation,
      workerProtocol: binding.workerProtocol ?? WORKER_PROTOCOL,
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
      const reread = async () => {
        const record = await journal.read(binding.executionId);
        if (
          [recordData, binding.pendingRecordData].some(
            (data) =>
              data && isDeepStrictEqual(record.data, JSON.parse(JSON.stringify(data))),
          )
        ) {
          binding.revision = record.revision;
          binding.pendingRecordData = undefined;
        }
        return record.revision;
      };
      if (binding.pendingRecordData) {
        if ((await reread()) !== binding.revision)
          throw Error("browser_execution_record_changed");
        binding.pendingRecordData = undefined;
      }
      try {
        const record = binding.revision
          ? await journal.update(binding.executionId, binding.revision, recordData)
          : await journal.create(binding.executionId, recordData);
        binding.revision = record.revision;
        binding.pendingRecordData = undefined;
      } catch (error) {
        binding.pendingRecordData = recordData;
        try {
          await reread();
        } catch {
          // Unknown disk state cannot confirm this write.
        }
        throw error;
      }
    };
    binding.persisted = (binding.persisted ?? Promise.resolve()).then(write, write);
    return binding.persisted;
  }
  const assets = new Map([
    ["/view", ["index.html", "text/html"]],
    ["/viewer.js", ["browser-viewer.js", "text/javascript"]],
    ["/viewer.css", ["viewer.css", "text/css"]],
  ]);
  const handleAgent = async (req, res, forwarded = false) => {
    const life = new AbortController();
    res.once("close", () => {
      if (!res.writableEnded) life.abort();
    });
    let grant: Access;
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
      if (await routeAgent(req, res, undefined, forwarded)) return;
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
      grant = match && (await access(match[1], "cdp", req));
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
      upstream.on("error", (error) => {
        if (res.writableEnded) return;
        if (!res.headersSent)
          res
            .writeHead(errorStatus(error, grant))
            .end(JSON.stringify({ error: errorCode(error, grant) }));
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
        res
          .writeHead(errorStatus(error, grant))
          .end(JSON.stringify({ error: errorCode(error, grant) }));
      else res.destroy();
    }
  };
  const server = createServer(handleAgent);
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
      (request.executionId && request.executionId !== grant.binding.executionId) ||
      grant.binding.revoked ||
      grant.life.signal.aborted ||
      (grant.expiresAt && grant.expiresAt <= Date.now()) ||
      !allowedOrigin(request)
    )
      return;
    try {
      await options.authorizeAccess?.(grant.binding.executionId, request);
    } catch {
      throw Error("browser_access_denied");
    }
    return grant.life.signal.aborted || grant.binding.revoked ? undefined : grant;
  }
  const endpoint = (binding: ExecutionBinding) => {
    let grant = [...grants.values()].find(
      (grant) => grant.binding === binding && grant.kind === "cdp" && !grant.expiresAt,
    );
    grant ??= issue(binding, "cdp");
    return {
      protocol: "browser-sandbox.host/v1",
      wsEndpoint: `${origin.replace(/^http/, "ws")}/execution/${ownership ? `${binding.executionId}/` : ""}${grant.token}`,
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
  function workerStorage(
    binding: ExecutionBinding,
    worker: ManagedWorker,
    adapter: StorageAdapter,
  ): StorageAdapter {
    return {
      load: (signal) => adapter.load(signal),
      save: (signal) => adapter.save(signal),
      async commit(result, signal) {
        await ownership?.assertOwned(binding.executionId);
        const confirmation = await adapter.commit(result, signal);
        const archive = result.archive as { sha256?: unknown } | undefined;
        if (
          binding.profileId &&
          /^[a-f0-9]{64}$/.test(confirmation?.confirmedSha) &&
          confirmation.confirmedSha === archive?.sha256 &&
          bindings.get(binding.executionId) === binding &&
          binding.incarnation === worker.incarnation &&
          binding.worker === worker
        ) {
          const commit = {
            incarnation: worker.incarnation,
            result: finalizationDetails(result),
          };
          if (!validConfirmedCommit(commit, binding.incarnation))
            throw Error("browser_profile_archive_invalid");
          binding.confirmedCommit = commit;
          await persist(binding);
        }
        return confirmation;
      },
    };
  }
  function finalizationResult(binding: ExecutionBinding, result: any) {
    if (binding.profileId && result.status === "absent") {
      if (binding.confirmedCommit?.incarnation !== binding.incarnation) {
        binding.recoveryError = "browser_profile_finalization_lost";
        throw Error(binding.recoveryError);
      }
      result = { ...binding.confirmedCommit.result, stopped: true };
    }
    if (result.stopped !== true) throw Error("browser_worker_stop_unconfirmed");
    return result;
  }
  function automationCallbacks(binding: ExecutionBinding) {
    const invoke = async (
      operation: string,
      executionId: string,
      signal?: AbortSignal,
    ) => {
      if (!binding.host || binding.host.signal.aborted)
        throw Error("browser_automation_unavailable");
      await ownership?.assertOwned(binding.executionId);
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
    if (!binding.storage) bindStorage(binding);
    binding.storagePeer = peer;
    storageAttachments.add(binding);
    const disconnect = () => {
      peer.signal.removeEventListener("abort", disconnect);
      if (binding.storagePeer !== peer) return;
      binding.storageConnection?.close();
      binding.storageConnection = undefined;
      binding.storagePeer = undefined;
      binding.storageDisconnect = undefined;
      if (!binding.profileId) binding.storage = undefined;
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
      workerStorage(binding, binding.worker, binding.storage),
      binding.storageMode === "host" && binding.storagePeer
        ? AbortSignal.any([binding.storagePeer.signal, ...(signal ? [signal] : [])])
        : signal,
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
    if (worker && ownership)
      worker.runtime.setEpoch?.(ownership.epoch(binding.executionId));
    if (
      worker &&
      (worker.profileId !== binding.profileId ||
        (binding.worker && binding.worker.id !== worker.id))
    )
      throw Error("browser_worker_ownership_mismatch");
    if (binding.worker === worker) return;
    binding.storageConnection?.close();
    binding.storageConnection = undefined;
    closeWorkerChannel(binding.worker);
    binding.worker = worker;
    binding.starting = worker ? Promise.resolve(worker) : undefined;
  }
  function closeUnusedDiscovery(workers: ManagedWorker[]) {
    const live = new Set([...bindings.values()].map((binding) => binding.worker?.id));
    for (const worker of workers) if (!live.has(worker.id)) closeWorkerChannel(worker);
  }
  async function checkGeneration(binding: ExecutionBinding, signal?: AbortSignal) {
    const status = await binding.worker.runtime.request<any>(
      { op: "worker_status", executionId: binding.executionId },
      { signal, timeoutMs: 10000 },
    );
    if (!supportedProtocol(status?.protocol))
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
    if (ownership && !ownership.owns(binding.executionId))
      throw Error("browser_controller_fenced");
    if (binding.revoked || closing) throw Error("browser_execution_revoked");
    if (binding.profileAcquired === false) throw Error("browser_profile_busy");
    if (binding.creation?.abort.signal.aborted) {
      await waitForControl(
        binding.starting.catch(() => {}),
        signal,
      );
      return ensureWorker(binding, signal);
    }
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
      binding.recovered = false;
      binding.creationFailure = undefined;
      binding.incarnation = randomUUID();
      binding.workerProtocol = WORKER_PROTOCOL;
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
              automation: binding.host ? automationCallbacks(binding) : undefined,
            },
            creationSignal,
          );
          binding.worker = worker;
          worker.runtime.setEpoch?.(ownership?.epoch(binding.executionId));
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
    if (signal.aborted) throw Error("browser_control_timeout");
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
      await recoverProfiles(request.profileId, request.executionId);
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
        await recoverProfiles(request.profileId, request.executionId);
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
          (hostStorage && binding.storageMode !== "host" && binding.profileId))
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
          profileAcquired: ownership && request.profileId ? false : undefined,
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
        if (request.profileId && !ownership)
          profileLeases.set(request.profileId, binding);
        bindings.set(id, binding);
      }
      if (ownership && binding.profileId && !binding.profileAcquired) {
        await persist(binding);
        await ownership.acquireProfile(id, binding.profileId);
        binding.profileAcquired = true;
        profileLeases.set(binding.profileId, binding);
      }
      if (
        hostStorage &&
        binding.storagePeer &&
        (request.op === "storage_attach" || binding.storagePeer !== peer) &&
        !binding.storagePeer.signal.aborted
      )
        throw Error("browser_storage_already_attached");
      if (hostStorage) {
        if (!binding.storageMode) {
          binding.storageMode = "host";
          bindStorage(binding);
        }
        bindHostStorage(binding, peer);
      }
      if (request.automation && peer) binding.host = peer;
      if (request.op === "storage_attach" && binding.starting) {
        try {
          await waitForControl(binding.starting, signal);
          if (!binding.storageConnection) await attachWorkerStorage(binding, signal);
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
      if (binding?.recoveryError === "browser_worker_protocol_unsupported")
        throw Error(binding.recoveryError);
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
          } else if (
            !binding.stopConfirmed &&
            !binding.cleanupUnconfirmed &&
            binding.recoveryError !== "browser_profile_finalization_lost"
          ) {
            worker.runtime.setAutomation?.(automationCallbacks(binding));
            await attachWorkerStorage(binding, signal);
          }
        } finally {
          closeUnusedDiscovery(workers);
        }
      }
      if (
        binding.recoveryError === "browser_profile_finalization_lost" &&
        !binding.stopConfirmed &&
        binding.confirmedCommit?.incarnation !== binding.incarnation
      )
        throw Error(binding.recoveryError);
      if (request.ifIdle && binding.worker && !binding.stopConfirmed) {
        try {
          binding.finalResult = await binding.worker.runtime.request(
            {
              op: "finalize",
              executionId: id,
              ifIdle: true,
              expectedGeneration: request.expectedGeneration,
            },
            { signal },
          );
          binding.finalResult = finalizationResult(binding, binding.finalResult);
          binding.stopConfirmed = true;
        } catch (error) {
          if (
            !["browser_worker_not_idle", "browser_session_lost"].includes(
              errorCode(error),
            )
          ) {
            revokeExecution(binding);
            binding.phase = "finalizing";
            binding.reclaimReason ??= "host-finish";
            await persist(binding).catch(() => {});
          }
          throw error;
        }
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
          result = finalizationResult(binding, result);
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
      if (ownership) await ownership.complete(id);
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
        cdpUrl: `${origin.replace(/^http/, "ws")}/execution/${ownership ? `${id}/` : ""}${agent.token}`,
        viewerUrl: `${origin}/view#${ownership ? `${id}.` : ""}${viewer.token}`,
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
    const worker = await waitForControl(binding.starting, signal);
    const result = await worker.runtime.request<any>(request, { signal });
    if (request.op === "session" && result.generation && !binding.generation) {
      binding.generation = result.generation;
      await persist(binding);
    }
    return result;
  }
  const mutations = new Map<string, Promise<unknown>>();
  function serializeExecution<T>(id: string, action: () => Promise<T>): Promise<T> {
    const work = (mutations.get(id) ?? Promise.resolve()).then(action);
    const settled = work
      .catch(() => {})
      .finally(() => {
        if (mutations.get(id) === settled) mutations.delete(id);
      });
    mutations.set(id, settled);
    return work;
  }
  async function command(
    request: any,
    peer?: ControlPeer,
    signal?: AbortSignal,
    forwarded = false,
  ) {
    if (ownership && request?.executionId) {
      const owner = await ownership.resolve(
        request.executionId,
        ["register", "storage_attach", "finish", "finalize"].includes(request.op),
      );
      if (owner && !ownership.owns(request.executionId)) {
        if (forwarded) throw Error("browser_controller_owner_moved");
        return forwarding.request(owner.origin, request, peer, signal);
      }
      if (owner) await loadOwned(request.executionId);
    }
    if (request?.op === "controller_status") return dispatch(request, peer, signal);
    return serializeExecution(request?.executionId, () =>
      dispatch(request, peer, signal),
    );
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
  let waiting = 0;
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
    const alive = new Set([client, upstream]);
    const clientPong = () => alive.add(client);
    const upstreamPong = () => alive.add(upstream);
    client.on("pong", clientPong);
    upstream.on("pong", upstreamPong);
    const heartbeat = setInterval(() => {
      for (const peer of [client, upstream]) {
        if (peer.readyState !== WebSocket.OPEN) continue;
        if (peer.isPaused) {
          alive.add(peer);
          continue;
        }
        if (!alive.delete(peer)) return close();
        peer.ping();
      }
    }, 30000);
    heartbeat.unref();
    let closed = false;
    const close = () => {
      if (closed) return;
      closed = true;
      grant.binding.lastActivityAt = Date.now();
      clearInterval(heartbeat);
      client.off("pong", clientPong);
      upstream.off("pong", upstreamPong);
      alive.clear();
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
  const handleUpgrade = async (req, socket, head, forwarded = false) => {
    const life = new AbortController();
    socket.once("close", () => life.abort());
    let grant: Access;
    try {
      if (await routeAgent(req, socket, head, forwarded)) return;
      if (req.url === "/view/socket" && allowedOrigin(req)) {
        if (closing) throw Error("browser_execution_revoked");
        if (waiting >= 64 || viewers.clients.size >= 64)
          throw Error("browser_viewer_capacity_exceeded");
        viewers.handleUpgrade(req, socket, head, (client) => {
          waiting++;
          let authenticated = false;
          const timer = setTimeout(() => client.terminate(), 5000);
          client.once("close", () => {
            clearTimeout(timer);
            if (!authenticated) waiting--;
          });
          client.on("error", () => client.terminate());
          client.once("message", async (raw) => {
            clearTimeout(timer);
            waiting--;
            authenticated = true;
            client.pause();
            let grant: Access;
            const reservation = { destroy: () => client.terminate() };
            try {
              let auth: { type?: string; token?: string } | null;
              try {
                auth = JSON.parse(String(raw));
              } catch {
                throw Error("browser_access_denied");
              }
              if (ownership && !req.executionId && auth?.type === "authenticate") {
                const id = await ownership.findCapability(auth.token);
                if (id) {
                  const owner = await ownership.resolve(id);
                  if (owner && !ownership.owns(id)) {
                    if (forwarded) throw Error("browser_controller_owner_moved");
                    forwardControllerViewer(client, owner.origin, id, auth.token);
                    return;
                  }
                  await loadOwned(id);
                }
              }
              grant =
                auth?.type === "authenticate" &&
                (await access(auth.token, "view", req));
              if (
                !grant ||
                grant.connections.size >= (grant.control || grant.resize ? 1 : 4)
              )
                throw Error("browser_access_denied");
              grant.connections.add(reservation);
              client.once("close", () => grant.connections.delete(reservation));
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
            } catch (error) {
              client.resume();
              const status = errorStatus(error, grant);
              client.close(
                status === 403 ? 1008 : status === 503 ? 1013 : 1011,
                errorCode(error, grant).slice(0, 123),
              );
            } finally {
              grant?.connections.delete(reservation);
            }
          });
        });
        return;
      }
      const match = /^\/execution\/([a-f0-9]{64})$/.exec(req.url ?? "");
      grant = match && (await access(match[1], "cdp", req));
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
    } catch (error) {
      const status = errorStatus(error, grant);
      const body = JSON.stringify({ error: errorCode(error, grant) });
      const reason =
        status === 403
          ? "Forbidden"
          : status === 503
            ? "Service Unavailable"
            : "Bad Gateway";
      socket.end(
        `HTTP/1.1 ${status} ${reason}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: ${Buffer.byteLength(body)}\r\n\r\n${body}`,
      );
    }
  };
  server.on("upgrade", handleUpgrade);
  async function restoreRecord(
    record,
    discovered: ManagedWorker[],
    owned: Map<string, ManagedWorker>,
    finishedRevisions = new Map<string, number>(),
  ) {
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
        (item) => item.executionId === id && item.incarnation === data.incarnation,
      );
    if (
      worker &&
      (worker.executionId !== id ||
        worker.incarnation !== data.incarnation ||
        worker.profileId !== data.profileId)
    )
      throw Error("browser_worker_ownership_mismatch");
    if (ownership && data.profileId && data.profileAcquired !== false)
      await ownership.acquireProfile(id, data.profileId);
    const binding: ExecutionBinding = {
      executionId: id,
      incarnation: data.incarnation,
      profileId: data.profileId,
      profileAcquired: data.profileAcquired,
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
      confirmedCommit: data.confirmedCommit,
      generation: data.generation,
      // Journal v1 originally described worker protocol v1.
      workerProtocol: data.workerProtocol ?? "browser-sandbox.worker/v1",
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
    if (
      binding.profileId &&
      !binding.infrastructureOnly &&
      binding.profileAcquired !== false
    ) {
      if (!validProfileId(binding.profileId) || profileLeases.has(binding.profileId))
        throw Error("browser_profile_lease_recovery_failed");
      profileLeases.set(binding.profileId, binding);
      bindStorage(binding);
    }
    if (worker) {
      owned.delete(worker.id);
      binding.starting = Promise.resolve(worker);
      binding.slot = admission.restore();
      try {
        if (!binding.stopConfirmed && !supportedProtocol(binding.workerProtocol))
          throw Error("browser_worker_protocol_unsupported");
        if (ownership)
          await options.backend.fence?.(
            worker,
            ownership.epoch(id),
            AbortSignal.timeout(15000),
            binding.stopConfirmed,
          );
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
        if (!supportedProtocol(status.protocol))
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
        binding.statusError = binding.recoveryError;
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
      if (!/^[a-f0-9]{64}$/.test(grant.token) || !["cdp", "view"].includes(grant.kind))
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
  }
  async function restoreOrphan(worker: ManagedWorker, revision: number) {
    if (bindings.has(worker.executionId))
      throw Error("browser_worker_ownership_mismatch");
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
      if (ownership && !worker.infrastructureOnly)
        await options.backend.fence?.(
          worker,
          ownership.epoch(worker.executionId),
          AbortSignal.timeout(15000),
        );
      if (worker.infrastructureOnly) {
        bindings.set(binding.executionId, binding);
        await persist(binding);
        return;
      }
      await attachWorkerStorage(binding);
      const status = await worker.runtime.request<any>(
        { op: "worker_status", executionId: worker.executionId },
        { timeoutMs: 10000 },
      );
      if (typeof status.protocol === "string") binding.workerProtocol = status.protocol;
      if (!supportedProtocol(status.protocol))
        throw Error("browser_worker_protocol_unsupported");
      binding.phase = "finalizing";
    } catch (error) {
      binding.recoveryError = errorCode(error);
    }
    bindings.set(binding.executionId, binding);
    await persist(binding);
  }
  function dropOwned(id: string) {
    const binding = bindings.get(id);
    if (!binding) return;
    revokeExecution(binding);
    binding.storageDisconnect?.();
    binding.storageConnection?.close();
    closeWorkerChannel(binding.worker);
    binding.slot?.release();
    bindings.delete(id);
    if (binding.profileId) profileLeases.delete(binding.profileId);
  }
  const loading = new Map<string, Promise<void>>();
  function loadOwned(id: string): Promise<void> {
    if (bindings.has(id) || (finished.has(id) && !ownership)) return Promise.resolve();
    let pending = loading.get(id);
    if (!pending) {
      pending = (async () => {
        await ownership.assertOwned(id);
        const record = await ownership.journal.read(id);
        if (!record) return;
        if (record.data.phase === "finished") {
          await ownership.complete(id);
          rememberFinished(id);
          return;
        }
        const discovered = await options.backend.list(AbortSignal.timeout(30000));
        try {
          await restoreRecord(
            record,
            discovered,
            new Map(discovered.map((worker) => [worker.id, worker])),
          );
        } finally {
          closeUnusedDiscovery(discovered);
        }
      })().finally(() => loading.delete(id));
      loading.set(id, pending);
    }
    return pending;
  }
  async function refreshOwnership() {
    const ids = await ownership.refresh();
    for (const id of ids) {
      await ownership.resolve(id);
      if (ownership.owns(id)) await loadOwned(id);
    }
    const discovered = await options.backend.list(AbortSignal.timeout(30000));
    try {
      for (const worker of discovered) {
        if (bindings.has(worker.executionId)) continue;
        await ownership.resolve(worker.executionId, true);
        if (!ownership.owns(worker.executionId)) continue;
        const record = await journal.read(worker.executionId);
        if (record?.data.phase === "finished") {
          const current = await options.backend.list(AbortSignal.timeout(30000));
          try {
            const remaining = current.find((item) => item.id === worker.id);
            if (remaining) await restoreOrphan(remaining, record.revision);
            else await ownership.complete(worker.executionId);
          } finally {
            closeUnusedDiscovery(current);
          }
          continue;
        }
        await loadOwned(worker.executionId);
        if (ownership.owns(worker.executionId) && !bindings.has(worker.executionId))
          await restoreOrphan(worker, record?.revision ?? 0);
      }
    } finally {
      closeUnusedDiscovery(discovered);
    }
  }
  async function routeAgent(req, destination, head?: Buffer, forwarded = false) {
    if (!ownership) return false;
    const match = /^\/execution\/([a-f0-9-]{36})\/(.+)$/.exec(req.url ?? "");
    const legacy = /^\/execution\/([a-f0-9]{64})(?:\/|$)/.exec(req.url ?? "");
    const id =
      match?.[1] ?? (legacy ? await ownership.findCapability(legacy[1]) : undefined);
    if (!id) return false;
    const owner = await ownership.resolve(id);
    if (!owner) throw Error("browser_access_denied");
    if (!ownership.owns(id)) {
      if (forwarded) throw Error("browser_controller_owner_moved");
      forwardControllerRequest(req, destination, owner.origin, head);
      return true;
    }
    await loadOwned(id);
    req.executionId = id;
    if (match)
      req.url = match[2] === "view/socket" ? "/view/socket" : `/execution/${match[2]}`;
    return false;
  }
  const peerServer = ownership
    ? createServer((req, res) => handleAgent(req, res, true))
    : undefined;
  const peerControl = peerServer
    ? createNetworkHostControl({
        server: peerServer,
        dispatch: (request, peer, signal) => command(request, peer, signal, true),
      })
    : undefined;
  peerServer?.on("upgrade", (req, socket, head) => {
    if (req.url !== "/host") void handleUpgrade(req, socket, head, true);
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
      [server, controlServer, peerServer]
        .filter((listener) => listener)
        .filter((listener) => listener.listening)
        .map(
          (listener) => new Promise<void>((resolve) => listener.close(() => resolve())),
        ),
    );
    throw error;
  }
  try {
    if (ownership) {
      journal = ownership.journal;
      ownership.onLost(dropOwned);
      await refreshOwnership();
      void ownership.watch();
    } else if (options.stateDirectory) {
      if (!options.backend.list) throw Error("browser_worker_discovery_unavailable");
      const discovered = await options.backend.list(AbortSignal.timeout(30000));
      discoveredWorkers = discovered;
      const owned = new Map(discovered.map((worker) => [worker.id, worker]));
      const finishedRevisions = new Map<string, number>();
      journal = await createExecutionJournal({
        directory: options.stateDirectory,
        installationId: options.installationId,
        onRecord: (record) =>
          restoreRecord(record, discovered, owned, finishedRevisions),
      });
      for (const id of journal.finishedIds()) rememberFinished(id);
      for (const worker of owned.values())
        await restoreOrphan(
          worker,
          journal.has(worker.executionId)
            ? (finishedRevisions.get(worker.executionId) ?? 0)
            : 0,
        );
    }
  } catch (error) {
    await constructionFailed(error);
  }
  let reconciliation: Promise<void> = Promise.resolve();
  function reconcile(): Promise<void> {
    const run = async () => {
      if (closing) return;
      if (ownership) await refreshOwnership();
      const currentBindings = [...bindings.values()];
      const recoveryIncarnations = new Map<ExecutionBinding, string>();
      for (const binding of currentBindings)
        if ((binding.revoked || binding.cleanupUnconfirmed) && !binding.creation)
          recoveryIncarnations.set(binding, binding.incarnation);
      let discovery: Promise<ManagedWorker[]> | undefined;
      let discovered: ManagedWorker[] | undefined;
      try {
        for (const binding of currentBindings) {
          if (closing) break;
          await serializeExecution(binding.executionId, async () => {
            if (
              closing ||
              bindings.get(binding.executionId) !== binding ||
              (ownership && !ownership.owns(binding.executionId))
            )
              return;
            try {
              if (binding.revoked || binding.cleanupUnconfirmed) {
                // A shared snapshot cannot prove absence for a newer recovery incarnation.
                if (recoveryIncarnations.get(binding) !== binding.incarnation) return;
                if (binding.phase === "recovery-blocked") {
                  if (binding.profileId && !binding.stopConfirmed) return;
                  binding.phase = binding.stopConfirmed
                    ? "cleanup-pending"
                    : "recovery-required";
                }
                if (Date.now() < (binding.recoveryRetryAt ?? 0)) return;
                if (
                  binding.recoveryError === "browser_worker_protocol_unsupported" &&
                  binding.profileId &&
                  !binding.stopConfirmed
                ) {
                  // Preserve the durable descriptor and grants so a compatible controller can roll back.
                  binding.phase = "recovery-blocked";
                  event(binding, "recovery-blocked", { error: binding.recoveryError });
                  return;
                }
                const attempts = Math.min(3, (binding.recoveryAttempts ?? 0) + 1);
                if (attempts !== binding.recoveryAttempts) {
                  binding.recoveryAttempts = attempts;
                  await persist(binding, "recovery-attempt");
                }
                if (options.backend.list) {
                  discovered = await (discovery ??= options.backend.list(
                    AbortSignal.timeout(30000),
                  ));
                  const worker = discovered.find(
                    (worker) =>
                      worker.executionId === binding.executionId &&
                      worker.incarnation === binding.incarnation,
                  );
                  if (!worker) {
                    adoptRecoveredWorker(binding, undefined);
                    binding.cleanupUnconfirmed = false;
                    binding.stopConfirmed = true;
                    binding.finalResult ??= {
                      stopped: true,
                      used: true,
                      graceful: false,
                    };
                    binding.phase = "cleanup-pending";
                  } else if (
                    binding.recoveryError === "browser_worker_protocol_unsupported" &&
                    !binding.stopConfirmed
                  ) {
                    throw Error(binding.recoveryError);
                  } else if (!binding.cleanupUnconfirmed && !binding.stopConfirmed) {
                    const retryingSave = binding.phase === "finalizing";
                    const sameResource =
                      binding.worker?.id === worker.id &&
                      worker.profileId === binding.profileId;
                    const controlFailed =
                      /^browser_control_(disconnected|unavailable|timeout)$/.test(
                        binding.statusError ?? "",
                      );
                    if (!retryingSave || !sameResource || controlFailed)
                      adoptRecoveredWorker(binding, worker);
                    const currentWorker = binding.worker;
                    try {
                      currentWorker.runtime.setAutomation?.(
                        automationCallbacks(binding),
                      );
                      // A failed save has already revoked the worker's live session.
                      if (!retryingSave) {
                        const status = await currentWorker.runtime.request<any>(
                          { op: "worker_status", executionId: binding.executionId },
                          { timeoutMs: 10000 },
                        );
                        if (!supportedProtocol(status.protocol))
                          throw Error("browser_worker_protocol_unsupported");
                      }
                      if (!binding.storageConnection)
                        await attachWorkerStorage(binding, AbortSignal.timeout(10000));
                      await dispatch({
                        op: "finalize",
                        executionId: binding.executionId,
                      });
                    } catch (error) {
                      if (
                        !binding.profileId &&
                        binding.recoveryAttempts >= 3 &&
                        errorCode(error) !== "browser_worker_protocol_unsupported"
                      ) {
                        try {
                          await removeWorker(currentWorker, AbortSignal.timeout(30000));
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
                        await dispatch({
                          op: "finalize",
                          executionId: binding.executionId,
                        });
                      } else throw error;
                    }
                    return;
                  }
                }
                if (binding.recoveryError === "browser_worker_protocol_unsupported") {
                  if (!binding.stopConfirmed) throw Error(binding.recoveryError);
                  binding.recoveryError = undefined;
                }
                if (
                  binding.worker ||
                  binding.cleanupUnconfirmed ||
                  ["cleanup-pending", "finalizing"].includes(binding.phase) ||
                  binding.stopConfirmed
                )
                  await dispatch({ op: "finalize", executionId: binding.executionId });
                return;
              }
              const now = Date.now();
              if (now - binding.registeredAt >= maxExecutionMs) {
                binding.reclaimReason = "maximum-lifetime";
                await dispatch({ op: "finalize", executionId: binding.executionId });
                return;
              }
              if (now - binding.lastActivityAt < idleRegistrationMs || binding.creation)
                return;
              if (!binding.worker) {
                binding.reclaimReason = "idle";
                await dispatch({ op: "finalize", executionId: binding.executionId });
                return;
              }
              const status = await binding.worker.runtime.request<any>(
                { op: "worker_status", executionId: binding.executionId },
                { timeoutMs: 10000 },
              );
              binding.checkedAt = Date.now();
              binding.statusError = undefined;
              binding.idleProtection = workerIdleProtection(status, now, !ownership);
              if (binding.idleProtection) return;
              binding.reclaimReason = "idle";
              await dispatch({
                op: "finalize",
                executionId: binding.executionId,
                ifIdle: true,
                expectedGeneration: status.session.generation,
              });
            } catch (error) {
              binding.checkedAt = Date.now();
              binding.statusError = errorCode(error);
              binding.idleProtection = "unknown";
              if (binding.revoked)
                binding.recoveryRetryAt =
                  binding.checkedAt +
                  1000 * 2 ** Math.max(0, (binding.recoveryAttempts ?? 1) - 1);
              if (
                binding.revoked &&
                binding.profileId &&
                !binding.stopConfirmed &&
                binding.statusError === "browser_worker_protocol_unsupported"
              ) {
                binding.phase = "recovery-blocked";
                binding.recoveryError = binding.statusError;
                event(binding, "recovery-blocked", { error: binding.recoveryError });
              } else if (
                binding.revoked &&
                binding.profileId &&
                !binding.stopConfirmed &&
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
          });
        }
      } finally {
        if (discovered) closeUnusedDiscovery(discovered);
      }
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
  if (peerServer)
    await new Promise<void>((resolve, reject) => {
      peerServer.once("error", reject);
      peerServer.listen(
        options.peerPort ?? 8084,
        options.peerHost ?? "0.0.0.0",
        resolve,
      );
    }).catch(constructionFailed);
  const renewal = ownership
    ? setInterval(() => {
        void ownership.renew().catch(() => {});
      }, 5000)
    : undefined;
  renewal?.unref();
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
      clearInterval(renewal);
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
      if (ownership) {
        try {
          await ownership.release();
        } catch (error) {
          failure ??= error;
        } finally {
          await ownership.close();
        }
      }
      forwarding.close();
      peerControl?.close();
      control.close();
      for (const grant of [...grants.values()]) revoke(grant);
      for (const client of [...viewers.clients, ...cdp.clients]) client.terminate();
      viewers.close();
      cdp.close();
      for (const socket of rawConnections) socket.destroy();
      controlServer.closeAllConnections();
      peerServer?.closeAllConnections();
      await Promise.all(
        [server, controlServer, peerServer]
          .filter(Boolean)
          .map(
            (listener) =>
              new Promise<void>((resolve) => listener.close(() => resolve())),
          ),
      );
      if (failure) throw failure;
    },
  };
}
function errorCode(error: any, grant?: Access) {
  // Session loss revokes its own grant; preserve the cause for the detecting request.
  if (
    error?.message === "browser_session_lost" &&
    grant?.binding.recoveryError === "browser_session_lost"
  )
    return "browser_session_lost";
  if (grant?.life.signal.aborted || grant?.binding.revoked)
    return "browser_execution_revoked";
  return /^browser_[a-z_]+$/.test(error?.message)
    ? error.message
    : "browser_worker_unavailable";
}

function errorStatus(error: unknown, grant?: Access) {
  const code = errorCode(error, grant);
  if (["browser_controller_owner_moved", "browser_controller_fenced"].includes(code))
    return 503;
  if (["browser_access_denied", "browser_execution_revoked"].includes(code)) return 403;
  return /_(capacity_exceeded|overloaded)$/.test(code) ? 503 : 502;
}

function finalizationDetails(result: any) {
  return {
    ...(typeof result.used === "boolean" ? { used: result.used } : {}),
    ...(typeof result.graceful === "boolean" ? { graceful: result.graceful } : {}),
    ...(result.archive
      ? {
          archive: { sha256: result.archive.sha256, byteSize: result.archive.byteSize },
        }
      : {}),
  };
}
function validConfirmedCommit(commit: any, incarnation: string) {
  const result = commit?.result;
  const archive = result?.archive;
  return (
    !!commit &&
    typeof commit === "object" &&
    !Array.isArray(commit) &&
    typeof commit.incarnation === "string" &&
    commit.incarnation === incarnation &&
    Object.keys(commit).every((key) => ["incarnation", "result"].includes(key)) &&
    !!result &&
    typeof result === "object" &&
    !Array.isArray(result) &&
    Object.keys(result).every((key) => ["used", "graceful", "archive"].includes(key)) &&
    ["used", "graceful"].every(
      (key) => result[key] === undefined || typeof result[key] === "boolean",
    ) &&
    !!archive &&
    typeof archive === "object" &&
    !Array.isArray(archive) &&
    Object.keys(archive).every((key) => ["sha256", "byteSize"].includes(key)) &&
    typeof archive.sha256 === "string" &&
    /^[a-f0-9]{64}$/.test(archive.sha256) &&
    Number.isSafeInteger(archive.byteSize) &&
    archive.byteSize > 0 &&
    archive.byteSize <= MAX_ARCHIVE_BYTES
  );
}
export function validateControllerRecord(data: any) {
  const invalid = () => {
    throw Error("browser_execution_journal_corrupt");
  };
  if (!data || typeof data !== "object" || Array.isArray(data)) invalid();
  if (
    data.workerProtocol !== undefined &&
    (typeof data.workerProtocol !== "string" ||
      !/^browser-sandbox\.worker\/v[1-9][0-9]{0,3}$/.test(data.workerProtocol))
  )
    invalid();
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
  for (const key of [
    "stopConfirmed",
    "cleanupUnconfirmed",
    "infrastructureOnly",
    "profileAcquired",
  ])
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
    data.confirmedCommit !== undefined &&
    (!data.profileId || !validConfirmedCommit(data.confirmedCommit, data.incarnation))
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
