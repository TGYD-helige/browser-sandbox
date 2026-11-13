import { randomBytes, randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createServer, request as httpRequest } from "node:http";
import type { AddressInfo } from "node:net";
import { pipeline, Transform } from "node:stream";
import WebSocket, { createWebSocketStream, WebSocketServer } from "ws";
import { waitForControl } from "./automation-control.js";
import { MAX_FILE_BYTES } from "./files.js";
import type { Registration } from "./host-client.js";
import { WorkerCleanupFailure } from "./lifecycle-errors.js";
import type { ManagedBackend, ManagedWorker } from "./managed-backend.js";
import { type ControlPeer, createNetworkHostControl } from "./network-control.js";
import { normalizeAllowedDomains } from "./network-policy.js";
import type { SessionStatus } from "./session.js";

export interface ExecutionBinding {
  executionId: string;
  incarnation: string;
  allowedDomains: string[];
  worker?: ManagedWorker;
  starting?: Promise<ManagedWorker>;
  creation?: { abort: AbortController; waiters: number };
  creationFailure?: unknown;
  endpoint?: { cdpUrl: string; viewerToken: string };
  life: AbortController;
  revoked: boolean;
  finishing?: Promise<any>;
  host?: ControlPeer;
}
interface StorageAttachment {
  registration: Registration;
  peer: ControlPeer;
  channel?: { close(): void };
  cleanup(): void;
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
  port?: number;
  host?: string;
  controlPort?: number;
  controlHost?: string;
  publicOrigin?: string;
  viewerOrigins?: string[];
  creationTimeoutMs?: number;
  onWorkerEvent?: (event: {
    type: "creating" | "ready" | "failed" | "removed";
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
  const finished = new Set<string>();
  const storage = new Map<string, StorageAttachment>();
  const rememberFinished = (id: string) => {
    finished.add(id);
    if (finished.size > 65536) finished.delete(finished.values().next().value);
  };
  let closing = false;
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
        res
          .writeHead(errorStatus(error))
          .end(JSON.stringify({ error: errorCode(error) }));
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
  function issue(
    binding: ExecutionBinding,
    kind: Access["kind"],
    durationMs?: number,
    control = false,
    resize = false,
  ) {
    if (grants.size >= 2000) throw Error("browser_access_capacity_exceeded");
    const token = randomBytes(32).toString("hex");
    const grant: Access = {
      token,
      binding,
      kind,
      control,
      resize,
      life: new AbortController(),
      connections: new Set(),
      expiresAt: durationMs ? Date.now() + durationMs : undefined,
    };
    grants.set(token, grant);
    if (durationMs) {
      grant.timer = setTimeout(() => revoke(grant), durationMs);
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
      wsEndpoint: `${origin.replace(/^http/, "ws")}/execution/${grant.token}`,
    };
  };
  const event = (
    binding: ExecutionBinding,
    type: "creating" | "ready" | "failed" | "removed",
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
  async function attachWorkerStorage(
    worker: ManagedWorker,
    attachment: StorageAttachment,
    signal: AbortSignal,
  ) {
    const executionId = attachment.registration.executionId;
    const channel = await worker.runtime.attachStorage(
      attachment.registration,
      {
        load: (signal) => attachment.peer.callback("load", { executionId }, signal),
        save: (signal) => attachment.peer.callback("save", { executionId }, signal),
        commit: (payload, signal) =>
          attachment.peer.callback("commit", { executionId, payload }, signal),
      },
      signal,
    );
    if (attachment.peer.signal.aborted || storage.get(executionId) !== attachment) {
      channel.close();
      throw Error("browser_storage_disconnected");
    }
    attachment.channel = channel;
  }
  async function removeWorker(worker: ManagedWorker, signal: AbortSignal) {
    let failure: unknown;
    try {
      await options.backend.remove(worker, signal);
    } catch (error) {
      failure = error;
    }
    try {
      worker.runtime.close?.();
    } catch (error) {
      failure = failure ? new WorkerCleanupFailure(failure, error) : error;
    }
    if (failure) throw failure;
  }
  async function ensureWorker(binding: ExecutionBinding, signal: AbortSignal) {
    signal.throwIfAborted();
    if (binding.revoked) throw Error("browser_execution_revoked");
    if (binding.creation?.abort.signal.aborted) {
      await waitForControl(
        binding.starting.catch(() => {}),
        signal,
      );
      return ensureWorker(binding, signal);
    }
    if (binding.creationFailure instanceof WorkerCleanupFailure)
      throw binding.creationFailure;
    if (binding.worker && !binding.endpoint)
      throw binding.creationFailure ?? Error("browser_worker_cleanup_unconfirmed");
    if (!binding.starting) {
      const attempt = { abort: new AbortController(), waiters: 0 };
      binding.creation = attempt;
      binding.creationFailure = undefined;
      binding.incarnation = randomUUID();
      const creationSignal = AbortSignal.any([
        binding.life.signal,
        attempt.abort.signal,
        AbortSignal.timeout(options.creationTimeoutMs ?? 90000),
      ]);
      const startedAt = performance.now();
      event(binding, "creating");
      const starting = (async () => {
        const worker = await options.backend.create(
          {
            executionId: binding.executionId,
            incarnation: binding.incarnation,
            allowedDomains: binding.allowedDomains,
            automation: binding.host
              ? {
                  pause: async (executionId, signal) => {
                    await binding.host.callback("pause", { executionId }, signal);
                  },
                  resume: async (executionId, signal) => {
                    await binding.host.callback("resume", { executionId }, signal);
                  },
                }
              : undefined,
          },
          creationSignal,
        );
        binding.worker = worker;
        try {
          creationSignal.throwIfAborted();
          const result = await worker.runtime.request<{
            cdpUrl: string;
            viewerToken: string;
          }>(
            {
              op: "register",
              executionId: binding.executionId,
              allowedDomains: binding.allowedDomains,
            },
            { signal: creationSignal },
          );
          creationSignal.throwIfAborted();
          const attachment = storage.get(binding.executionId);
          if (attachment) await attachWorkerStorage(worker, attachment, creationSignal);
          creationSignal.throwIfAborted();
          binding.endpoint = result;
          binding.creation = undefined;
          event(binding, "ready", { elapsedMs: performance.now() - startedAt });
          return worker;
        } catch (error) {
          try {
            await removeWorker(worker, AbortSignal.timeout(30000));
            binding.worker = undefined;
          } catch (cleanup) {
            throw new WorkerCleanupFailure(error, cleanup);
          }
          throw error;
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
      if (!attempt.waiters && !binding.endpoint) attempt.abort.abort();
    }
  }
  async function command(
    request: any,
    peer?: ControlPeer,
    signal = AbortSignal.timeout(180000),
  ): Promise<any> {
    signal.throwIfAborted();
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
    if (request.op === "storage_attach") {
      if (
        !peer ||
        peer.signal.aborted ||
        closing ||
        finished.has(id) ||
        binding?.revoked
      )
        throw Error("browser_storage_disconnected");
      if (storage.has(id)) throw Error("browser_storage_already_attached");
      if (storage.size >= 2000) throw Error("browser_access_capacity_exceeded");
      const attachment = {
        registration: { executionId: id, profileId: request.profileId },
        peer,
        channel: undefined as { close(): void } | undefined,
        cleanup() {
          attachment.channel?.close();
          peer.signal.removeEventListener("abort", attachment.cleanup);
          if (storage.get(id) === attachment) storage.delete(id);
        },
      };
      storage.set(id, attachment);
      peer.signal.addEventListener("abort", attachment.cleanup, { once: true });
      try {
        if (binding?.starting) {
          const worker = await waitForControl(binding.starting, signal);
          signal.throwIfAborted();
          if (!attachment.channel)
            await attachWorkerStorage(worker, attachment, signal);
        }
        return { attached: true };
      } catch (error) {
        attachment.cleanup();
        throw error;
      }
    }
    if (request.op === "storage_detach") {
      const attachment = storage.get(id);
      if (attachment && attachment.peer !== peer)
        throw Error("browser_host_authority_required");
      attachment?.cleanup();
      return { detached: true };
    }
    if (request.op === "register") {
      if (closing || finished.has(id) || binding?.revoked)
        throw Error("browser_execution_revoked");
      if (request.tenantId !== undefined)
        throw Error("browser_tenant_isolation_unavailable");
      if (request.profileId !== undefined || request.storage)
        throw Error("browser_profile_storage_unavailable");
      const domains = normalizeAllowedDomains(request.allowedDomains);
      if (binding && JSON.stringify(binding.allowedDomains) !== JSON.stringify(domains))
        throw Error("browser_domains_changed");
      if (!binding) {
        if (bindings.size >= 65536) throw Error("browser_execution_capacity_exceeded");
        binding = {
          executionId: id,
          incarnation: randomUUID(),
          allowedDomains: domains,
          life: new AbortController(),
          revoked: false,
          host: request.automation ? peer : undefined,
        };
        bindings.set(id, binding);
      }
      return endpoint(binding);
    }
    if (["finalize", "finish"].includes(request.op)) {
      if (!binding) {
        storage.get(id)?.cleanup();
        return { status: "absent" };
      }
      if (binding.finishing) return binding.finishing;
      const current = binding;
      current.finishing = (async () => {
        current.revoked = true;
        current.life.abort();
        for (const grant of [...grants.values()])
          if (grant.binding === current) revoke(grant);
        await current.starting?.catch(() => {});
        if (!current.worker && current.creationFailure instanceof WorkerCleanupFailure)
          throw current.creationFailure;
        let result = { used: false, stopped: true };
        if (current.worker) {
          try {
            result = await current.worker.runtime.request(
              { op: "finalize", executionId: id },
              { signal },
            );
          } catch (error) {
            if (
              ![
                "browser_control_disconnected",
                "browser_control_unavailable",
                "browser_control_timeout",
              ].includes(error.message)
            ) {
              try {
                current.worker.runtime.close?.();
              } catch (cleanup) {
                throw new WorkerCleanupFailure(error, cleanup);
              }
              throw error;
            }
            try {
              await removeWorker(current.worker, AbortSignal.timeout(30000));
            } catch (cleanup) {
              throw new WorkerCleanupFailure(error, cleanup);
            }
            storage.get(id)?.cleanup();
            bindings.delete(id);
            rememberFinished(id);
            event(current, "removed", { error: errorCode(error) });
            throw Error("browser_worker_finalization_failed");
          }
          await removeWorker(current.worker, signal);
        }
        storage.get(id)?.cleanup();
        bindings.delete(id);
        rememberFinished(id);
        event(current, "removed");
        return result;
      })().catch((error) => {
        current.finishing = undefined;
        throw error;
      });
      return current.finishing;
    }
    if (!binding || binding.revoked || closing)
      throw Error("browser_execution_revoked");
    if (request.op === "endpoint") return endpoint(binding);
    if (request.op === "access_create") {
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
      return { revoked: true };
    }
    if (request.op === "session" && !binding.starting) {
      if ((request.action ?? "status") === "status")
        return { state: "unused", connectedClients: 0 } satisfies SessionStatus;
      throw Error("browser_session_not_live");
    }
    if (!binding.starting) throw Error("browser_session_not_live");
    const worker = await waitForControl(binding.starting, signal);
    return worker.runtime.request(request, { signal });
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
    upstream.on("open", () => {
      if (grant.life.signal.aborted) return close();
      if (auth) upstream.send(JSON.stringify(auth));
      downstream.pipe(incoming).pipe(downstream);
    });
    upstream.on("error", close);
  }
  const cdp = new WebSocketServer({ noServer: true, maxPayload: 32 * 1024 * 1024 });
  server.on("upgrade", async (req, socket, head) => {
    const life = new AbortController();
    socket.once("close", () => life.abort());
    try {
      if (req.url === "/view/socket" && allowedOrigin(req)) {
        if (waiting >= 64 || viewers.clients.size >= 64 || closing)
          throw Error("browser_access_denied");
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
              const status = errorStatus(error);
              client.close(
                status === 403 ? 1008 : status === 503 ? 1013 : 1011,
                errorCode(error).slice(0, 123),
              );
            } finally {
              grant?.connections.delete(reservation);
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
    } catch (error) {
      const status = errorStatus(error);
      const body = JSON.stringify({ error: errorCode(error) });
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
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(options.port ?? 8080, options.host ?? "127.0.0.1", resolve);
  });
  try {
    await new Promise<void>((resolve, reject) => {
      controlServer.once("error", reject);
      controlServer.listen(
        options.controlPort ?? 0,
        options.controlHost ?? "127.0.0.1",
        resolve,
      );
    });
  } catch (error) {
    control.close();
    cdp.close();
    viewers.close();
    for (const socket of rawConnections) socket.destroy();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    throw error;
  }
  origin ??= `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  return {
    controlOrigin: `http://127.0.0.1:${(controlServer.address() as AddressInfo).port}`,
    origin,
    command,
    bindings,
    async stop({ preserveWorkers = false }: { preserveWorkers?: boolean } = {}) {
      closing = true;
      let failure: unknown;
      if (preserveWorkers) {
        for (const binding of bindings.values()) {
          binding.revoked = true;
          if (!binding.endpoint) binding.life.abort();
        }
      }
      for (const binding of bindings.values()) {
        try {
          if (preserveWorkers) {
            if (!binding.endpoint) {
              await binding.starting?.catch((error) => {
                if (
                  error?.name !== "AbortError" &&
                  error?.message !== "browser_worker_creation_cancelled"
                )
                  throw error;
              });
            }
          } else {
            await command({ op: "finalize", executionId: binding.executionId });
          }
        } catch (error) {
          failure ??= error;
        } finally {
          if (preserveWorkers) binding.worker?.runtime.close?.();
        }
      }
      for (const attachment of storage.values()) attachment.cleanup();
      control.close();
      for (const grant of [...grants.values()]) revoke(grant);
      for (const client of [...viewers.clients, ...cdp.clients]) client.terminate();
      viewers.close();
      cdp.close();
      for (const socket of rawConnections) socket.destroy();
      controlServer.closeAllConnections();
      await Promise.all([
        new Promise<void>((resolve) => server.close(() => resolve())),
        new Promise<void>((resolve) => controlServer.close(() => resolve())),
      ]);
      if (failure) throw failure;
    },
  };
}
function errorCode(error: any) {
  return /^browser_[a-z_]+$/.test(error?.message)
    ? error.message
    : "browser_worker_unavailable";
}

function errorStatus(error: unknown) {
  const code = errorCode(error);
  if (code === "browser_access_denied") return 403;
  return /_(capacity_exceeded|overloaded)$/.test(code) ? 503 : 502;
}
