import { randomBytes, randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { type AutomationCoordinator, waitForControl } from "./automation-control.js";
import { attachCdpProxy } from "./cdp-proxy.js";
import { createHttpHandler } from "./http-routes.js";

export type { AutomationCoordinator } from "./automation-control.js";
export interface AccessOptions {
  port: number;
  host?: string;
  publicOrigin?: string;
  viewerOrigins?: string[];
  automation?: AutomationCoordinator;
  /** Test/local development only; production authentication requires HTTPS. */
  allowInsecureAuthLoopback?: boolean;
}
interface Grant {
  token: string;
  entry: any;
  kind: "cdp" | "view";
  control: boolean;
  resize: boolean;
  actorId: string;
  leaseId: string;
  epoch?: string;
  browser?: Promise<unknown>;
  browserSignal?: AbortSignal;
  expiresAt: number;
  life: AbortController;
  connections: Map<any, (browserEnded: boolean) => boolean>;
  timer: ReturnType<typeof setTimeout>;
  cleanup(): void;
  browserEnded(): void;
}
const errorCode = (error: any) =>
  /^browser_[a-z_]+$/.test(error?.message)
    ? error.message
    : "browser_view_request_failed";

const assets = new Map([
  ["/view", ["index.html", "text/html; charset=utf-8"]],
  ["/viewer.js", ["browser-viewer.js", "text/javascript; charset=utf-8"]],
  ["/viewer.css", ["viewer.css", "text/css; charset=utf-8"]],
]);

/** Optional trusted remote entry point. The UID-authenticated local listener stays private. */
export async function createAccessServer({
  executions,
  WebSocket,
  WebSocketServer,
  options,
  fileTransfers,
  authentication,
  automationControl,
  healthy = async () => true,
}: any) {
  const config = options as AccessOptions;
  // Private worker transport carries execution references and trusted viewer context, never grants.
  const worker = options.workerExecutionId as string | undefined;
  let workerEntry: any;
  const references = new Map<string, Grant>();
  const grants = new Map<string, Grant>();
  const viewportOwners = new Map<any, Grant>();
  let origin = config.publicOrigin;
  if (origin) {
    const url = new URL(origin);
    if (
      !["http:", "https:"].includes(url.protocol) ||
      url.origin !== origin ||
      url.username ||
      url.password
    )
      throw Error("browser_access_origin_invalid");
    if (
      !worker &&
      url.protocol === "http:" &&
      !["127.0.0.1", "localhost", "[::1]"].includes(url.hostname)
    )
      throw Error("browser_access_tls_required");
  }
  if (
    !origin &&
    config.host &&
    !["127.0.0.1", "localhost", "::1"].includes(config.host)
  )
    throw Error("browser_access_origin_required");
  const viewerOrigins = new Set<string>(config.viewerOrigins ?? []);
  if (viewerOrigins.size > 8) throw Error("browser_access_origin_invalid");
  for (const value of viewerOrigins) {
    const url = new URL(value);
    if (
      url.origin !== value ||
      (url.protocol !== "https:" &&
        !(
          url.protocol === "http:" &&
          ["127.0.0.1", "localhost", "[::1]"].includes(url.hostname)
        ))
    )
      throw Error("browser_access_origin_invalid");
  }
  const allowedOrigin = (value: string) => value === origin || viewerOrigins.has(value);
  function bindBrowser(grant: Grant) {
    if (worker || grant.browser || !grant.entry.starting) return;
    grant.browser = grant.entry.starting;
    grant.browserSignal = grant.entry.abort.signal;
    grant.browserSignal.addEventListener("abort", grant.browserEnded, { once: true });
    if (grant.browserSignal.aborted) grant.browserEnded();
  }
  function resolve(token: string, kind: Grant["kind"]) {
    const grant = worker ? references.get(kind) : grants.get(token);
    if (worker && grant?.token !== token) return null;
    if (
      !grant ||
      grant.kind !== kind ||
      (!worker && grant.expiresAt <= Date.now()) ||
      grant.entry.revoked ||
      grant.life.signal.aborted ||
      executions.closing ||
      executions.get(grant.entry.executionId) !== grant.entry
    )
      return null;
    if (
      grant.life.signal.aborted ||
      (grant.browser && grant.browser !== grant.entry.starting)
    ) {
      grant.cleanup();
      return null;
    }
    return grant;
  }
  function track(
    grant: Grant,
    connection: any,
    close = (_browserEnded: boolean) => {
      connection.destroy ? connection.destroy() : connection.terminate();
      return false;
    },
  ) {
    if (grant.connections.has(connection)) return;
    grant.connections.set(connection, close);
    connection.once("close", () => {
      grant.connections.delete(connection);
      if (grant.life.signal.aborted && !grant.connections.size) grant.cleanup();
    });
  }
  function issue(
    entry: any,
    kind: Grant["kind"],
    control: boolean,
    expiresAt: number,
    resize = false,
  ) {
    const token = randomBytes(32).toString("hex");
    const life = new AbortController();
    const revoke = (browserEnded = false) => {
      if (viewportOwners.get(entry) === grant) {
        viewportOwners.delete(entry);
        executions.releaseViewport(entry, grant.actorId);
      }
      life.abort();
      grant.browserSignal?.removeEventListener("abort", grant.browserEnded);
      for (const [connection, close] of grant.connections) {
        if (close(browserEnded) !== true) grant.connections.delete(connection);
      }
      if (!grant.connections.size) {
        grants.delete(token);
        clearTimeout(grant.timer);
        entry.lifetime.signal.removeEventListener("abort", grant.cleanup);
      }
    };
    const grant: Grant = {
      token,
      entry,
      kind,
      control,
      resize,
      actorId: randomUUID(),
      leaseId: randomUUID(),
      expiresAt,
      life,
      connections: new Map(),
      timer: worker ? undefined : setTimeout(() => revoke(), expiresAt - Date.now()),
      cleanup: () => revoke(),
      browserEnded: () => revoke(true),
    };
    grant.timer?.unref();
    entry.lifetime.signal.addEventListener("abort", grant.cleanup, { once: true });
    grants.set(token, grant);
    bindBrowser(grant);
    return grant;
  }
  const stopResponses = new WeakSet<object>();
  const files = createHttpHandler({
    executions,
    healthy: async () => true,
    resolvePeerUid: undefined,
    fileTransfers,
    authentication,
    onSessionStop: (_req, res) => stopResponses.add(res),
    resolveAccess: (token) => resolve(token, "cdp")?.entry,
  });
  const httpSockets = new WeakSet<object>();
  const server = createServer(async (req, res) => {
    httpSockets.add(req.socket);
    res.setHeader("cache-control", "no-store");
    res.setHeader("referrer-policy", "no-referrer");
    res.setHeader("x-content-type-options", "nosniff");
    res.setHeader(
      "content-security-policy",
      `default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' blob: data:; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'self' ${[...viewerOrigins].join(" ")}`,
    );
    if (req.headers.origin && !allowedOrigin(req.headers.origin)) {
      res.writeHead(403);
      res.end();
      return;
    }
    if (worker && req.url === "/health") {
      res
        .writeHead(
          !executions.closing && (await healthy().catch(() => false)) ? 204 : 503,
        )
        .end();
      return;
    }
    const asset = assets.get(req.url);
    if (asset && req.method === "GET") {
      try {
        res.setHeader("content-type", asset[1]);
        res.end(await readFile(new URL(`./viewer/${asset[0]}`, import.meta.url)));
      } catch {
        res.writeHead(503);
        res.end();
      }
      return;
    }
    const match = /^\/execution\/([a-f0-9]{64})\/(session|auth|files\/[^/?]+)$/.exec(
      req.url ?? "",
    );
    const grant = match && resolve(match[1], "cdp");
    if (!grant) {
      res.writeHead(403);
      res.end();
      return;
    }
    track(grant, res, (browserEnded) => {
      if (browserEnded && stopResponses.has(res)) {
        // Only this admitted terminal response may drain, within the handler's deadline.
        res.shouldKeepAlive = false;
        return true;
      } else {
        req.destroy();
        res.destroy();
        return false;
      }
    });
    await files(req, res);
  });
  // Each socket serves one HTTP request; upgrades must also leave its response intact.
  server.maxRequestsPerSocket = 1;
  const cdp = attachCdpProxy({
    server,
    WebSocket,
    WebSocketServer,
    executions,
    resolvePeerUid: undefined,
    canUpgrade: (req) => !httpSockets.has(req.socket),
    authorize: async (req) => {
      if (req.headers.origin && !allowedOrigin(req.headers.origin)) return null;
      const token = /^\/execution\/([a-f0-9]{64})$/.exec(req.url ?? "")?.[1];
      const grant = resolve(token, "cdp");
      if (!grant) return null;
      track(grant, req.socket, (browserEnded) => {
        if (!browserEnded) {
          req.socket.destroy();
          return false;
        }
        // The CDP proxy must send its startup-failure close frame before teardown.
        const timer = setTimeout(() => req.socket.destroy(), 1000);
        timer.unref();
        req.socket.once("close", () => clearTimeout(timer));
        return true;
      });
      return { entry: grant.entry, signal: grant.life.signal };
    },
  });
  const viewers = new WebSocketServer({ noServer: true, maxPayload: 16 * 1024 });
  let waiting = 0;
  server.on("upgrade", (req, socket, head) => {
    if (httpSockets.has(socket) || (worker && req.url === "/host")) return;
    if (/^\/execution\/[a-f0-9]{64}$/.test(req.url ?? "")) return;
    if (
      req.url !== "/view/socket" ||
      !allowedOrigin(req.headers.origin) ||
      waiting >= 64 ||
      viewers.clients.size >= 64 ||
      executions.closing
    ) {
      socket.end("HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n");
      return;
    }
    viewers.handleUpgrade(req, socket, head, (client) => {
      waiting++;
      const authTimer = setTimeout(() => client.terminate(), 5000);
      let authenticated = false;
      client.once("close", () => {
        clearTimeout(authTimer);
        if (!authenticated) waiting--;
      });
      client.once("message", (raw) => {
        clearTimeout(authTimer);
        waiting--;
        authenticated = true;
        let grant: Grant;
        try {
          const auth = JSON.parse(String(raw));
          grant = auth.type === "authenticate" && resolve(auth.token, "view");
          if (worker && grant) {
            grant = issue(
              workerEntry,
              "view",
              auth.control === true,
              Number.MAX_SAFE_INTEGER,
              auth.resize === true,
            );
            client.once("close", grant.cleanup);
          }
          if (
            !grant ||
            grant.connections.size >= (grant.control || grant.resize ? 1 : 4)
          )
            throw Error();
        } catch {
          client.close(1008, "browser_access_denied");
          return;
        }
        track(grant, client);
        attachViewer(client, grant);
      });
      client.on("error", () => client.terminate());
    });
  });
  function attachViewer(client: any, grant: Grant) {
    const life = new AbortController();
    const signal = AbortSignal.any([life.signal, grant.life.signal]);
    let stream: AbortController;
    let selected: string;
    let generation: string = grant.entry.session.status(
      grant.entry.peers.size,
    ).generation;
    let sequence = 0,
      outstanding = 0;
    let busy = false;
    let polling = false;
    let pollDone = Promise.resolve();
    let finishPoll: () => void;
    let interaction: any;
    let lastTargets = "";
    let activeObservation: any;
    const send = (message: any) => {
      if (signal.aborted || client.readyState !== WebSocket.OPEN) return;
      const value = JSON.stringify(message);
      if (value.length > 12 * 1024 * 1024 || client.bufferedAmount > 1024 * 1024)
        return client.close(1013, "browser_view_slow");
      client.send(value);
    };
    const identity = () => ({
      actorId: grant.actorId,
      leaseId: grant.leaseId,
      epoch: grant.epoch,
    });
    const control = async (action: any, opSignal = signal) => {
      const result = await executions.dispatch(
        {
          op: "takeover",
          executionId: grant.entry.executionId,
          action: { ...action, ...identity() },
        },
        opSignal,
      );
      opSignal.throwIfAborted();
      if (result.epoch) grant.epoch = result.epoch;
      return result;
    };
    const human = () =>
      grant.entry.takeover?.heldBy(grant.actorId, grant.leaseId, grant.epoch) === true;
    const frame = async (opSignal = signal) => {
      interaction = await control({ op: "frame", targetId: selected }, opSignal);
      send({ type: "frame", generation, ...interaction, interactive: true });
    };
    const poll = async () => {
      if (polling || signal.aborted) return;
      polling = true;
      pollDone = new Promise<void>((resolve) => {
        finishPoll = resolve;
      });
      try {
        if (!resolve(grant.token, "view"))
          return client.close(1008, "browser_access_denied");
        const status = grant.entry.session.status(grant.entry.peers.size);
        if (generation !== status.generation) {
          generation = status.generation;
          grant.epoch = undefined;
          interaction = undefined;
          activeObservation = undefined;
          stream?.abort();
          selected = undefined;
          outstanding = 0;
        }
        const leasedByYou = human();
        const state = grant.entry.takeover?.state ?? "automation";
        send({
          type: "state",
          generation,
          browser: status,
          control: state,
          canResize: grant.resize,
          canAcquire: grant.control && automationControl.canPause(grant.entry),
          authentication: grant.control
            ? authentication.prompt(grant.entry)
            : undefined,
          leasedByYou,
          leaseExpiresAt: grant.entry.takeover?.leaseExpiresAt,
          expiresAt: grant.expiresAt,
        });
        if (busy) return;
        const observation = await executions.observe(grant.entry, signal);
        if (!observation) return;
        const targets = await observation.targets(signal);
        const encoded = JSON.stringify(targets);
        if (encoded !== lastTargets) {
          lastTargets = encoded;
          send({ type: "targets", targets });
        }
        if (!targets.some((target: any) => target.targetId === selected)) {
          selected = targets[0]?.targetId;
          stream?.abort();
          activeObservation = undefined;
        }
        if (!selected) return;
        if (human()) {
          stream?.abort();
          activeObservation = undefined;
          if (!interaction) {
            busy = true;
            try {
              await frame();
            } finally {
              busy = false;
            }
          }
        } else if (activeObservation !== observation) {
          stream?.abort();
          stream = new AbortController();
          activeObservation = observation;
          await observation.watch(
            selected,
            (value: any) => {
              if (human() || busy) return;
              if (value.error) {
                activeObservation = undefined;
                send({ type: "error", error: value.error });
                return;
              }
              if (outstanding) return;
              outstanding = ++sequence;
              send({
                type: "frame",
                generation,
                sequence: outstanding,
                ...value,
                interactive: false,
              });
            },
            AbortSignal.any([signal, stream.signal]),
          );
        }
      } catch (error) {
        activeObservation = undefined;
        if (!signal.aborted) send({ type: "error", error: errorCode(error) });
      } finally {
        polling = false;
        finishPoll();
      }
    };
    client.on("message", async (raw) => {
      let message: any;
      try {
        message = JSON.parse(String(raw));
        if (!message || typeof message !== "object" || Array.isArray(message))
          throw Error();
      } catch {
        client.close(1008, "browser_request_invalid");
        return;
      }
      if (message.type === "ack") {
        if (message.sequence === outstanding) outstanding = 0;
        return;
      }
      if (busy) {
        send({ type: "error", id: message.id, error: "browser_view_busy" });
        return;
      }
      if (!Number.isSafeInteger(message.id)) {
        client.close(1008, "browser_request_invalid");
        return;
      }
      busy = true;
      const opSignal = AbortSignal.any([signal, AbortSignal.timeout(30_000)]);
      try {
        await waitForControl(pollDone, opSignal);
        if (!resolve(grant.token, "view")) throw Error("browser_access_denied");
        let result: any = {};
        if (message.type === "authRespond") {
          if (!grant.control) throw Error("browser_control_denied");
          authentication.respond(grant.entry, message.requestId, message.response);
        } else if (message.type === "selectTarget") {
          if (typeof message.targetId !== "string" || message.targetId.length > 128)
            throw Error("browser_page_unavailable");
          const observation = await executions.observe(grant.entry, opSignal);
          if (
            !observation ||
            !(await observation.targets(opSignal)).some(
              (target) => target.targetId === message.targetId,
            )
          )
            throw Error("browser_page_unavailable");
          selected = message.targetId;
          stream?.abort();
          activeObservation = undefined;
          interaction = undefined;
          outstanding = 0;
          if (human()) await frame(opSignal);
        } else if (message.type === "resize") {
          if (!grant.resize || viewportOwners.get(grant.entry) !== grant)
            throw Error("browser_viewport_denied");
          if (message.generation !== generation || message.targetId !== selected)
            throw Error("browser_frame_stale");
          if (
            grant.entry.takeover &&
            grant.entry.takeover.state !== "automation" &&
            !human()
          )
            throw Error("browser_control_busy");
          result = await executions.dispatch(
            {
              op: "viewport",
              ownerId: grant.actorId,
              executionId: grant.entry.executionId,
              targetId: selected,
              width: message.width,
              height: message.height,
            },
            opSignal,
          );
          interaction = undefined;
          stream?.abort();
          activeObservation = undefined;
          outstanding = 0;
          if (human()) await frame(opSignal);
        } else {
          if (!grant.control) throw Error("browser_control_denied");
          if (message.type === "acquire") {
            result = await automationControl.run(grant.entry, opSignal, async () => {
              await automationControl.pause(grant.entry, opSignal);
              return control(
                { op: "acquire", automationStopped: true, targetId: selected },
                opSignal,
              );
            });
            selected = result.targetId;
            interaction = undefined;
            await frame(opSignal);
          } else if (message.type === "input") {
            if (
              !interaction ||
              message.frameRevision !== interaction.frameRevision ||
              message.generation !== generation ||
              message.targetId !== selected
            )
              throw Error("browser_frame_stale");
            const { action } = message;
            if (
              !action ||
              !["pointer", "text", "key", "scroll", "navigate", "reload"].includes(
                action.op,
              )
            )
              throw Error("browser_human_operation_invalid");
            const { frameRevision, origin: pageOrigin, targetId } = interaction;
            interaction = undefined;
            result = await control(
              {
                op: action.op,
                x: action.x,
                y: action.y,
                deltaX: action.deltaX,
                deltaY: action.deltaY,
                text: action.text,
                key: action.key,
                modifiers: action.modifiers,
                url: action.url,
                frameRevision,
                origin: pageOrigin,
                targetId,
              },
              opSignal,
            );
            await frame(opSignal);
          } else if (message.type === "refresh") {
            interaction = undefined;
            await frame(opSignal);
          } else if (message.type === "renew")
            result = await control({ op: "renew" }, opSignal);
          else if (message.type === "release") {
            if (
              !interaction ||
              message.frameRevision !== interaction.frameRevision ||
              message.targetId !== selected
            )
              throw Error("browser_frame_stale");
            result = await automationControl.run(grant.entry, opSignal, async () => {
              await control(
                {
                  op: "confirmTarget",
                  targetId: interaction.targetId,
                  origin: interaction.origin,
                  frameRevision: interaction.frameRevision,
                },
                opSignal,
              );
              const released = await control(
                { op: "release", origin: interaction.origin },
                opSignal,
              );
              grant.epoch = undefined;
              interaction = undefined;
              activeObservation = undefined;
              let resumed = false;
              try {
                resumed = await automationControl.resume(grant.entry, opSignal);
              } catch {
                /* Control release is separate from host resumption. */
              }
              return { ...released, resumed };
            });
          } else throw Error("browser_human_operation_invalid");
        }
        const publicResult = { ...result };
        delete publicResult.epoch;
        delete publicResult.leaseId;
        send({ type: "result", id: message.id, result: publicResult });
      } catch (error) {
        send({ type: "error", id: message.id, error: errorCode(error) });
      } finally {
        busy = false;
        void poll();
      }
    });
    const timer = setInterval(() => void poll(), 1000);
    client.once("close", () => {
      clearInterval(timer);
      life.abort();
      stream?.abort();
    });
    void poll();
  }
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(config.port, config.host ?? "127.0.0.1", resolve);
  });
  const port = (server.address() as AddressInfo).port;
  origin ??= `http://127.0.0.1:${port}`;
  return {
    port,
    server,
    bindWorker(entry: any) {
      if (!worker || entry.executionId !== worker)
        throw Error("browser_identity_mismatch");
      if (workerEntry === entry)
        return {
          cdpUrl: `${origin.replace(/^http/, "ws")}/execution/${references.get("cdp").token}`,
          viewerToken: references.get("view").token,
        };
      workerEntry = entry;
      references.set("cdp", issue(entry, "cdp", false, Number.MAX_SAFE_INTEGER));
      references.set("view", issue(entry, "view", false, Number.MAX_SAFE_INTEGER));
      return {
        cdpUrl: `${origin.replace(/^http/, "ws")}/execution/${references.get("cdp").token}`,
        viewerToken: references.get("view").token,
      };
    },
    browserStarting(entry: any) {
      for (const grant of grants.values())
        if (grant.entry === entry) bindBrowser(grant);
    },
    command(request: any) {
      if (worker && request.op !== "access_confirm_stopped")
        throw Error("browser_host_operation_invalid");
      const entry = executions.get(request.executionId);
      if (!entry || entry.revoked || executions.closing)
        throw Error("browser_execution_revoked");
      if (request.op === "access_create") {
        const durationMs = request.durationMs ?? 600_000;
        if (
          !Number.isInteger(durationMs) ||
          durationMs < 1 ||
          durationMs > 1_800_000 ||
          (request.control !== undefined && typeof request.control !== "boolean") ||
          (request.resize !== undefined && typeof request.resize !== "boolean")
        )
          throw Error("browser_access_request_invalid");
        if (request.resize && viewportOwners.has(entry))
          throw Error("browser_viewport_busy");
        if (grants.size >= 2000) throw Error("browser_access_capacity_exceeded");
        const expiresAt = Date.now() + durationMs;
        const agent = issue(entry, "cdp", false, expiresAt);
        const viewer = issue(
          entry,
          "view",
          request.control === true,
          expiresAt,
          request.resize === true,
        );
        if (viewer.resize) viewportOwners.set(entry, viewer);
        return {
          executionId: entry.executionId,
          cdpUrl: `${origin.replace(/^http/, "ws")}/execution/${agent.token}`,
          viewerUrl: `${origin}/view#${viewer.token}`,
          expiresAt,
        };
      }
      if (request.op === "access_revoke") {
        for (const grant of grants.values()) if (grant.entry === entry) grant.cleanup();
        return { revoked: true };
      }
      if (request.op === "access_confirm_stopped") {
        if (request.automationStopped !== true)
          throw Error("browser_automation_stop_required");
        automationControl.confirm(entry);
        return { confirmed: true };
      }
      throw Error("browser_access_request_invalid");
    },
    async stop() {
      for (const grant of grants.values()) grant.cleanup();
      for (const client of viewers.clients) client.terminate();
      cdp.close();
      viewers.close();
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}
