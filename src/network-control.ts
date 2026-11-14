import type { Server } from "node:http";
import WebSocket, { WebSocketServer } from "ws";
import { type AutomationCoordinator, waitForControl } from "./automation-control.js";
import type { Registration, StorageAdapter } from "./host-client.js";
import type { BrowserRuntime } from "./runtime-provider.js";

export const CONTROL_TIMEOUT_MS = 180000;

const code = (error: any) =>
  /^browser_[a-z_]+$/.test(error?.message) ? error.message : "browser_control_failed";

export interface ControlPeer {
  signal: AbortSignal;
  callback<T = any>(op: string, payload: any, signal?: AbortSignal): Promise<T>;
}

/** Bidirectional bounded RPC. Disconnect rejects work in both directions; nothing is replayed. */
function connectPeer(
  socket: WebSocket,
  dispatch: (request: any, signal: AbortSignal) => Promise<any>,
) {
  const life = new AbortController();
  const pending = new Map<
    number,
    { resolve(value: any): void; reject(error: Error): void }
  >();
  const incoming = new Map<number, AbortController>();
  let sequence = 0;
  const send = (frame: any) => {
    const body = JSON.stringify(frame);
    if (life.signal.aborted || socket.readyState !== WebSocket.OPEN)
      throw Error("browser_control_disconnected");
    if (
      Buffer.byteLength(body) >
        (frame.type === "request" ? 262144 : 32 * 1024 * 1024) ||
      socket.bufferedAmount > 32 * 1024 * 1024
    )
      throw Error("browser_control_overloaded");
    socket.send(body);
  };
  const close = () => {
    life.abort();
    for (const entry of pending.values())
      entry.reject(Error("browser_control_disconnected"));
    pending.clear();
    for (const controller of incoming.values()) controller.abort();
    incoming.clear();
  };
  socket.on("close", close);
  socket.on("error", close);
  socket.on("message", async (raw) => {
    try {
      const size = Buffer.byteLength(raw as Buffer);
      if (
        size > 262144 &&
        !String((raw as Buffer).subarray(0, 32)).startsWith('{"type":"result"')
      )
        throw Error();
      const frame = JSON.parse(String(raw));
      if (frame.type === "request" && size > 262144) throw Error();
      if (!Number.isSafeInteger(frame.id) || frame.id < 1) throw Error();
      if (frame.type === "result") {
        const entry = pending.get(frame.id);
        if (entry) {
          pending.delete(frame.id);
          frame.error
            ? entry.reject(Error(code({ message: frame.error })))
            : entry.resolve(frame.result);
        }
      } else if (frame.type === "cancel") incoming.get(frame.id)?.abort();
      else if (frame.type === "request") {
        if (incoming.size >= 64 || incoming.has(frame.id)) throw Error();
        const controller = new AbortController();
        incoming.set(frame.id, controller);
        const signal = AbortSignal.any([
          life.signal,
          controller.signal,
          AbortSignal.timeout(CONTROL_TIMEOUT_MS),
        ]);
        try {
          send({
            type: "result",
            id: frame.id,
            result: await dispatch(frame.request, signal),
          });
        } catch (error) {
          if (!life.signal.aborted)
            send({ type: "result", id: frame.id, error: code(error) });
        } finally {
          incoming.delete(frame.id);
        }
      } else throw Error();
    } catch {
      socket.terminate();
    }
  });
  return {
    signal: life.signal,
    async call<T = any>(
      request: any,
      options: { signal?: AbortSignal; timeoutMs?: number } = {},
    ): Promise<T> {
      if (pending.size >= 64) throw Error("browser_control_overloaded");
      const signal = AbortSignal.any([
        life.signal,
        ...(options.signal ? [options.signal] : []),
        AbortSignal.timeout(options.timeoutMs ?? CONTROL_TIMEOUT_MS),
      ]);
      signal.throwIfAborted();
      const id = ++sequence;
      return new Promise<T>((resolve, reject) => {
        const cleanup = () => {
          pending.delete(id);
          signal.removeEventListener("abort", abort);
        };
        const abort = () => {
          cleanup();
          if (!life.signal.aborted) {
            try {
              send({ type: "cancel", id });
            } catch {}
          }
          reject(
            Error(
              life.signal.aborted
                ? "browser_control_disconnected"
                : "browser_control_timeout",
            ),
          );
        };
        pending.set(id, {
          resolve: (value) => {
            cleanup();
            resolve(value);
          },
          reject: (error) => {
            cleanup();
            reject(error);
          },
        });
        signal.addEventListener("abort", abort, { once: true });
        try {
          send({ type: "request", id, request });
        } catch (error) {
          cleanup();
          reject(error);
        }
      });
    },
  };
}

export function createNetworkHostControl({
  server,
  dispatch,
  authenticateHost = async () => {},
}: {
  server: Server;
  dispatch(request: any, peer: ControlPeer, signal: AbortSignal): Promise<any>;
  authenticateHost?: (request: import("node:http").IncomingMessage) => Promise<void>;
}) {
  const sockets = new WebSocketServer({ noServer: true, maxPayload: 32 * 1024 * 1024 });
  const upgrade = async (request, socket, head) => {
    if (request.url !== "/host") return;
    try {
      await authenticateHost(request);
      if (socket.destroyed) return;
      sockets.handleUpgrade(request, socket, head, (client) => {
        const rpc = connectPeer(client, (input, signal) =>
          dispatch(input, peer, signal),
        );
        const peer: ControlPeer = {
          signal: rpc.signal,
          callback: (op, payload, signal) => rpc.call({ op, ...payload }, { signal }),
        };
      });
    } catch {
      socket.end("HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n");
    }
  };
  server.on("upgrade", upgrade);
  return {
    close() {
      server.off("upgrade", upgrade);
      for (const socket of sockets.clients) socket.terminate();
      sockets.close();
    },
  };
}

export interface ControllerRuntime extends BrowserRuntime {
  identity: "managed";
  close(): void;
  setAutomation(automation: AutomationCoordinator): void;
}

/** Internal trusted networks only; host authentication is reserved at /host. */
export function createControllerRuntime(
  origin: string,
  { automation: initialAutomation }: { automation?: AutomationCoordinator } = {},
): ControllerRuntime {
  let automation = initialAutomation;
  const storage = new Map<string, StorageAdapter>();
  const url = new URL("/host", origin);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  const socket = new WebSocket(url, {
    maxPayload: 32 * 1024 * 1024,
    handshakeTimeout: 30000,
  });
  const rpc = connectPeer(socket, async (request, signal) => {
    if (["pause", "resume"].includes(request.op)) {
      if (!automation) throw Error("browser_automation_stop_required");
      await automation[request.op](request.executionId, signal);
      return { confirmed: true };
    }
    const adapter = storage.get(request.executionId);
    if (!adapter) throw Error("browser_storage_disconnected");
    if (request.op === "load") return adapter.load(signal);
    if (request.op === "save") return adapter.save(signal);
    if (request.op === "commit") return adapter.commit(request.payload, signal);
    throw Error("browser_control_operation_invalid");
  });
  const ready = new Promise<void>((resolve, reject) => {
    socket.once("open", resolve);
    socket.once("error", () => reject(Error("browser_control_unavailable")));
    socket.once("close", () => reject(Error("browser_control_disconnected")));
  });
  ready.catch(() => {});
  const request = async <T>(
    input: Record<string, unknown>,
    options?: { signal?: AbortSignal; timeoutMs?: number },
  ): Promise<T> => {
    if (rpc.signal.aborted) throw Error("browser_control_disconnected");
    const signal = AbortSignal.any([
      ...(options?.signal ? [options.signal] : []),
      AbortSignal.timeout(options?.timeoutMs ?? CONTROL_TIMEOUT_MS),
    ]);
    await waitForControl(ready, signal);
    return rpc.call<T>({ ...input, automation: !!automation }, { ...options, signal });
  };
  return {
    identity: "managed",
    setAutomation(value) {
      automation = value;
    },
    request,
    async attachStorage(
      registration: Registration,
      adapter: StorageAdapter,
      options: { signal?: AbortSignal } = {},
    ) {
      if (storage.has(registration.executionId))
        throw Error("browser_storage_already_attached");
      storage.set(registration.executionId, adapter);
      try {
        await request({ op: "storage_attach", ...registration }, options);
      } catch (error) {
        storage.delete(registration.executionId);
        throw error;
      }
      let closed = false;
      return {
        close() {
          if (closed) return;
          closed = true;
          if (storage.get(registration.executionId) !== adapter) return;
          storage.delete(registration.executionId);
          request({
            op: "storage_detach",
            executionId: registration.executionId,
          }).catch(() => {});
        },
      };
    },
    close() {
      socket.terminate();
    },
  };
}
