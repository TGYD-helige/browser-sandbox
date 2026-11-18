import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import WebSocket from "ws";

export function assertFencedOwner(events, executionId) {
  const own = events.filter((event) => event.executionId === executionId);
  assert(
    own.some(
      (event) => event.type === "fenced" && event.error === "browser_controller_fenced",
    ),
    "Resumed controller did not report its own fencing",
  );
  assert(
    !own.some((event) => event.type === "removed"),
    "Resumed old controller reported worker removal",
  );
}

export async function observeSingleRemoval(
  readCount,
  { durationMs = 6000, now = () => performance.now(), pause = delay } = {},
) {
  const started = now();
  while (true) {
    assert.equal(await readCount(), 1, "Orphan was reclaimed more than once");
    const remaining = durationMs - (now() - started);
    if (remaining <= 0) return now() - started;
    await pause(Math.min(remaining, 100));
  }
}

export function summarizeFailover(values) {
  const sorted = [...values].sort((a, b) => a - b);
  return {
    medianMs: sorted[Math.floor(sorted.length / 2)],
    maxMs: sorted.at(-1),
    samples: sorted.length,
  };
}

export async function verifyWithRecoveredOwner(
  wait,
  readOwner,
  replicaSetUid,
  readStatus,
  verify,
) {
  const owner = await wait(
    "The successor did not establish a recovered execution owner",
    async () => {
      const { pod, lease } = await readOwner();
      if (
        !pod ||
        (replicaSetUid &&
          !pod.metadata.ownerReferences?.some(
            (ref) =>
              ref.kind === "ReplicaSet" && ref.controller && ref.uid === replicaSetUid,
          ))
      )
        return false;
      const status = await readStatus(pod).catch(() => undefined);
      if (status?.execution?.phase !== "active") return false;
      const current = await readOwner();
      if (
        current.pod?.metadata.uid === pod.metadata.uid &&
        current.lease.metadata.uid === lease.metadata.uid &&
        current.lease.metadata.annotations?.["browser-sandbox.io/epoch"] ===
          lease.metadata.annotations?.["browser-sandbox.io/epoch"]
      )
        return current;
    },
    Boolean,
    45000,
  );
  return verify(owner);
}

export function observeCdpHandshake(socket) {
  const diagnostic = {};
  socket.once("error", ({ code, message }) => {
    if ("status" in diagnostic) return;
    if (/^[A-Z0-9_]{1,48}$/.test(code ?? "")) diagnostic.code = code;
    else
      diagnostic.reason = [
        "Opening handshake has timed out",
        "WebSocket was closed before the connection was established",
        "Invalid Upgrade header",
        "Invalid Sec-WebSocket-Accept header",
      ].includes(message)
        ? message
        : "unclassified";
  });
  socket.once("unexpected-response", (_request, response) => {
    diagnostic.status = response.statusCode;
    // Keep only a bounded controller error body. Proxies may echo credentials or URLs.
    const bytes = Buffer.alloc(512);
    let length = 0;
    const finish = (complete = false) => {
      if ("body" in diagnostic) return;
      clearTimeout(deadline);
      let error;
      try {
        if (complete) error = JSON.parse(bytes.toString("utf8", 0, length))?.error;
      } catch {}
      diagnostic.body =
        typeof error === "string" && /^browser_[a-z_]{1,100}$/.test(error)
          ? { error }
          : null;
      socket.terminate();
    };
    const deadline = setTimeout(finish, 1000).unref();
    response.on("data", (chunk) => {
      length += chunk.copy(bytes, length, 0, bytes.length - length);
      if (length === bytes.length) finish();
    });
    response.once("end", () => finish(response.complete));
    response.once("error", () => finish());
    response.once("close", () => finish());
  });
  return diagnostic;
}

// This CI client sends one CDP command at a time and only retries the opening handshake.
export async function connectDiagnosticCdp(
  endpoint,
  { retryOwnerMoved = false, signal = AbortSignal.timeout(30000) } = {},
) {
  for (let attempts = 1; ; attempts++) {
    signal.throwIfAborted();
    const socket = new WebSocket(endpoint, {
      maxPayload: 32 * 1024 * 1024,
      handshakeTimeout: 30000,
    });
    const handshake = observeCdpHandshake(socket);
    const close = () => socket.terminate();
    const closed = new Promise((resolve) => socket.once("close", resolve));
    socket.on("error", () => {});
    signal.addEventListener("abort", close, { once: true });
    try {
      await new Promise((resolve, reject) => {
        socket.once("open", resolve);
        socket.once("error", reject);
        socket.once("close", () => reject(Error("browser_cdp_closed")));
        if (signal.aborted) close();
      });
    } catch {
      close();
      await closed;
      signal.throwIfAborted();
      if (
        !retryOwnerMoved ||
        handshake.status !== 503 ||
        handshake.body?.error !== "browser_controller_owner_moved"
      )
        throw Object.assign(Error("browser_cdp_connect_failed"), { handshake });
      await delay(250, undefined, { signal });
      continue;
    } finally {
      signal.removeEventListener("abort", close);
    }
    let id = 0;
    let pending;
    socket.on("close", () => pending?.reject(Error("browser_cdp_closed")));
    socket.on("message", (raw) => {
      try {
        const reply = JSON.parse(String(raw));
        if (!pending || reply.id !== id) return;
        if (reply.error) pending.reject(Error("browser_cdp_operation_failed"));
        else pending.resolve(reply.result);
      } catch {
        close();
      }
    });
    return {
      attempts,
      closed,
      close,
      async call(method, params = {}, sessionId) {
        assert(!pending, "The CI client only supports one pending CDP command");
        if (socket.readyState !== WebSocket.OPEN) throw Error("browser_cdp_closed");
        pending = Promise.withResolvers();
        const request = pending;
        const timer = setTimeout(
          () => request.reject(Error("browser_cdp_timeout")),
          id++ === 0 ? 60000 : 15000,
        );
        try {
          socket.send(JSON.stringify({ id, method, params, sessionId }), (error) => {
            if (error) request.reject(error);
          });
          return await request.promise;
        } finally {
          clearTimeout(timer);
          pending = undefined;
        }
      },
    };
  }
}

// Rollout can close a Service-selected replica after CDP checks but before status.
export async function readVerifiedStatus(action, read, signal) {
  if (action !== "verify-rollout") return read(signal);
  while (true) {
    signal.throwIfAborted();
    try {
      const status = await read(signal);
      signal.throwIfAborted();
      return status;
    } catch (error) {
      signal.throwIfAborted();
      if (
        error.message !== "browser_controller_owner_moved" &&
        !["UND_ERR_SOCKET", "ECONNRESET", "ECONNREFUSED"].includes(error.cause?.code)
      )
        throw error;
    }
    await delay(100, undefined, { signal });
  }
}
