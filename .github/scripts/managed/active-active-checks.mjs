import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";

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

export async function verifyWithRolloutOwner(
  wait,
  readOwner,
  replicaSetUid,
  readStatus,
  verify,
) {
  const owner = await wait(
    "The new ReplicaSet did not establish a recovered execution owner",
    async () => {
      const { pod, lease } = await readOwner();
      if (
        !pod?.metadata.ownerReferences?.some(
          (ref) =>
            ref.kind === "ReplicaSet" && ref.controller && ref.uid === replicaSetUid,
        )
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
  return verify(owner.pod);
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
    const finish = () => {
      if ("body" in diagnostic) return;
      clearTimeout(deadline);
      let error;
      try {
        error = JSON.parse(bytes.toString("utf8", 0, length))?.error;
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
    response.once("end", finish);
    response.once("error", finish);
    response.once("close", finish);
  });
  return diagnostic;
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
