import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { createServer } from "node:http";
import { test } from "node:test";
import { WebSocketServer } from "ws";
import { connectCdp } from "../../../dist/cdp.js";
import { sessionRequest } from "../../../dist/session.js";
import {
  assertFencedOwner,
  connectDiagnosticCdp,
  observeCdpHandshake,
  observeSingleRemoval,
  readVerifiedStatus,
  summarizeFailover,
  verifyWithRecoveredOwner,
} from "./active-active-checks.mjs";

test("resumed controller evidence requires its own fencing report and excludes worker removal events", () => {
  assert.throws(() => assertFencedOwner([], "execution"));
  const fenced = {
    executionId: "execution",
    type: "fenced",
    error: "browser_controller_fenced",
  };
  assert.throws(() =>
    assertFencedOwner(
      [fenced, { executionId: "execution", type: "removed" }],
      "execution",
    ),
  );
  // Synthetic unknown events cannot stand in for finalization or archive-save evidence.
  assert.doesNotThrow(() =>
    assertFencedOwner(
      [
        fenced,
        { executionId: "execution", type: "finalized" },
        { executionId: "execution", type: "commit" },
      ],
      "execution",
    ),
  );
  assert.doesNotThrow(() => assertFencedOwner([fenced], "execution"));
});

test("orphan evidence continues observing after the first removal and detects a late duplicate", async () => {
  // Synthetic event timing: the duplicate appears after the initial successful observation.
  let clock = 0,
    reads = 0;
  const options = {
    durationMs: 600,
    now: () => clock,
    pause: async (ms) => {
      clock += ms;
    },
  };
  await assert.rejects(
    observeSingleRemoval(async () => {
      reads++;
      return clock < 300 ? 1 : 2;
    }, options),
  );
  assert(reads > 1);
  clock = 0;
  assert.equal(await observeSingleRemoval(async () => 1, options), 600);
});

test("five failover observations report a maximum rather than a population P95", () => {
  assert.deepEqual(summarizeFailover([5, 1, 3, 4, 2]), {
    medianMs: 3,
    maxMs: 5,
    samples: 5,
  });
});

test("takeover status reconnects after a controlled HTTP connection loss without replaying CDP", async (t) => {
  let requests = 0;
  const server = createServer(async (req, res) => {
    let input = "";
    for await (const chunk of req) input += chunk;
    assert.deepEqual(JSON.parse(input), { action: "status" });
    if (++requests === 1) return req.socket.destroy();
    res.setHeader("content-type", "application/json");
    res.end(JSON.stringify({ generation: "original" }));
  });
  t.after(async () => {
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  // Synthetic rollout window: the selected replica closes after receiving the status request.
  const endpoint = `ws://127.0.0.1:${server.address().port}/execution/${"a".repeat(64)}`;
  const result = await readVerifiedStatus(
    "verify-rollout",
    (signal) => sessionRequest(endpoint, "status", undefined, signal),
    AbortSignal.timeout(5000),
  );
  assert.equal(result.generation, "original");
  assert.equal(requests, 2);
});

test("takeover status retries only owner movement and connection loss within one deadline", async () => {
  let reads = 0;
  const status = await readVerifiedStatus(
    "verify-rollout",
    async () => {
      if (++reads === 1) throw Error("browser_controller_owner_moved");
      return { generation: "original" };
    },
    AbortSignal.timeout(5000),
  );
  assert.equal(status.generation, "original");
  assert.equal(reads, 2);
  for (const error of [
    Error("browser_access_denied"),
    Error("browser_session_failed"),
    new TypeError("invalid response"),
  ]) {
    reads = 0;
    await assert.rejects(
      readVerifiedStatus(
        "verify-rollout",
        async () => {
          reads++;
          throw error;
        },
        AbortSignal.timeout(5000),
      ),
      (value) => value === error,
    );
    assert.equal(reads, 1);
  }
  reads = 0;
  await assert.rejects(
    async () => {
      const result = await readVerifiedStatus(
        "verify-rollout",
        async () => {
          reads++;
          return { generation: "changed" };
        },
        AbortSignal.timeout(5000),
      );
      assert.equal(result.generation, "original");
    },
    { code: "ERR_ASSERTION" },
  );
  assert.equal(reads, 1, "generation mismatch remains an immediate failure");
  reads = 0;
  const deadline = new AbortController();
  await assert.rejects(
    readVerifiedStatus(
      "verify-rollout",
      async (signal) => {
        assert.equal(signal, deadline.signal);
        reads++;
        deadline.abort(Error("test deadline"));
        throw Error("browser_controller_owner_moved");
      },
      deadline.signal,
    ),
    /test deadline/,
  );
  assert.equal(reads, 1, "an expired shared deadline cannot start another request");
});

test("steady-state verification never reconnects even for retryable transport or owner errors", async () => {
  for (const error of [
    Error("browser_controller_owner_moved"),
    new TypeError("fetch failed", { cause: { code: "UND_ERR_SOCKET" } }),
  ]) {
    let reads = 0;
    await assert.rejects(
      readVerifiedStatus(
        "verify",
        async () => {
          if (++reads === 1) throw error;
          return { generation: "original" };
        },
        AbortSignal.timeout(5000),
      ),
      (value) => value === error,
    );
    assert.equal(reads, 1);
  }
});

test("post-rollout CDP verification waits for recovered ownership on the new ReplicaSet", async () => {
  const owner = (rs, uid = "owner") => ({
    lease: { metadata: { uid: "lease" }, spec: { holderIdentity: uid } },
    pod: {
      metadata: {
        uid,
        ownerReferences: [{ kind: "ReplicaSet", uid: rs, controller: true }],
      },
    },
  });
  // Synthetic rollout states include a second handoff while the first new owner answers status.
  const snapshots = [
    owner("old"),
    {},
    owner("new"),
    owner("new"),
    owner("new", "next-owner"),
    owner("new", "next-owner"),
    owner("new", "next-owner"),
  ];
  const readOwner = () => {
    assert(snapshots.length > 0, "owner readiness probe did not converge");
    return snapshots.shift();
  };
  const probed = [];
  let verified = 0;
  await verifyWithRecoveredOwner(
    async (_description, read, ready) => {
      let candidate;
      do candidate = await read();
      while (!ready(candidate));
      return candidate;
    },
    readOwner,
    "new",
    async (pod) => {
      probed.push(pod.metadata.uid);
      return { execution: { phase: probed.length === 1 ? "absent" : "active" } };
    },
    async ({ pod }) => {
      assert.equal(pod?.metadata.uid, "next-owner");
      assert.deepEqual(probed, ["owner", "owner", "next-owner"]);
      assert.equal(snapshots.length, 0);
      verified++;
    },
  );
  assert.equal(verified, 1);
});

test("diagnostic CDP retries only a confirmed owner-moved handshake", async (t) => {
  const requests = new Map();
  const server = createServer();
  const sockets = new WebSocketServer({ noServer: true });
  server.on("upgrade", (req, socket, head) => {
    const path = req.url;
    const count = (requests.get(path) ?? 0) + 1;
    requests.set(path, count);
    if (path === "/successor" && count === 2) {
      sockets.handleUpgrade(req, socket, head, (accepted) => {
        accepted.on("message", (raw) => {
          const { id } = JSON.parse(String(raw));
          accepted.send(JSON.stringify({ id, result: { version: "successor" } }));
        });
      });
      return;
    }
    const incomplete = path === "/incomplete";
    const status = path === "/successor" || incomplete ? 503 : Number(path.slice(1));
    const body =
      status === 502
        ? "untrusted proxy details"
        : JSON.stringify({
            error:
              status === 503
                ? "browser_controller_owner_moved"
                : "browser_access_denied",
            capability: "must-not-log",
          });
    socket.end(
      `HTTP/1.1 ${status} Rejected\r\nConnection: close\r\nContent-Length: ${Buffer.byteLength(body) + Number(incomplete)}\r\n\r\n${body}`,
    );
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(async () => {
    for (const socket of sockets.clients) socket.terminate();
    await new Promise((resolve) => sockets.close(resolve));
    await new Promise((resolve) => server.close(resolve));
  });
  const origin = `ws://127.0.0.1:${server.address().port}`;
  const signal = AbortSignal.timeout(5000);
  for (const [status, body] of [
    [503, { error: "browser_controller_owner_moved" }],
    [403, { error: "browser_access_denied" }],
    [502, null],
  ]) {
    await assert.rejects(
      connectDiagnosticCdp(`${origin}/${status}`, {
        signal,
        retryOwnerMoved: status !== 503,
      }),
      (error) => {
        assert.equal(error.message, "browser_cdp_connect_failed");
        assert.deepEqual(error.handshake, { status, body });
        return true;
      },
    );
    assert.equal(requests.get(`/${status}`), 1);
  }
  await assert.rejects(
    connectDiagnosticCdp(`${origin}/incomplete`, { signal, retryOwnerMoved: true }),
    (error) => {
      assert.deepEqual(error.handshake, { status: 503, body: null });
      return true;
    },
  );
  assert.equal(requests.get("/incomplete"), 1);
  const connection = await connectDiagnosticCdp(`${origin}/successor`, {
    retryOwnerMoved: true,
    signal,
  });
  try {
    assert.equal(connection.attempts, 2);
    assert.equal(requests.get("/successor"), 2);
    assert.deepEqual(await connection.call("Browser.getVersion"), {
      version: "successor",
    });
  } finally {
    connection.close();
    await connection.closed;
  }
  await new Promise((resolve) => server.close(resolve));
  await assert.rejects(connectDiagnosticCdp(origin, { signal }), (error) => {
    assert.deepEqual(error.handshake, { code: "ECONNREFUSED" });
    return true;
  });
  const socket = new EventEmitter();
  const diagnostic = observeCdpHandshake(socket);
  socket.emit("error", Error("Opening handshake has timed out"));
  assert.deepEqual(diagnostic, { reason: "Opening handshake has timed out" });
});

test("runtime CDP uses its own WebSocket while CI diagnoses a handshake separately", async (t) => {
  const server = new WebSocketServer({ port: 0, host: "127.0.0.1" });
  t.after(async () => {
    for (const socket of server.clients) socket.terminate();
    await new Promise((resolve) => server.close(resolve));
  });
  await new Promise((resolve) => server.once("listening", resolve));
  const runtimeConnection = await connectCdp(
    `ws://127.0.0.1:${server.address().port}`,
    AbortSignal.timeout(5000),
    class {
      constructor() {
        assert.fail("Runtime CDP exposes a CI-only socket factory");
      }
    },
  );
  runtimeConnection.close();
  await runtimeConnection.closed;
});
