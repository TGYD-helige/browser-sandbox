import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import WebSocket, { WebSocketServer } from "ws";
import { createGateway } from "../dist/gateway.js";

async function fixture(t) {
  const directory = await mkdtemp(join(tmpdir(), "worker-reclamation-"));
  const upstream = new WebSocketServer({ host: "127.0.0.1", port: 0 });
  await once(upstream, "listening");
  let closes = 0;
  const gateway = await createGateway({
    WebSocket,
    WebSocketServer,
    port: 0,
    dataRoot: join(directory, "data"),
    socketPath: join(directory, "control.sock"),
    healthy: async () => true,
    resolvePeerUid: async () => 100001,
    launch: async () => ({
      endpoint: `ws://127.0.0.1:${upstream.address().port}`,
      close: async () => {
        closes++;
      },
    }),
  });
  t.after(async () => {
    await gateway.stop();
    for (const client of upstream.clients) client.terminate();
    await new Promise((resolve) => upstream.close(resolve));
    await rm(directory, { recursive: true, force: true });
  });
  const executionId = randomUUID();
  const command = (request) => gateway.command({ executionId, ...request }, true);
  const { wsEndpoint } = await command({ op: "register", executorUid: 100001 });
  return {
    command,
    wsEndpoint,
    get closes() {
      return closes;
    },
  };
}

test("worker atomically rechecks retention after a controller's idle preflight", async (t) => {
  const f = await fixture(t);
  const client = new WebSocket(f.wsEndpoint);
  await once(client, "open");
  const connected = await f.command({ op: "worker_status" });
  assert.equal(connected.session.connectedClients, 1);
  client.close();
  await once(client, "close");
  const before = await f.command({ op: "worker_status" });
  await f.command({ op: "session", action: "retain", durationMs: 10000 });
  await assert.rejects(
    f.command({
      op: "finalize",
      ifIdle: true,
      expectedGeneration: before.session.generation,
    }),
    /browser_worker_not_idle/,
  );
  assert.equal(f.closes, 0);
  assert.equal((await f.command({ op: "worker_status" })).session.state, "retained");
  await f.command({ op: "session", action: "release" });
  assert.equal(
    (
      await f.command({
        op: "finalize",
        ifIdle: true,
        expectedGeneration: before.session.generation,
      })
    ).stopped,
    true,
  );
  assert.equal(f.closes, 1);
});

test("idle finalization cannot close a connected or different-generation browser", async (t) => {
  const f = await fixture(t);
  const client = new WebSocket(f.wsEndpoint);
  await once(client, "open");
  const status = await f.command({ op: "worker_status" });
  await assert.rejects(
    f.command({
      op: "finalize",
      ifIdle: true,
      expectedGeneration: status.session.generation,
    }),
    /browser_worker_not_idle/,
  );
  await assert.rejects(
    f.command({ op: "finalize", ifIdle: true, expectedGeneration: "stale-generation" }),
    /browser_session_lost/,
  );
  assert.equal(f.closes, 0);
  client.close();
  await once(client, "close");
});

test("renewal after generation loss fails without creating a replacement", async (t) => {
  const f = await fixture(t);
  const client = new WebSocket(f.wsEndpoint);
  await once(client, "open");
  await f.command({ op: "session", action: "retain", durationMs: 10000 });
  client.close();
  await once(client, "close");
  await f.command({ op: "session", action: "stop" });
  await assert.rejects(
    f.command({ op: "session", action: "renew", durationMs: 10000 }),
    /browser_session_not_live/,
  );
  assert.equal(f.closes, 1);
  assert.equal((await f.command({ op: "worker_status" })).session.state, "stopped");
});
