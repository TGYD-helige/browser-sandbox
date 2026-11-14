import assert from "node:assert/strict";
import { once } from "node:events";
import fs from "node:fs/promises";
import { createServer } from "node:net";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { createWorkerStorage } from "../dist/worker-storage.js";

async function fixture(t) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "worker-storage-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const sockets = new Set();
  const connected = [];
  let waiting;
  const server = createServer((socket) => {
    sockets.add(socket);
    socket.on("error", () => {});
    socket.once("close", () => sockets.delete(socket));
    socket.once("data", (data) => {
      const registration = JSON.parse(String(data));
      assert.equal(registration.executorUid, 100001);
      const entry = {
        socket,
        registration,
        ready: () => socket.write('{"ready":true}\n'),
      };
      if (waiting) {
        waiting(entry);
        waiting = undefined;
      } else connected.push(entry);
    });
  });
  const socketPath = path.join(root, "control");
  server.listen(socketPath + ".auth");
  await once(server, "listening");
  t.after(async () => {
    for (const socket of sockets) socket.destroy();
    await new Promise((resolve) => server.close(resolve));
  });
  const peerLife = new AbortController();
  t.after(() => peerLife.abort());
  const peer = { signal: peerLife.signal, callback: async () => ({}) };
  const bridge = createWorkerStorage(socketPath);
  return {
    bridge,
    peer,
    peerLife,
    sockets,
    connected: () =>
      connected.length
        ? Promise.resolve(connected.shift())
        : new Promise((resolve) => {
            waiting = resolve;
          }),
  };
}
const request = {
  executionId: "11111111-1111-4111-8111-111111111111",
  profileId: "account",
};

test("worker storage detach closes the real Unix channel before a replacement attaches", async (t) => {
  const f = await fixture(t);
  for (let i = 0; i < 4; i++) {
    const pending = f.bridge(
      { ...request, op: "storage_attach" },
      f.peer,
      AbortSignal.timeout(1000),
    );
    const current = await f.connected();
    current.ready();
    assert.deepEqual(await pending, { attached: true });
    assert.equal(f.sockets.size, 1);
    const closed = once(current.socket, "close");
    assert.deepEqual(await f.bridge({ ...request, op: "storage_detach" }, f.peer), {
      detached: true,
    });
    await closed;
    assert.equal(f.sockets.size, 0);
  }
});

test("aborting a pending worker attachment closes a Unix handshake that never becomes ready", {
  timeout: 2000,
}, async (t) => {
  const f = await fixture(t);
  const pending = f.bridge(
    { ...request, op: "storage_attach" },
    f.peer,
    AbortSignal.timeout(1000),
  );
  const rejection = assert.rejects(pending, /browser_storage_disconnected/);
  const current = await f.connected();
  const closed = once(current.socket, "close");
  f.peerLife.abort();
  await rejection;
  await closed;
  assert.equal(f.sockets.size, 0);
});

test("attachment setup cancellation does not become the lifetime of a ready Unix channel", async (t) => {
  const f = await fixture(t);
  const setup = new AbortController();
  const pending = f.bridge({ ...request, op: "storage_attach" }, f.peer, setup.signal);
  const current = await f.connected();
  current.ready();
  await pending;
  const reply = once(current.socket, "data");
  const closed = once(current.socket, "close");
  setup.abort();
  current.socket.write('{"requestId":"probe","op":"load"}\n');
  const [data] = await Promise.race([
    reply,
    closed.then(() => {
      throw Error("Setup cancellation closed the ready channel");
    }),
  ]);
  assert.deepEqual(JSON.parse(String(data)), { requestId: "probe", result: {} });
  await f.bridge({ ...request, op: "storage_detach" }, f.peer);
});

test("setup cancellation stops a Unix handshake that never becomes ready", {
  timeout: 2000,
}, async (t) => {
  const f = await fixture(t);
  const setup = new AbortController();
  const pending = f.bridge({ ...request, op: "storage_attach" }, f.peer, setup.signal);
  const rejection = assert.rejects(pending, /browser_storage_disconnected/);
  const current = await f.connected();
  const closed = once(current.socket, "close");
  setup.abort();
  await rejection;
  await closed;
  const replacement = f.bridge({ ...request, op: "storage_attach" }, f.peer);
  (await f.connected()).ready();
  await replacement;
  await f.bridge({ ...request, op: "storage_detach" }, f.peer);
});
