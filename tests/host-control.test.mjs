import assert from "node:assert/strict";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { connect, createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { attachStorage, hostRequest } from "../dist/host-client.js";
import { createHostControl } from "../dist/host-control.js";

async function splitWrite(socket, value) {
  const frame = Buffer.from(JSON.stringify(value) + "\n");
  const offset = frame.indexOf(Buffer.from("中")) + 1;
  assert(offset > 0);
  socket.write(frame.subarray(0, offset));
  await delay(25);
  socket.write(frame.subarray(offset));
}

test("host response and storage commit preserve UTF-8 split across socket reads", async () => {
  const root = await mkdtemp(join(tmpdir(), "host-utf8-"));
  const socketPath = join(root, "control.sock");
  const peers = new Set();
  const server = createServer((socket) => {
    peers.add(socket);
    socket.on("close", () => peers.delete(socket));
    socket.once("data", async (chunk) => {
      const request = JSON.parse(String(chunk));
      if (request.op === "storage_attach") {
        socket.write(JSON.stringify({ ready: true }) + "\n");
        await splitWrite(socket, {
          requestId: "commit",
          op: "commit",
          payload: { name: "中文" },
        });
      } else await splitWrite(socket, { result: { name: "中文" } });
    });
  });
  server.listen(socketPath + ".auth");
  await once(server, "listening");
  let storage;
  try {
    assert.deepEqual(await hostRequest(socketPath, { op: "status" }), { name: "中文" });
    let committed;
    const received = new Promise((resolve) => {
      committed = resolve;
    });
    storage = await attachStorage(
      socketPath,
      { executionId: "fixture", executorUid: 100001 },
      {
        load: async () => ({}),
        save: async () => ({}),
        commit: async (payload) => {
          committed(payload);
          return { confirmedSha: "ok" };
        },
      },
    );
    assert.deepEqual(await received, { name: "中文" });
  } finally {
    storage?.close();
    for (const socket of peers) socket.destroy();
    await new Promise((resolve) => server.close(resolve));
    await rm(root, { recursive: true, force: true });
  }
});

test("host request parsing preserves UTF-8 split across socket reads", async () => {
  const root = await mkdtemp(join(tmpdir(), "host-request-utf8-"));
  const socketPath = join(root, "control.sock");
  const control = await createHostControl({
    socketPath,
    controlTimeoutMs: 2000,
    profileFinalizeTimeoutMs: 2000,
    executions: { dispatch: async (request) => request.name },
  });
  const socket = connect(socketPath + ".auth");
  let result = "";
  socket.on("data", (chunk) => {
    result += chunk;
  });
  try {
    await once(socket, "connect");
    const ended = once(socket, "end");
    await splitWrite(socket, { op: "echo", name: "中文" });
    await ended;
    assert.deepEqual(JSON.parse(result), { result: "中文" });
  } finally {
    socket.destroy();
    await control.stop();
    await rm(root, { recursive: true, force: true });
  }
});
