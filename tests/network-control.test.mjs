import assert from "node:assert/strict";
import { createServer } from "node:http";
import { test } from "node:test";
import {
  createControllerRuntime,
  createNetworkHostControl,
} from "../dist/network-control.js";

test("network host control transports operations and rejects pending callbacks on disconnect", async (t) => {
  const server = createServer();
  let context;
  const control = createNetworkHostControl({
    server,
    dispatch: async (request, peer, signal) => {
      context = peer;
      if (request.op === "pause")
        return peer.callback("pause", { executionId: request.executionId }, signal);
      return { op: request.op, executionId: request.executionId };
    },
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(async () => {
    control.close();
    await new Promise((resolve) => server.close(resolve));
  });
  const runtime = createControllerRuntime(`http://127.0.0.1:${server.address().port}`, {
    automation: {
      pause: async (_id, signal) =>
        new Promise((_, reject) =>
          signal.addEventListener("abort", () => reject(Error("disconnected")), {
            once: true,
          }),
        ),
      resume: async () => {},
    },
  });
  t.after(() => runtime.close());
  assert.deepEqual(await runtime.request({ op: "register", executionId: "example" }), {
    op: "register",
    executionId: "example",
  });
  const pending = runtime.request({ op: "pause", executionId: "example" });
  const rejected = assert.rejects(pending, /browser_control_disconnected/);
  await new Promise((resolve) => setImmediate(resolve));
  runtime.close();
  await rejected;
  if (!context.signal.aborted)
    await new Promise((resolve) =>
      context.signal.addEventListener("abort", resolve, { once: true }),
    );
});

test("storage callbacks cross the host connection without serializing archive bytes", async (t) => {
  const server = createServer();
  const control = createNetworkHostControl({
    server,
    dispatch: async (request, peer, signal) => {
      if (request.op === "storage_attach") return { attached: true };
      const seed = await peer.callback(
        "load",
        { executionId: request.executionId },
        signal,
      );
      const target = await peer.callback(
        "save",
        { executionId: request.executionId },
        signal,
      );
      const committed = await peer.callback(
        "commit",
        {
          executionId: request.executionId,
          payload: { archive: { sha256: "fixture-sha" } },
        },
        signal,
      );
      return { seed, target, committed };
    },
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(async () => {
    control.close();
    await new Promise((resolve) => server.close(resolve));
  });
  const runtime = createControllerRuntime(`http://127.0.0.1:${server.address().port}`);
  t.after(() => runtime.close());
  const attached = await runtime.attachStorage(
    { executionId: "storage" },
    {
      load: async () => ({ absent: true }),
      save: async () => ({ url: "https://storage.example.test/archive" }),
      commit: async (payload) => {
        assert.equal(payload.archive.sha256, "fixture-sha");
        return { confirmedSha: "fixture-sha" };
      },
    },
  );
  assert.deepEqual(await runtime.request({ op: "roundtrip", executionId: "storage" }), {
    seed: { absent: true },
    target: { url: "https://storage.example.test/archive" },
    committed: { confirmedSha: "fixture-sha" },
  });
  attached.close();
  await assert.rejects(
    runtime.request({ op: "roundtrip", executionId: "storage" }),
    /browser_storage_disconnected/,
  );
});
