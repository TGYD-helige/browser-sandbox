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

test("controller routes attached storage callbacks between the real host and worker transports", async (t) => {
  const { createController } = await import("../dist/controller.js");
  const { randomUUID } = await import("node:crypto");
  const executionId = randomUUID();
  const worker = createServer((_req, res) => res.end("fixture"));
  let callbacks;
  const control = createNetworkHostControl({
    server: worker,
    dispatch: async (request, peer, signal) => {
      if (request.op === "register")
        return {
          cdpUrl: `ws://127.0.0.1:${worker.address().port}/execution/` + "a".repeat(64),
          viewerToken: "b".repeat(64),
        };
      if (request.op === "storage_attach") {
        callbacks = await peer.callback("load", { executionId }, signal);
        return { attached: true };
      }
      return { stopped: true };
    },
  });
  await new Promise((resolve) => worker.listen(0, "127.0.0.1", resolve));
  t.after(async () => {
    control.close();
    await new Promise((resolve) => worker.close(resolve));
  });
  const origin = `http://127.0.0.1:${worker.address().port}`;
  const controller = await createController({
    port: 0,
    backend: {
      create: async (placement) => ({
        ...placement,
        id: "synthetic",
        origin,
        runtime: createControllerRuntime(origin),
      }),
      remove: async () => {},
    },
  });
  t.after(() => controller.stop());
  const runtime = createControllerRuntime(controller.controlOrigin);
  t.after(() => runtime.close());
  const attachment = await runtime.attachStorage(
    { executionId },
    {
      load: async () => ({ absent: true }),
      save: async () => ({}),
      commit: async () => ({ confirmedSha: "unused" }),
    },
  );
  const { wsEndpoint } = await runtime.request({ op: "register", executionId });
  assert.equal(callbacks, undefined, "Attachment allocated a worker eagerly");
  assert.equal(
    await (
      await fetch(wsEndpoint.replace("ws:", "http:") + "/files/fixture.txt")
    ).text(),
    "fixture",
  );
  assert.deepEqual(callbacks, { absent: true });
  attachment.close();
  await runtime.request({ op: "storage_detach", executionId });
  const lateAttachment = await runtime.attachStorage(
    { executionId },
    {
      load: async () => ({ absent: false }),
      save: async () => ({}),
      commit: async () => ({ confirmedSha: "unused" }),
    },
  );
  assert.deepEqual(
    callbacks,
    { absent: false },
    "Attachment after worker readiness was silently ignored",
  );
  lateAttachment.close();
  await runtime.request({ op: "finalize", executionId });
  attachment.close();
});

test("storage attachment cancellation reaches the remote operation", {
  timeout: 2000,
}, async (t) => {
  const server = createServer();
  let enter, cancelled;
  const entered = new Promise((resolve) => {
    enter = resolve;
  });
  const aborted = new Promise((resolve) => {
    cancelled = resolve;
  });
  const control = createNetworkHostControl({
    server,
    dispatch: async (_request, _peer, signal) => {
      enter();
      await new Promise((resolve) =>
        signal.addEventListener("abort", resolve, { once: true }),
      );
      cancelled();
      return { attached: false };
    },
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const runtime = createControllerRuntime(`http://127.0.0.1:${server.address().port}`);
  t.after(async () => {
    runtime.close();
    control.close();
    await new Promise((resolve) => server.close(resolve));
  });
  const life = new AbortController();
  const rejected = assert.rejects(
    runtime.attachStorage({ executionId: "cancelled" }, {}, life.signal),
    /browser_control_timeout/,
  );
  await entered;
  life.abort();
  await rejected;
  await aborted;
});

test("duplicate attachment and repeated stale close preserve the current host adapter", async (t) => {
  const server = createServer((_req, res) => res.end());
  let attached = false;
  const control = createNetworkHostControl({
    server,
    dispatch: async (request, peer, signal) => {
      if (request.op === "storage_attach") {
        if (attached) throw Error("browser_storage_already_attached");
        attached = true;
        return { attached: true };
      }
      if (request.op === "storage_detach") {
        attached = false;
        return { detached: true };
      }
      if (request.op === "probe")
        return peer.callback("load", { executionId: "fixture" }, signal);
      return {};
    },
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(() => {
    control.close();
    return new Promise((resolve) => server.close(resolve));
  });
  const runtime = createControllerRuntime(`http://127.0.0.1:${server.address().port}`);
  t.after(() => runtime.close());
  const original = await runtime.attachStorage(
    { executionId: "fixture" },
    { load: async () => ({ marker: "original" }) },
  );
  await assert.rejects(
    runtime.attachStorage(
      { executionId: "fixture" },
      { load: async () => ({ marker: "duplicate" }) },
    ),
    /browser_storage_already_attached/,
  );
  assert.deepEqual(await runtime.request({ op: "probe" }), { marker: "original" });
  original.close();
  await runtime.request({ op: "barrier" });
  const next = await runtime.attachStorage(
    { executionId: "fixture" },
    { load: async () => ({ marker: "next" }) },
  );
  original.close();
  assert.deepEqual(await runtime.request({ op: "probe" }), { marker: "next" });
  next.close();
});

