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
