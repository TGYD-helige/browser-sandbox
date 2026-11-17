import assert from "node:assert/strict";
import { createServer } from "node:http";
import { test } from "node:test";
import { createHostForwarding } from "../dist/controller-forwarding.js";
import { createNetworkHostControl } from "../dist/network-control.js";

test("concurrent forwarded host commands share the pending WebSocket handshake", async () => {
  const server = createServer();
  const host = createNetworkHostControl({
    server,
    dispatch: async (request) => ({ executionId: request.executionId }),
  });
  const forwarding = createHostForwarding();
  const life = new AbortController();
  try {
    await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
    const origin = `http://127.0.0.1:${server.address().port}`;
    const peer = { signal: life.signal, callback: async () => {} };
    const responses = await Promise.all(
      ["a", "b"].map((executionId) =>
        forwarding.request(origin, { executionId }, peer),
      ),
    );
    assert.deepEqual(responses, [{ executionId: "a" }, { executionId: "b" }]);
  } finally {
    life.abort();
    forwarding.close();
    host.close();
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
  }
});
