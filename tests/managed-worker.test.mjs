import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import WebSocket, { WebSocketServer } from "ws";
import { connectCdp } from "../dist/cdp.js";
import { createController } from "../dist/controller.js";
import { createGateway } from "../dist/gateway.js";
import {
  createControllerRuntime,
  createNetworkHostControl,
} from "../dist/network-control.js";
import { sessionRequest } from "../dist/session.js";

test("controller and a private single-execution worker preserve text frames and session retention", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "managed-worker-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const browser = new WebSocketServer({ host: "127.0.0.1", port: 0 });
  await once(browser, "listening");
  t.after(
    () =>
      new Promise((resolve) => {
        for (const socket of browser.clients) socket.terminate();
        browser.close(resolve);
      }),
  );
  browser.on("connection", (socket) =>
    socket.on("message", (raw) => {
      const request = JSON.parse(String(raw));
      socket.send(
        JSON.stringify({
          id: request.id,
          result:
            request.method === "Target.getTargets"
              ? { targetInfos: [] }
              : { product: "synthetic-browser" },
        }),
      );
    }),
  );
  const executionId = randomUUID();
  const gateway = await createGateway({
    WebSocket,
    WebSocketServer,
    port: 0,
    socketPath: join(root, "control.sock"),
    dataRoot: join(root, "data"),
    healthy: async () => true,
    workerExecutionId: executionId,
    access: { port: 0 },
    launch: async () => ({
      endpoint: `ws://127.0.0.1:${browser.address().port}`,
      close: async () => {},
    }),
  });
  t.after(() => gateway.stop());
  const workerControl = createNetworkHostControl({
    server: gateway.workerServer,
    dispatch: (request, _peer, signal) => gateway.workerCommand(request, signal),
  });
  t.after(() => workerControl.close());
  const origin = `http://127.0.0.1:${gateway.accessPort}`;
  let allocations = 0;
  const controller = await createController({
    port: 0,
    backend: {
      async create(placement) {
        allocations++;
        return {
          ...placement,
          id: "synthetic",
          origin,
          runtime: createControllerRuntime(origin),
        };
      },
      async remove() {},
    },
  });
  t.after(() => controller.stop());
  const runtime = createControllerRuntime(controller.origin);
  t.after(() => runtime.close());
  await assert.rejects(
    gateway.workerCommand({ op: "register", executionId: randomUUID() }),
    /browser_identity_mismatch/,
  );
  const { wsEndpoint } = await runtime.request({ op: "register", executionId });
  let cdp = await connectCdp(wsEndpoint);
  assert.equal((await cdp.call("Browser.getVersion")).product, "synthetic-browser");
  const retained = await sessionRequest(wsEndpoint, "retain", 10000);
  cdp.close();
  await cdp.closed;
  cdp = await connectCdp(wsEndpoint);
  assert.equal((await cdp.call("Browser.getVersion")).product, "synthetic-browser");
  assert.equal((await sessionRequest(wsEndpoint)).generation, retained.generation);
  const access = await runtime.request({ op: "access_create", executionId });
  const viewer = new WebSocket(
    controller.origin.replace("http:", "ws:") + "/view/socket",
    { origin: controller.origin },
  );
  viewer.on("error", () => {});
  t.after(() => viewer.terminate());
  await once(viewer, "open");
  const frame = once(viewer, "message");
  viewer.send(
    JSON.stringify({
      type: "authenticate",
      token: new URL(access.viewerUrl).hash.slice(1),
    }),
  );
  const [raw, binary] = await frame;
  assert.equal(binary, false, "Viewer JSON must remain text over the proxy");
  assert.equal(JSON.parse(String(raw)).type, "state");
  await runtime.request({ op: "access_revoke", executionId });
  await assert.rejects(
    connectCdp(access.cdpUrl),
    /browser_cdp_connect_failed|browser_cdp_closed/,
  );
  assert.equal(
    (await fetch(wsEndpoint.replace("ws:", "http:") + "/session")).status,
    403,
  );
  cdp.close();
  await cdp.closed;
  await runtime.request({ op: "finalize", executionId });
  assert.equal(allocations, 1);
});
