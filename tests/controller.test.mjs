import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { createServer } from "node:http";
import { test } from "node:test";
import { createController } from "../dist/controller.js";
import { createControllerRuntime } from "../dist/network-control.js";
import { createBrowserSessions } from "../dist/runtime-provider.js";

test("managed sessions register without a UID, allocate lazily, proxy files, and revoke before cleanup", async (t) => {
  let created = 0,
    removed = 0;
  const worker = createServer((req, res) => res.end(req.url));
  await new Promise((resolve) => worker.listen(0, "127.0.0.1", resolve));
  t.after(() => new Promise((resolve) => worker.close(resolve)));
  const origin = `http://127.0.0.1:${worker.address().port}`;
  const controller = await createController({
    port: 0,
    backend: {
      async create(placement) {
        created++;
        return {
          ...placement,
          id: randomUUID(),
          origin,
          runtime: {
            request: async (request) =>
              request.op === "register"
                ? {
                    cdpUrl:
                      origin.replace("http", "ws") + "/execution/" + "a".repeat(64),
                    viewerToken: "b".repeat(64),
                  }
                : { stopped: true },
          },
        };
      },
      async remove() {
        removed++;
      },
    },
  });
  t.after(() => controller.stop());
  const runtime = createControllerRuntime(controller.origin);
  t.after(() => runtime.close());
  const sessions = createBrowserSessions({
    provider: { acquire: async () => runtime, release: async () => {} },
  });
  const session = await sessions.open();
  assert.equal(session.executorUid, undefined);
  assert.equal(created, 0);
  const fileUrl = session.wsEndpoint.replace("ws:", "http:") + "/files/example.txt";
  assert.equal(
    await (await fetch(fileUrl)).text(),
    "/execution/" + "a".repeat(64) + "/files/example.txt",
  );
  assert.equal(created, 1);
  await fetch(fileUrl);
  assert.equal(created, 1);
  await session.finalize();
  assert.equal(removed, 1);
  assert.equal((await fetch(fileUrl)).status, 403);
});

test("revocation during first use fences readiness and deletes the late worker", async (t) => {
  let created;
  const started = new Promise((resolve) => {
    created = resolve;
  });
  let release;
  const barrier = new Promise((resolve) => {
    release = resolve;
  });
  let removed = 0;
  const controller = await createController({
    port: 0,
    backend: {
      async create(placement) {
        created();
        await barrier;
        return {
          ...placement,
          id: "late",
          origin: "http://127.0.0.1:1",
          runtime: { request: async () => assert.fail("cancelled registration") },
        };
      },
      async remove() {
        removed++;
      },
    },
  });
  t.after(() => controller.stop());
  const executionId = randomUUID();
  const { wsEndpoint } = await controller.command({ op: "register", executionId });
  const request = fetch(wsEndpoint.replace("ws:", "http:") + "/files/late.txt");
  await started;
  const finishing = controller.command({ op: "finalize", executionId });
  release();
  await finishing;
  assert.equal((await request).status, 502);
  assert.equal(removed, 1);
  assert.equal(controller.bindings.size, 0);
});

test("managed sessions reject tenant placement and persistent profiles before worker creation", async (t) => {
  const controller = await createController({
    port: 0,
    backend: {
      create: async () => assert.fail("unsupported placement allocated worker"),
      remove: async () => {},
    },
  });
  t.after(() => controller.stop());
  const runtime = createControllerRuntime(controller.origin);
  t.after(() => runtime.close());
  const sessions = createBrowserSessions({
    provider: {
      identity: "managed",
      acquire: async () => runtime,
      release: async () => {},
    },
  });
  await assert.rejects(
    sessions.open({ tenantId: "tenant" }),
    /browser_tenant_isolation_unavailable/,
  );
  await assert.rejects(
    sessions.open({ profileId: "profile" }),
    /browser_profile_storage_unavailable/,
  );
});

test("non-loopback controller listeners require a reachable public origin", async () => {
  await assert.rejects(
    createController({ host: "0.0.0.0", port: 0, backend: {} }),
    /browser_access_origin_required/,
  );
});

test("temporary worker transport loss removes the owned worker and reports failed graceful finalization", async (t) => {
  let removed = 0;
  const requestLife = new AbortController();
  const worker = createServer((_req, res) => res.end("fixture"));
  await new Promise((resolve) => worker.listen(0, "127.0.0.1", resolve));
  t.after(() => new Promise((resolve) => worker.close(resolve)));
  const origin = `http://127.0.0.1:${worker.address().port}`;
  const controller = await createController({
    port: 0,
    backend: {
      async create(placement) {
        return {
          ...placement,
          id: "temporary",
          origin,
          runtime: {
            request: async (request) => {
              if (request.op === "register")
                return {
                  cdpUrl:
                    origin.replace("http:", "ws:") + "/execution/" + "a".repeat(64),
                  viewerToken: "b".repeat(64),
                };
              requestLife.abort();
              throw Error("browser_control_disconnected");
            },
          },
        };
      },
      async remove(_worker, signal) {
        assert.equal(
          signal.aborted,
          false,
          "Owned cleanup inherited the dropped host request",
        );
        removed++;
      },
    },
  });
  t.after(() => controller.stop());
  const executionId = randomUUID();
  const { wsEndpoint } = await controller.command({ op: "register", executionId });
  await fetch(wsEndpoint.replace("ws:", "http:") + "/files/fixture.txt");
  await assert.rejects(
    controller.command({ op: "finalize", executionId }, undefined, requestLife.signal),
    /browser_worker_finalization_failed/,
  );
  assert.equal(removed, 1);
  assert.equal(controller.bindings.size, 0);
});
