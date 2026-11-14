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
  const runtime = createControllerRuntime(controller.controlOrigin);
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
  const runtime = createControllerRuntime(controller.controlOrigin);
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

test("persistent registration acquires a cross-worker lease before allocation and releases it only on finish", async (t) => {
  let created = 0;
  const controller = await createController({
    port: 0,
    backend: {
      async list() {
        return [];
      },
      async create() {
        created++;
        throw Error("must remain lazy");
      },
      async remove() {},
    },
  });
  t.after(() => controller.stop());
  const runtime = createControllerRuntime(controller.controlOrigin);
  t.after(() => runtime.close());
  const adapter = {
    async load() {
      throw Error("unused must not load");
    },
    async save() {
      throw Error("unused must not save");
    },
    async commit() {
      throw Error("unused must not commit");
    },
  };
  const first = { executionId: randomUUID(), profileId: "account" };
  await runtime.attachStorage(first, adapter);
  await runtime.request({ op: "register", ...first, storage: true });
  await assert.rejects(
    runtime.attachStorage({ executionId: randomUUID(), profileId: "account" }, adapter),
    /browser_profile_busy/,
  );
  assert.equal(created, 0);
  await runtime.request({ op: "finalize", executionId: first.executionId });
  await runtime.attachStorage(
    { executionId: randomUUID(), profileId: "account" },
    adapter,
  );
  assert.equal(created, 0);
});

test("persistent registration fails closed on missing storage and failed or occupied worker discovery", async (t) => {
  const profileId = "account";
  for (const discovery of ["failed", "occupied"]) {
    let created = 0;
    const controller = await createController({
      port: 0,
      backend: {
        async list() {
          if (discovery === "failed") throw Error("browser_worker_discovery_failed");
          return [{ executionId: randomUUID(), incarnation: randomUUID(), profileId }];
        },
        async create() {
          created++;
        },
        async remove() {},
      },
    });
    t.after(() => controller.stop());
    const runtime = createControllerRuntime(controller.controlOrigin);
    t.after(() => runtime.close());
    await assert.rejects(
      runtime.request({ op: "register", executionId: randomUUID(), profileId }),
      /browser_profile_storage_unavailable/,
    );
    await assert.rejects(
      runtime.attachStorage({ executionId: randomUUID(), profileId }, {}),
      discovery === "failed" ? /discovery_failed/ : /browser_profile_busy/,
    );
    assert.equal(created, 0);
  }
});

test("Profile deletion is a host operation without an artificial execution identity", async (t) => {
  const controller = await createController({
    port: 0,
    backend: {
      async list() {
        return [];
      },
    },
    profileStore: {
      async delete(profileId) {
        assert.equal(profileId, "account");
        return true;
      },
    },
  });
  t.after(() => controller.stop());
  const runtime = createControllerRuntime(controller.controlOrigin);
  t.after(() => runtime.close());
  assert.deepEqual(
    await runtime.request({ op: "profile_delete", profileId: "account" }),
    { deleted: true },
  );
});

test("a transient discovery failure stays fenced until a later authoritative read succeeds", async (t) => {
  let reads = 0;
  const controller = await createController({
    port: 0,
    backend: {
      async list() {
        if (++reads === 1) throw Error("browser_discovery_transient");
        return [];
      },
    },
  });
  t.after(() => controller.stop());
  const runtime = createControllerRuntime(controller.controlOrigin);
  t.after(() => runtime.close());
  const adapter = {};
  await assert.rejects(
    runtime.attachStorage({ executionId: randomUUID(), profileId: "account" }, adapter),
    /browser_discovery_transient/,
  );
  await runtime.attachStorage(
    { executionId: randomUUID(), profileId: "account" },
    adapter,
  );
  assert.equal(reads, 2);
});

test("recovered Profile occupancy is released only after a successful absent-worker read", async (t) => {
  const executionId = randomUUID(),
    incarnation = randomUUID();
  let state = "present",
    reads = 0,
    deleted = 0;
  const controller = await createController({
    port: 0,
    backend: {
      async list() {
        reads++;
        if (state === "unknown") throw Error("browser_discovery_unavailable");
        return state === "present"
          ? [{ executionId, incarnation, profileId: "account" }]
          : [];
      },
    },
    profileStore: {
      async delete() {
        deleted++;
        return true;
      },
    },
  });
  t.after(() => controller.stop());
  await assert.rejects(
    controller.command({ op: "profile_delete", profileId: "account" }),
    /browser_profile_busy/,
  );
  state = "unknown";
  await assert.rejects(
    controller.command({ op: "profile_delete", profileId: "account" }),
    /browser_discovery_unavailable/,
  );
  assert.equal(deleted, 0);
  state = "absent";
  assert.deepEqual(
    await controller.command({ op: "profile_delete", profileId: "account" }),
    { deleted: true },
  );
  assert.equal(deleted, 1);
  assert.equal(reads, 3);
});

test("confirmed recovered-worker absence permits Profile reuse and idle expiry", async (t) => {
  let workers = [
    { executionId: randomUUID(), incarnation: randomUUID(), profileId: "account" },
  ];
  const controller = await createController({
    port: 0,
    backend: {
      async list() {
        return workers;
      },
    },
    profileStore: {
      async expire(_signal, isLeased) {
        return isLeased("account") ? 0 : 1;
      },
    },
  });
  t.after(() => controller.stop());
  assert.equal(await controller.expireProfiles(), 0);
  workers = [];
  assert.equal(await controller.expireProfiles(), 1);
  const runtime = createControllerRuntime(controller.controlOrigin);
  t.after(() => runtime.close());
  await runtime.attachStorage({ executionId: randomUUID(), profileId: "account" }, {});
});

for (const mode of ["volume", "host", "host-disconnect"])
  test(`creation cancellation reaches pending ${mode} storage setup`, {
    timeout: 3000,
  }, async (t) => {
    const entered = Promise.withResolvers(),
      release = Promise.withResolvers(),
      removed = Promise.withResolvers();
    let setupSignal;
    const executionId = randomUUID();
    const controller = await createController({
      port: 0,
      backend: {
        list: async () => [],
        create: async (placement) => ({
          ...placement,
          id: "worker",
          origin: "http://127.0.0.1:9",
          runtime: {
            request: async () => ({ cdpUrl: "ws://127.0.0.1:9", viewerToken: "token" }),
            attachStorage: async (_registration, _adapter, signal) => {
              setupSignal = signal;
              entered.resolve();
              await Promise.race([
                release.promise,
                new Promise((_, reject) =>
                  setupSignal?.addEventListener(
                    "abort",
                    () => reject(setupSignal.reason),
                    { once: true },
                  ),
                ),
              ]);
              return { close() {} };
            },
          },
        }),
        remove: async () => removed.resolve(),
      },
      profileStore: { open: () => ({ close: async () => {} }) },
    });
    t.after(async () => {
      release.resolve();
      await controller.stop();
    });
    const runtime = createControllerRuntime(controller.controlOrigin);
    t.after(() => runtime.close());
    const registration = { executionId, profileId: "cancelled-storage" };
    if (mode !== "volume") await runtime.attachStorage(registration, {});
    const endpoint = await runtime.request({
      op: "register",
      ...registration,
      storage: mode !== "volume" ? true : undefined,
    });
    const life = new AbortController();
    const response = fetch(
      endpoint.wsEndpoint.replace("ws:", "http:") + "/files/pending",
      { signal: life.signal },
    ).catch(() => {});
    t.after(() => life.abort());
    await entered.promise;
    assert(
      setupSignal instanceof AbortSignal,
      "Storage setup must receive the creation signal",
    );
    if (mode === "host-disconnect") runtime.close();
    else life.abort();
    await removed.promise;
    assert.equal(setupSignal.aborted, true);
    await response;
  });
