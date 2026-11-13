import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { setImmediate as yieldTurn } from "node:timers/promises";
import { createController } from "../dist/controller.js";

async function fixture(t) {
  const directory = await mkdtemp(join(tmpdir(), "controller-recovery-"));
  const resources = new Map();
  const controllers = new Set();
  const worker = createServer((_req, res) => res.end("same-worker"));
  await new Promise((resolve) => worker.listen(0, "127.0.0.1", resolve));
  const origin = `http://127.0.0.1:${worker.address().port}`;
  let created = 0;
  const backend = {
    async list() {
      return [...resources.values()];
    },
    async create(placement) {
      created++;
      const resource = {
        ...placement,
        id: randomUUID(),
        origin,
        runtime: {
          async request(request) {
            if (request.op === "register")
              return {
                cdpUrl: `${origin.replace("http", "ws")}/execution/${"a".repeat(64)}`,
                viewerToken: "b".repeat(64),
              };
            if (request.op === "worker_status")
              return {
                protocol: "browser-sandbox.worker/v1",
                session: {
                  state: "retained",
                  generation: "original",
                  connectedClients: 0,
                  expiresAt: Date.now() + 10000,
                },
                takeover: "automation",
                pendingAuth: 0,
              };
            if (request.op === "session")
              return { state: "retained", generation: "original" };
            return { stopped: true };
          },
        },
      };
      resources.set(resource.id, resource);
      return resource;
    },
    async remove(resource) {
      resources.delete(resource.id);
    },
  };
  t.after(async () => {
    for (const controller of controllers) await controller.stop({ finalize: true });
    await new Promise((resolve) => worker.close(resolve));
    await rm(directory, { recursive: true, force: true });
  });
  return {
    directory,
    resources,
    backend,
    get created() {
      return created;
    },
    async start(extra = {}) {
      const controller = await createController({
        port: 0,
        backend,
        installationId: "test",
        stateDirectory: directory,
        ...extra,
      });
      controllers.add(controller);
      return controller;
    },
  };
}

const fileUrl = (controller, endpoint) =>
  `${controller.origin + new URL(endpoint.wsEndpoint).pathname}/files/test`;

test("controller restart preserves worker binding, original endpoint and retained generation", async (t) => {
  const f = await fixture(t);
  const executionId = randomUUID();
  const first = await f.start();
  const endpoint = await first.command({ op: "register", executionId });
  assert.equal(await (await fetch(fileUrl(first, endpoint))).text(), "same-worker");
  await first.stop();
  assert.equal(f.resources.size, 1);
  const second = await f.start();
  assert.equal(await (await fetch(fileUrl(second, endpoint))).text(), "same-worker");
  assert.equal(f.created, 1);
  assert.equal(
    (await second.command({ op: "session", executionId, action: "status" })).generation,
    "original",
  );
  await second.command({ op: "finalize", executionId });
  const third = await f.start();
  await assert.rejects(
    third.command({ op: "register", executionId }),
    /browser_execution_revoked/,
  );
});

test("unused registrations expire without allocating workers and remain fenced", async (t) => {
  t.mock.timers.enable({ apis: ["Date"], now: 1000 });
  const f = await fixture(t);
  const controller = await f.start({
    idleRegistrationMs: 100,
    reconciliationMs: 60000,
  });
  const executionId = randomUUID();
  await controller.command({ op: "register", executionId });
  t.mock.timers.tick(100);
  await controller.reconcile();
  assert.equal(f.created, 0);
  await assert.rejects(
    controller.command({ op: "register", executionId }),
    /browser_execution_revoked/,
  );
});

test("idle reclamation keeps retained sessions and failed status reads; maximum lifetime uses normal finalization", async (t) => {
  t.mock.timers.enable({ apis: ["Date"], now: 1000 });
  const f = await fixture(t);
  const controller = await f.start({
    idleRegistrationMs: 100,
    maxExecutionMs: 1000,
    reconciliationMs: 60000,
  });
  const executionId = randomUUID();
  const endpoint = await controller.command({ op: "register", executionId });
  await fetch(fileUrl(controller, endpoint));
  const worker = [...f.resources.values()][0];
  t.mock.timers.tick(100);
  await controller.reconcile();
  assert.equal(f.resources.size, 1);
  const original = worker.runtime.request;
  worker.runtime.request = async (request) => {
    if (request.op === "worker_status") throw Error("browser_control_disconnected");
    return original(request);
  };
  await controller.reconcile();
  assert.equal(f.resources.size, 1);
  worker.runtime.request = async (request) => {
    if (request.op === "finalize") throw Error("browser_profile_save_unconfirmed");
    return original(request);
  };
  t.mock.timers.tick(900);
  await controller.reconcile();
  assert.equal(f.resources.size, 1);
  assert.equal((await fetch(fileUrl(controller, endpoint))).status, 403);
  worker.runtime.request = original;
  await controller.command({ op: "finalize", executionId });
  assert.equal(f.resources.size, 0);
});

test("failed deletion retries only infrastructure after confirmed finalization, including after restart", async (t) => {
  const f = await fixture(t);
  const controller = await f.start();
  const executionId = randomUUID();
  const endpoint = await controller.command({ op: "register", executionId });
  await fetch(fileUrl(controller, endpoint));
  const worker = [...f.resources.values()][0];
  const request = worker.runtime.request;
  let finalizations = 0;
  worker.runtime.request = async (input) => {
    if (input.op === "finalize") finalizations++;
    return request(input);
  };
  const remove = f.backend.remove;
  f.backend.remove = async () => {
    throw Error("browser_docker_unavailable");
  };
  await assert.rejects(
    controller.command({ op: "finalize", executionId }),
    /browser_docker_unavailable/,
  );
  assert.equal(controller.capacity().workers, 1);
  await controller.stop();
  f.backend.remove = remove;
  const restarted = await f.start();
  await restarted.reconcile();
  assert.equal(f.resources.size, 0);
  assert.equal(finalizations, 1);
  assert.equal(restarted.capacity().workers, 0);
});

test("failed discovery blocks admission and lost workers never renew into replacements", async (t) => {
  const f = await fixture(t);
  const controller = await f.start();
  const executionId = randomUUID();
  const endpoint = await controller.command({ op: "register", executionId });
  await fetch(fileUrl(controller, endpoint));
  await controller.stop();
  const list = f.backend.list;
  f.backend.list = async () => {
    throw Error("discovery_failed");
  };
  await assert.rejects(f.start(), /discovery_failed/);
  f.backend.list = list;
  f.resources.clear();
  const restarted = await f.start();
  await assert.rejects(
    restarted.command({
      op: "session",
      executionId,
      action: "renew",
      durationMs: 10000,
    }),
    /browser_execution_revoked/,
  );
  assert.equal((await fetch(fileUrl(restarted, endpoint))).status, 403);
  assert.equal(f.created, 1);
});

async function until(predicate) {
  const deadline = Date.now() + 5000;
  while (!predicate()) {
    if (Date.now() > deadline) assert.fail("reported controller state did not settle");
    await yieldTurn();
  }
}

test("controller queue overflow is explicit and a disconnected queued client never starts a worker", async (t) => {
  const f = await fixture(t);
  const controller = await f.start({ maxWorkers: 1, maxPending: 1, maxQueued: 1 });
  const endpoints = [];
  for (let index = 0; index < 3; index++)
    endpoints.push(
      await controller.command({ op: "register", executionId: randomUUID() }),
    );
  await fetch(fileUrl(controller, endpoints[0]));
  const client = new AbortController();
  const queued = fetch(fileUrl(controller, endpoints[1]), { signal: client.signal });
  const cancelled = assert.rejects(queued);
  await until(() => controller.capacity().queued === 1);
  const rejected = await fetch(fileUrl(controller, endpoints[2]));
  assert.equal(rejected.status, 502);
  assert.equal((await rejected.json()).error, "browser_worker_queue_full");
  client.abort();
  await cancelled;
  await until(() => controller.capacity().queued === 0);
  assert.equal(f.created, 1);
});

test("one disconnected client does not cancel a shared creation while another waits", async (t) => {
  const f = await fixture(t);
  const entered = Promise.withResolvers(),
    release = Promise.withResolvers();
  const create = f.backend.create;
  f.backend.create = async (placement, signal) => {
    entered.resolve(signal);
    await release.promise;
    signal.throwIfAborted();
    return create(placement, signal);
  };
  const controller = await f.start();
  const endpoint = await controller.command({
    op: "register",
    executionId: randomUUID(),
  });
  const client = new AbortController();
  const first = fetch(fileUrl(controller, endpoint), { signal: client.signal });
  const cancelled = assert.rejects(first);
  const platformSignal = await entered.promise;
  const second = fetch(fileUrl(controller, endpoint));
  await until(() => controller.capacity().waitingClients === 2);
  client.abort();
  await cancelled;
  await until(() => controller.capacity().waitingClients === 1);
  assert.equal(platformSignal.aborted, false);
  release.resolve();
  assert.equal(await (await second).text(), "same-worker");
  assert.equal(f.created, 1);
});

test("incompatible worker protocol and lost generation reject renewal while retaining recovery resources", async (t) => {
  const f = await fixture(t);
  const first = await f.start();
  const executionId = randomUUID();
  const endpoint = await first.command({ op: "register", executionId });
  await fetch(fileUrl(first, endpoint));
  await first.command({ op: "session", executionId, action: "status" });
  const worker = [...f.resources.values()][0];
  const original = worker.runtime.request;
  worker.runtime.request = async (request) =>
    request.op === "worker_status"
      ? { protocol: "browser-sandbox.worker/v2" }
      : original(request);
  await first.stop();
  const second = await f.start();
  await assert.rejects(
    second.command({ op: "session", executionId, action: "renew", durationMs: 1000 }),
    /browser_worker_protocol_unsupported/,
  );
  await second.reconcile();
  assert.equal(f.resources.size, 1);
  await second.stop();
  worker.runtime.request = async (request) =>
    request.op === "worker_status"
      ? {
          protocol: "browser-sandbox.worker/v1",
          session: { state: "lost", generation: "original", connectedClients: 0 },
          takeover: "automation",
          pendingAuth: 0,
        }
      : original(request);
  const third = await f.start();
  await assert.rejects(
    third.command({ op: "session", executionId, action: "renew", durationMs: 1000 }),
    /browser_session_lost/,
  );
  assert.equal((await fetch(fileUrl(third, endpoint))).status, 403);
  assert.equal(f.created, 1);
  assert.equal(f.resources.size, 1);
});

test("owned orphans use normal finalization and preserve resources when save is unconfirmed", async (t) => {
  const f = await fixture(t);
  const worker = await f.backend.create({
    executionId: randomUUID(),
    incarnation: randomUUID(),
  });
  const original = worker.runtime.request;
  worker.runtime.request = async (request) => {
    if (request.op === "finalize") throw Error("browser_profile_save_unconfirmed");
    return original(request);
  };
  const controller = await f.start();
  await controller.reconcile();
  assert.equal(f.resources.size, 1);
  assert.equal(controller.capacity().workers, 1);
  await assert.rejects(
    controller.command({ op: "register", executionId: worker.executionId }),
    /browser_execution_revoked/,
  );
  worker.runtime.request = original;
  await controller.reconcile();
  assert.equal(f.resources.size, 0);
  assert.equal(controller.capacity().workers, 0);
});

test("unconfirmed partial creation retains capacity until successful discovery proves absence", async (t) => {
  const f = await fixture(t);
  const controller = await f.start({ maxWorkers: 1 });
  const executionId = randomUUID();
  const endpoint = await controller.command({ op: "register", executionId });
  f.backend.create = async () => {
    throw Error("browser_worker_cleanup_unconfirmed");
  };
  const response = await fetch(fileUrl(controller, endpoint));
  assert.equal(response.status, 502);
  assert.equal(controller.capacity().workers, 1);
  const list = f.backend.list;
  f.backend.list = async () => {
    throw Error("browser_worker_discovery_failed");
  };
  await assert.rejects(
    controller.command({ op: "finalize", executionId }),
    /browser_worker_discovery_failed/,
  );
  assert.equal(controller.capacity().workers, 1);
  f.backend.list = list;
  await controller.command({ op: "finalize", executionId });
  assert.equal(controller.capacity().workers, 0);
});

test("semantically corrupt cleanup confirmations cannot skip worker finalization", async (t) => {
  for (const corrupt of [
    { stopConfirmed: "false" },
    { stopConfirmed: true, finalResult: { stopped: false } },
    { phase: "invented" },
  ]) {
    const f = await fixture(t);
    const first = await f.start();
    const executionId = randomUUID();
    const endpoint = await first.command({ op: "register", executionId });
    await fetch(fileUrl(first, endpoint));
    await first.stop();
    const path = join(f.directory, `${executionId}.json`);
    const record = JSON.parse(await readFile(path, "utf8"));
    Object.assign(record.data, corrupt);
    const broken = JSON.stringify(record);
    await writeFile(path, broken);
    await assert.rejects(f.start(), /browser_execution_journal_corrupt/);
    assert.equal(await readFile(path, "utf8"), broken);
    assert.equal(f.resources.size, 1);
  }
});

test("failed controller listen closes recovered transports without deleting workers", async (t) => {
  const f = await fixture(t);
  const first = await f.start();
  const endpoint = await first.command({ op: "register", executionId: randomUUID() });
  await fetch(fileUrl(first, endpoint));
  await first.stop();
  let closes = 0;
  const worker = [...f.resources.values()][0];
  worker.runtime.close = () => {
    closes++;
  };
  const blocker = createServer();
  await new Promise((resolve) => blocker.listen(0, "127.0.0.1", resolve));
  t.after(() => new Promise((resolve) => blocker.close(resolve)));
  await assert.rejects(f.start({ port: blocker.address().port }), {
    code: "EADDRINUSE",
  });
  assert.equal(closes, 1);
  assert.equal(f.resources.size, 1);
});
