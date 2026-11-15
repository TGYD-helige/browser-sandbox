import assert from "node:assert/strict";
import { randomBytes, randomUUID } from "node:crypto";
import { once } from "node:events";
import { mkdtemp, open, readFile, rename, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { setImmediate as yieldTurn } from "node:timers/promises";
import WebSocket, { WebSocketServer } from "ws";
import { createController } from "../dist/controller.js";
import { createExecutionJournal } from "../dist/execution-journal.js";

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
          async attachStorage() {
            return { close() {} };
          },
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
    const results = await Promise.allSettled(
      [...controllers].map((controller) => controller.stop({ finalize: true })),
    );
    await new Promise((resolve) => worker.close(resolve));
    await rm(directory, { recursive: true, force: true });
    const failure = results.find((result) => result.status === "rejected");
    if (failure) throw failure.reason;
  });
  return {
    directory,
    resources,
    worker,
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
  t.mock.timers.enable({ apis: ["Date"], now: 1000 });
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
  t.mock.timers.tick(1000);
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
  assert.equal(controller.capacity().pending, 0);
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
    { phase: "active", cleanupUnconfirmed: true },
    { phase: "recovery-required", cleanupUnconfirmed: true, generation: "published" },
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

test("network-only Profile orphans survive another restart after failed deletion", async (t) => {
  t.mock.timers.enable({ apis: ["Date"], now: 1000 });
  const f = await fixture(t);
  const worker = {
    id: "network/owned",
    executionId: randomUUID(),
    incarnation: randomUUID(),
    profileId: "persistent-profile",
    infrastructureOnly: true,
    origin: "",
    runtime: {
      request: async () => {
        throw Error("must not connect");
      },
    },
  };
  f.resources.set(worker.id, worker);
  const remove = f.backend.remove;
  f.backend.remove = async () => {
    throw Error("network removal failed");
  };
  const first = await f.start();
  await first.reconcile();
  assert.equal(f.resources.size, 1);
  await first.stop();
  const second = await f.start();
  assert.equal(second.bindings.get(worker.executionId).profileId, worker.profileId);
  await second.reconcile();
  assert.equal(f.resources.size, 1);
  f.backend.remove = remove;
  t.mock.timers.tick(2000);
  await second.reconcile();
  assert.equal(f.resources.size, 0);
  assert.equal(second.capacity().workers, 0);
});

test("slow worker creation refreshes idle activity before the first CDP connection", async (t) => {
  const f = await fixture(t);
  let now = Date.now();
  t.mock.method(Date, "now", () => now);
  const create = f.backend.create;
  f.backend.create = async (placement) => {
    now += 5000;
    const worker = await create(placement);
    const request = worker.runtime.request;
    worker.runtime.request = async (message) =>
      message.op === "worker_status"
        ? {
            protocol: "browser-sandbox.worker/v1",
            session: { state: "unused", connectedClients: 0 },
            takeover: "automation",
            pendingAuth: 0,
          }
        : request(message);
    return worker;
  };
  const controller = await f.start({ idleRegistrationMs: 1000 });
  const endpoint = await controller.command({
    op: "register",
    executionId: randomUUID(),
  });
  assert.equal((await fetch(fileUrl(controller, endpoint))).status, 200);
  await controller.reconcile();
  assert.equal(f.resources.size, 1);
});

test("restart cleans a network-only remainder of unconfirmed partial creation", async (t) => {
  const f = await fixture(t);
  f.backend.create = async (placement) => {
    const worker = {
      ...placement,
      id: "network/partial",
      origin: "",
      infrastructureOnly: true,
      runtime: {
        request: async () => {
          throw Error("must not connect");
        },
      },
    };
    f.resources.set(worker.id, worker);
    throw Error("browser_worker_cleanup_unconfirmed");
  };
  const first = await f.start();
  const endpoint = await first.command({ op: "register", executionId: randomUUID() });
  assert.equal((await fetch(fileUrl(first, endpoint))).status, 502);
  await assert.rejects(first.stop(), /browser_worker_cleanup_unconfirmed/);
  const second = await f.start();
  assert.equal(second.capacity().workers, 1);
  await second.reconcile();
  assert.equal(f.resources.size, 0);
  assert.equal(second.capacity().workers, 0);
});

test("restart retries deletion of an unregistered partial worker without runtime finalization", async (t) => {
  t.mock.timers.enable({ apis: ["Date"], now: 1000 });
  const f = await fixture(t);
  const create = f.backend.create;
  let finalized = 0;
  f.backend.create = async (placement) => {
    const worker = await create(placement);
    worker.runtime.request = async (request) => {
      if (request.op === "worker_status") throw Error("browser_execution_revoked");
      finalized++;
      return { status: "absent" };
    };
    throw Error("browser_worker_cleanup_unconfirmed");
  };
  const first = await f.start();
  const endpoint = await first.command({ op: "register", executionId: randomUUID() });
  assert.equal((await fetch(fileUrl(first, endpoint))).status, 502);
  await assert.rejects(first.stop(), /browser_worker_cleanup_unconfirmed/);
  const second = await f.start();
  const remove = f.backend.remove;
  f.backend.remove = async () => {
    throw Error("browser_worker_cleanup_unconfirmed");
  };
  await second.reconcile();
  assert.equal(second.capacity().workers, 1);
  assert.equal(f.resources.size, 1);
  f.backend.remove = remove;
  t.mock.timers.tick(1000);
  await second.reconcile();
  assert.equal(f.resources.size, 0);
  assert.equal(second.capacity().workers, 0);
  assert.equal(finalized, 0);
});

test("revoked recovered bindings do not consume the live access grant quota", async (t) => {
  const f = await fixture(t);
  const journal = await createExecutionJournal({
    directory: f.directory,
    installationId: "test",
  });
  for (let index = 0; index < 21; index++) {
    await journal.create(randomUUID(), {
      incarnation: randomUUID(),
      allowedDomains: [],
      registeredAt: Date.now(),
      lastActivityAt: Date.now(),
      phase: "cleanup-pending",
      stopConfirmed: true,
      finalResult: { stopped: true, used: false },
      grants: Array.from({ length: 100 }, () => ({
        token: randomBytes(32).toString("hex"),
        kind: "cdp",
        control: false,
        resize: false,
      })),
    });
  }
  const controller = await f.start();
  const endpoint = await controller.command({
    op: "register",
    executionId: randomUUID(),
  });
  assert.equal(
    (
      await fetch(
        `${controller.origin + new URL(endpoint.wsEndpoint).pathname}/session`,
      )
    ).status,
    200,
  );
});

test("recovery releases the Profile lease when discovery confirms its worker disappeared", async (t) => {
  const f = await fixture(t);
  const profileStore = { open: () => ({ close: async () => {} }) };
  const first = await f.start({ profileStore, reconciliationMs: 60000 });
  const executionId = randomUUID();
  const endpoint = await first.command({
    op: "register",
    executionId,
    profileId: "absent-profile",
  });
  await fetch(fileUrl(first, endpoint));
  await first.stop();
  f.resources.clear();
  const second = await f.start({ profileStore, reconciliationMs: 60000 });
  await second.reconcile();
  const status = await second.command({ op: "controller_status", executionId });
  assert.equal(status.execution.phase, "finished");
  assert.equal(second.capacity().workers, 0);
  assert.equal(f.created, 1);
  await assert.rejects(
    second.command({ op: "session", executionId, action: "renew", durationMs: 1000 }),
    /browser_execution_revoked/,
  );
  await second.command({
    op: "register",
    executionId: randomUUID(),
    profileId: "absent-profile",
  });
});

test("unreachable temporary recovery retries before deleting its infrastructure", async (t) => {
  t.mock.timers.enable({ apis: ["Date"], now: 1000 });
  const f = await fixture(t);
  const first = await f.start({ reconciliationMs: 60000 });
  const executionId = randomUUID();
  const endpoint = await first.command({ op: "register", executionId });
  await fetch(fileUrl(first, endpoint));
  await first.stop();
  const worker = [...f.resources.values()][0];
  const request = worker.runtime.request;
  worker.runtime.request = async () => {
    throw Error("browser_control_disconnected");
  };
  const second = await f.start({ reconciliationMs: 60000 });
  t.after(() => {
    worker.runtime.request = request;
  });
  await second.reconcile();
  t.mock.timers.tick(1000);
  await second.reconcile();
  assert.equal(f.resources.size, 1);
  t.mock.timers.tick(2000);
  await second.reconcile();
  assert.equal(f.resources.size, 0);
  assert.equal(second.capacity().workers, 0);
  assert.equal(
    (await second.command({ op: "controller_status", executionId })).execution.phase,
    "finished",
  );
});

test("unconfirmed persistent recovery becomes visible and retains its lease until explicit finish", async (t) => {
  t.mock.timers.enable({ apis: ["Date"], now: 1000 });
  const f = await fixture(t);
  const profileStore = { open: () => ({ close: async () => {} }) };
  const first = await f.start({ profileStore, reconciliationMs: 60000 });
  const executionId = randomUUID();
  const endpoint = await first.command({
    op: "register",
    executionId,
    profileId: "kept-profile",
  });
  await fetch(fileUrl(first, endpoint));
  await first.stop();
  const worker = [...f.resources.values()][0];
  const request = worker.runtime.request;
  worker.runtime.request = async () => {
    throw Error("browser_control_disconnected");
  };
  t.after(() => {
    worker.runtime.request = request;
  });
  const events = [];
  const second = await f.start({
    profileStore,
    reconciliationMs: 60000,
    onWorkerEvent: (event) => events.push(event),
  });
  for (const elapsed of [0, 1000, 2000]) {
    t.mock.timers.tick(elapsed);
    await second.reconcile();
  }
  const status = await second.command({ op: "controller_status", executionId });
  assert.equal(status.execution.phase, "recovery-blocked");
  assert.equal(status.recoveryBlocked, 1);
  assert.equal(status.execution.recoveryAttempts, 3);
  assert.equal(status.execution.statusError, "browser_control_disconnected");
  assert.equal(events.filter((event) => event.type === "recovery-blocked").length, 1);
  assert.equal(second.capacity().workers, 1);
  await assert.rejects(
    second.command({
      op: "register",
      executionId: randomUUID(),
      profileId: "kept-profile",
    }),
    /browser_profile_busy/,
  );
  await second.stop();
  let storageAttached = false;
  worker.runtime.attachStorage = async () => {
    storageAttached = true;
    return { close() {} };
  };
  const third = await f.start({ profileStore, reconciliationMs: 60000 });
  await third.reconcile();
  assert.equal(
    (await third.command({ op: "controller_status", executionId })).execution.phase,
    "recovery-blocked",
  );
  assert.equal(third.capacity().workers, 1);
  worker.runtime.request = async (input) => {
    if (input.op === "finalize" && !storageAttached)
      throw Error("browser_profile_save_unconfirmed");
    return request(input);
  };
  await third.command({ op: "finish", executionId });
  assert.equal(storageAttached, true);
  assert.equal(f.resources.size, 0);
  assert.equal(third.capacity().workers, 0);
  await third.command({
    op: "register",
    executionId: randomUUID(),
    profileId: "kept-profile",
  });
});

test("late protocol mismatch preserves the original endpoint through compatible rollback", async (t) => {
  t.mock.timers.enable({ apis: ["Date"], now: 1000 });
  const f = await fixture(t);
  const first = await f.start({ reconciliationMs: 60000 });
  const executionId = randomUUID();
  const endpoint = await first.command({ op: "register", executionId });
  await fetch(fileUrl(first, endpoint));
  await first.stop();
  const worker = [...f.resources.values()][0];
  const request = worker.runtime.request;
  let statusError = true;
  worker.runtime.request = async (input) => {
    if (input.op !== "worker_status") return request(input);
    if (statusError) throw Error("browser_control_disconnected");
    return { protocol: "browser-sandbox.worker/v2" };
  };
  t.after(() => {
    worker.runtime.request = request;
  });
  const second = await f.start({ reconciliationMs: 60000 });
  await second.reconcile();
  statusError = false;
  t.mock.timers.tick(1000);
  await second.reconcile();
  assert.equal(
    (await second.command({ op: "controller_status", executionId })).execution.phase,
    "recovery-required",
  );
  await second.stop();
  worker.runtime.request = request;
  const third = await f.start({ reconciliationMs: 60000 });
  assert.equal(await (await fetch(fileUrl(third, endpoint))).text(), "same-worker");
  assert.equal(f.created, 1);
});

test("a cancelled creation can retry the same registered execution", {
  timeout: 5000,
}, async (t) => {
  const f = await fixture(t);
  const entered = Promise.withResolvers();
  const create = f.backend.create;
  let attempts = 0,
    failed = false;
  f.backend.create = async (placement, signal) => {
    if (++attempts === 1) {
      await new Promise((_, reject) => {
        signal.addEventListener("abort", () => reject(signal.reason), { once: true });
        entered.resolve();
      });
    }
    return create(placement, signal);
  };
  const controller = await f.start({
    maxWorkers: 1,
    onWorkerEvent: (event) => {
      if (event.type === "failed") failed = true;
    },
  });
  const endpoint = await controller.command({
    op: "register",
    executionId: randomUUID(),
  });
  const caller = new AbortController();
  const first = fetch(fileUrl(controller, endpoint), { signal: caller.signal });
  const cancelled = assert.rejects(first);
  await entered.promise;
  caller.abort();
  await cancelled;
  await until(() => failed);
  assert.equal(controller.capacity().workers, 0);
  assert.equal(
    await (await fetch(fileUrl(controller, endpoint))).text(),
    "same-worker",
  );
  assert.equal(attempts, 2);
  assert.equal(f.resources.size, 1);
});

test("a failed recovery journal write does not stop reclamation of other executions", async (t) => {
  t.mock.timers.enable({ apis: ["Date"], now: 1000 });
  const f = await fixture(t);
  const first = await f.start({ reconciliationMs: 60000 });
  const executionId = randomUUID();
  const endpoint = await first.command({ op: "register", executionId });
  await fetch(fileUrl(first, endpoint));
  await first.stop();
  const worker = [...f.resources.values()][0];
  const request = worker.runtime.request;
  worker.runtime.request = async (input) => {
    if (input.op === "worker_status") throw Error("browser_control_disconnected");
    return request(input);
  };
  const second = await f.start({ reconciliationMs: 60000, idleRegistrationMs: 100 });
  const unused = randomUUID();
  await second.command({ op: "register", executionId: unused });
  const parked = `${f.directory}-unavailable`;
  await rename(f.directory, parked);
  try {
    for (const elapsed of [0, 1000, 2000]) {
      t.mock.timers.tick(elapsed);
      await second.reconcile();
    }
  } finally {
    await rename(parked, f.directory);
    worker.runtime.request = request;
    t.after(async () => {
      await second.stop({ finalize: false }).catch(() => {});
    });
  }
  assert.equal(
    (await second.command({ op: "controller_status", executionId })).execution.phase,
    "recovery-required",
  );
  t.mock.timers.tick(4000);
  await second.reconcile();
  assert.equal(
    (await second.command({ op: "controller_status", executionId: unused })).execution
      .phase,
    "finished",
  );
  assert.equal(
    (await second.command({ op: "controller_status", executionId })).execution.phase,
    "finished",
  );
  await second.stop({ finalize: false });
});

test("discovered Pending workers retain capacity without preventing controller startup", async (t) => {
  t.mock.timers.enable({ apis: ["Date"], now: 1000 });
  const f = await fixture(t);
  const first = await f.start({ reconciliationMs: 60000 });
  const executionId = randomUUID();
  const endpoint = await first.command({ op: "register", executionId });
  await fetch(fileUrl(first, endpoint));
  await first.stop();
  const worker = [...f.resources.values()][0];
  f.backend.list = async () =>
    f.resources.has(worker.id)
      ? [
          {
            ...worker,
            get runtime() {
              throw Error("browser_worker_not_ready");
            },
          },
        ]
      : [];
  const second = await f.start({ reconciliationMs: 60000 });
  assert.equal(second.capacity().workers, 1);
  for (const elapsed of [0, 1000, 2000]) {
    t.mock.timers.tick(elapsed);
    await second.reconcile();
  }
  assert.equal(second.capacity().workers, 0);
  assert.equal(
    (await second.command({ op: "controller_status", executionId })).execution.phase,
    "finished",
  );
});

test("automatic temporary cleanup preserves status and deletion failures and closes its channel", async (t) => {
  t.mock.timers.enable({ apis: ["Date"], now: 1000 });
  const f = await fixture(t);
  const first = await f.start({ reconciliationMs: 60000 });
  const executionId = randomUUID();
  const endpoint = await first.command({ op: "register", executionId });
  await fetch(fileUrl(first, endpoint));
  await first.stop();
  const worker = [...f.resources.values()][0];
  const request = worker.runtime.request,
    remove = f.backend.remove;
  let closed = 0;
  worker.runtime.close = () => {
    closed++;
  };
  worker.runtime.request = async () => {
    throw Error("browser_control_disconnected");
  };
  f.backend.remove = async () => {
    throw Error("browser_docker_unavailable");
  };
  try {
    const second = await f.start({ reconciliationMs: 60000 });
    for (const elapsed of [0, 1000, 2000]) {
      t.mock.timers.tick(elapsed);
      await second.reconcile();
    }
    const status = await second.command({ op: "controller_status", executionId });
    assert.equal(status.execution.phase, "recovery-required");
    assert.match(status.execution.statusError, /browser_control_disconnected/);
    assert.match(status.execution.statusError, /browser_docker_unavailable/);
    assert.equal(closed, 1);
    assert.equal(second.capacity().workers, 1);
  } finally {
    worker.runtime.request = request;
    f.backend.remove = remove;
  }
});

test("normal save retry does not require a live session after the worker has stopped", async (t) => {
  t.mock.timers.enable({ apis: ["Date"], now: 1000 });
  const f = await fixture(t);
  const controller = await f.start({
    profileStore: { open: () => ({ close: async () => {} }) },
    maxExecutionMs: 100,
    reconciliationMs: 60000,
  });
  const executionId = randomUUID();
  const endpoint = await controller.command({
    op: "register",
    executionId,
    profileId: "save-retry",
  });
  await fetch(fileUrl(controller, endpoint));
  const worker = [...f.resources.values()][0];
  const request = worker.runtime.request;
  let stopped = false,
    recover = false,
    saves = 0;
  worker.runtime.request = async (input) => {
    if (input.op === "worker_status" && stopped)
      throw Error("browser_execution_revoked");
    if (input.op === "finalize") {
      stopped = true;
      saves++;
      if (!recover) throw Error("browser_profile_storage_unavailable");
    }
    return request(input);
  };
  try {
    t.mock.timers.tick(100);
    await controller.reconcile();
    assert.equal(saves, 1);
    assert.equal(f.resources.size, 1);
    recover = true;
    t.mock.timers.tick(1000);
    await controller.reconcile();
    assert.equal(saves, 2);
    assert.equal(
      (await controller.command({ op: "controller_status", executionId })).execution
        .phase,
      "finished",
    );
    assert.equal(f.resources.size, 0);
  } finally {
    worker.runtime.request = request;
  }
});

test("concurrent Profile deletion and host reconnect do not deadlock recovered absence", {
  timeout: 5000,
}, async (t) => {
  const f = await fixture(t);
  const executionId = randomUUID(),
    incarnation = randomUUID();
  const worker = await f.backend.create({
    executionId,
    incarnation,
    profileId: "absent-host",
  });
  const journal = await createExecutionJournal({
    directory: f.directory,
    installationId: "test",
  });
  await journal.create(executionId, {
    phase: "finalizing",
    incarnation,
    profileId: "absent-host",
    storageMode: "host",
    allowedDomains: [],
    registeredAt: Date.now(),
    lastActivityAt: Date.now(),
    grants: [],
    workerId: worker.id,
  });
  const controller = await f.start({
    reconciliationMs: 60000,
    profileStore: {
      async delete() {
        return true;
      },
    },
  });
  f.resources.clear();
  const entered = Promise.withResolvers(),
    release = Promise.withResolvers();
  f.backend.list = async () => {
    entered.resolve();
    await release.promise;
    return [];
  };
  const deletion = controller.command({
    op: "profile_delete",
    profileId: "absent-host",
  });
  await entered.promise;
  const peer = { signal: new AbortController().signal };
  const reconnect = controller.command(
    { op: "storage_attach", executionId, profileId: "absent-host" },
    peer,
  );
  const outcomes = Promise.allSettled([deletion, reconnect]);
  await yieldTurn();
  release.resolve();
  let timer;
  try {
    const results = await Promise.race([
      outcomes,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(Error("Profile recovery deadlocked")), 1000);
      }),
    ]);
    assert.deepEqual(results[0], { status: "fulfilled", value: { deleted: true } });
    assert.equal(results[1].status, "rejected");
    assert.match(results[1].reason.message, /browser_execution_revoked/);
  } finally {
    clearTimeout(timer);
    await controller.stop({ finalize: false });
  }
});

test("an incompatible persisted worker protocol preserves a pending Profile save", async (t) => {
  const f = await fixture(t);
  const executionId = randomUUID(),
    incarnation = randomUUID();
  const worker = await f.backend.create({
    executionId,
    incarnation,
    profileId: "pending-save",
  });
  const journal = await createExecutionJournal({
    directory: f.directory,
    installationId: "test",
  });
  await journal.create(executionId, {
    phase: "finalizing",
    incarnation,
    profileId: "pending-save",
    storageMode: "volume",
    allowedDomains: [],
    registeredAt: Date.now(),
    lastActivityAt: Date.now(),
    grants: [],
    workerId: worker.id,
    workerProtocol: "browser-sandbox.worker/v2",
  });
  const original = await readFile(join(f.directory, `${executionId}.json`), "utf8");
  const controller = await f.start({
    reconciliationMs: 60000,
    profileStore: { open: () => ({ close: async () => {} }) },
  });
  await controller.reconcile();
  const status = await controller.command({ op: "controller_status", executionId });
  assert.equal(status.execution.phase, "recovery-blocked");
  assert.equal(status.execution.statusError, "browser_worker_protocol_unsupported");
  assert.equal(f.resources.size, 1);
  assert.equal(
    await readFile(join(f.directory, `${executionId}.json`), "utf8"),
    original,
  );
  let operations = 0;
  const request = worker.runtime.request;
  worker.runtime.request = async (...args) => {
    operations++;
    return request(...args);
  };
  const list = f.backend.list;
  f.backend.list = async (...args) => {
    operations++;
    return list(...args);
  };
  try {
    await assert.rejects(
      controller.command({ op: "finish", executionId }),
      /browser_worker_protocol_unsupported/,
    );
    assert.equal(operations, 0);
    assert.equal(f.resources.size, 1);
    assert.equal(
      await readFile(join(f.directory, `${executionId}.json`), "utf8"),
      original,
    );
  } finally {
    f.backend.list = list;
    worker.runtime.request = request;
    await controller.stop({ finalize: false });
  }
});

test("finish recovers after journal directory sync fails following a published update", async (t) => {
  const f = await fixture(t);
  const controller = await f.start({ reconciliationMs: 60000 });
  const executionId = randomUUID();
  const endpoint = await controller.command({ op: "register", executionId });
  await fetch(fileUrl(controller, endpoint));
  const filename = join(f.directory, `${executionId}.json`);
  const before = JSON.parse(await readFile(filename, "utf8"));
  const directory = await open(f.directory, "r");
  const prototype = Object.getPrototypeOf(directory);
  await directory.close();
  const sync = prototype.sync;
  let inject = true;
  const mock = t.mock.method(prototype, "sync", async function () {
    if (inject && (await this.stat()).isDirectory()) {
      inject = false;
      throw Object.assign(Error("Injected directory sync failure"), { code: "EIO" });
    }
    return sync.call(this);
  });
  try {
    await assert.rejects(controller.command({ op: "endpoint", executionId }), {
      code: "EIO",
    });
    assert.equal(
      JSON.parse(await readFile(filename, "utf8")).revision,
      before.revision + 1,
    );
    assert.deepEqual(
      await controller.command({ op: "endpoint", executionId }),
      endpoint,
    );
    await controller.command({ op: "finish", executionId });
    assert.equal(controller.capacity().workers, 0);
    assert.equal(f.resources.size, 0);
  } finally {
    mock.mock.restore();
    await controller.stop({ finalize: false }).catch(() => {});
  }
});

for (const failure of ["browser_docker_unavailable", "browser_kubernetes_api_failed"]) {
  test(`temporary recovery survives a simulated ${failure} outage`, async (t) => {
    t.mock.timers.enable({ apis: ["Date"], now: 1000 });
    const f = await fixture(t);
    const first = await f.start({ reconciliationMs: 60000 });
    const executionId = randomUUID();
    const endpoint = await first.command({ op: "register", executionId });
    await fetch(fileUrl(first, endpoint));
    await first.stop();
    const worker = [...f.resources.values()][0];
    const request = worker.runtime.request;
    worker.runtime.request = async () => {
      throw Error("browser_control_disconnected");
    };
    const second = await f.start({ reconciliationMs: 60000 });
    const list = f.backend.list;
    let calls = 0;
    f.backend.list = async () => {
      calls++;
      throw Error(failure);
    };
    try {
      for (const elapsed of [0, 1000, 2000]) {
        t.mock.timers.tick(elapsed);
        await second.reconcile();
      }
      const status = await second.command({ op: "controller_status", executionId });
      assert.notEqual(status.execution.phase, "recovery-blocked");
      assert.equal(status.execution.recoveryAttempts, 3);
      assert.equal(calls, 3);
      for (let tick = 0; tick < 10; tick++) await second.reconcile();
      assert.equal(calls, 3, "Reconciliation ticks must respect the retry deadline");
      const record = await readFile(join(f.directory, `${executionId}.json`), "utf8");
      t.mock.timers.tick(4000);
      await second.reconcile();
      assert.equal(calls, 4);
      assert.equal(
        await readFile(join(f.directory, `${executionId}.json`), "utf8"),
        record,
        "A saturated attempt count must not rewrite the journal",
      );
      f.backend.list = list;
      worker.runtime.request = request;
      t.mock.timers.tick(4000);
      await second.reconcile();
      assert.equal(f.resources.size, 0);
      assert.equal(second.capacity().workers, 0);
      assert.equal(
        (await second.command({ op: "controller_status", executionId })).execution
          .phase,
        "finished",
      );
    } finally {
      f.backend.list = list;
      worker.runtime.request = request;
    }
  });
}

test("failed idle Profile save revokes access and retries finalization before maximum lifetime", async (t) => {
  t.mock.timers.enable({ apis: ["Date"], now: 1000 });
  const f = await fixture(t);
  const controller = await f.start({
    profileStore: { open: () => ({ close: async () => {} }) },
    idleRegistrationMs: 100,
    maxExecutionMs: 1000000,
    reconciliationMs: 60000,
  });
  const executionId = randomUUID();
  const endpoint = await controller.command({
    op: "register",
    executionId,
    profileId: "idle-save",
  });
  await fetch(fileUrl(controller, endpoint));
  const worker = [...f.resources.values()][0];
  const request = worker.runtime.request;
  let stopped = false,
    recover = false,
    saves = 0;
  worker.runtime.request = async (input) => {
    if (input.op === "worker_status") {
      if (stopped) throw Error("browser_execution_revoked");
      const status = await request(input);
      return { ...status, session: { ...status.session, expiresAt: Date.now() - 1 } };
    }
    if (input.op === "finalize") {
      if (!stopped) assert.equal(input.ifIdle, true);
      stopped = true;
      saves++;
      if (!recover) throw Error("browser_profile_storage_unavailable");
    }
    return request(input);
  };
  try {
    t.mock.timers.tick(100);
    await controller.reconcile();
    assert.equal(
      (await controller.command({ op: "controller_status", executionId })).execution
        .phase,
      "finalizing",
    );
    assert.equal((await fetch(fileUrl(controller, endpoint))).status, 403);
    assert.equal(controller.capacity().workers, 1);
    recover = true;
    t.mock.timers.tick(1000);
    await controller.reconcile();
    assert.equal(saves, 2);
    assert.equal(controller.capacity().workers, 0);
    assert.equal(f.resources.size, 0);
    await controller.command({
      op: "register",
      executionId: randomUUID(),
      profileId: "idle-save",
    });
  } finally {
    worker.runtime.request = request;
  }
});

test("one reconciliation discovery snapshot serves all revoked executions", async (t) => {
  const f = await fixture(t);
  const first = await f.start({ reconciliationMs: 60000 });
  for (let index = 0; index < 3; index++) {
    const endpoint = await first.command({ op: "register", executionId: randomUUID() });
    await fetch(fileUrl(first, endpoint));
  }
  await first.stop();
  const requests = [...f.resources.values()].map((worker) => [
    worker,
    worker.runtime.request,
  ]);
  for (const [worker] of requests)
    worker.runtime.request = async () => {
      throw Error("browser_control_disconnected");
    };
  const second = await f.start({ reconciliationMs: 60000 });
  for (const [worker, request] of requests) worker.runtime.request = request;
  const list = f.backend.list;
  let calls = 0;
  f.backend.list = async (...args) => {
    calls++;
    return list(...args);
  };
  try {
    await second.reconcile();
    assert.equal(calls, 1);
    assert.equal(f.resources.size, 0);
    assert.equal(second.capacity().workers, 0);
  } finally {
    f.backend.list = list;
  }
});

test("a long CDP tunnel receives a fresh idle window when it disconnects", async (t) => {
  t.mock.timers.enable({ apis: ["Date"], now: 1000 });
  const f = await fixture(t);
  const sockets = new WebSocketServer({ server: f.worker });
  t.after(async () => {
    for (const socket of sockets.clients) socket.terminate();
    await new Promise((resolve) => sockets.close(resolve));
  });
  const controller = await f.start({
    idleRegistrationMs: 100,
    reconciliationMs: 60000,
  });
  const executionId = randomUUID();
  const endpoint = await controller.command({ op: "register", executionId });
  const accepted = once(sockets, "connection");
  const client = new WebSocket(endpoint.wsEndpoint);
  t.after(() => client.terminate());
  const ready = once(client, "message");
  const [upstream] = await accepted;
  upstream.send("ready");
  await ready;
  const worker = [...f.resources.values()][0];
  const request = worker.runtime.request;
  worker.runtime.request = async (input) => {
    if (input.op === "worker_status")
      return {
        protocol: "browser-sandbox.worker/v1",
        session: {
          state: "live",
          generation: "original",
          connectedClients: sockets.clients.size,
        },
        takeover: "automation",
        pendingAuth: 0,
      };
    return request(input);
  };
  t.mock.timers.tick(150);
  await controller.reconcile();
  assert.equal(f.resources.size, 1);
  const closed = Promise.all([once(client, "close"), once(upstream, "close")]);
  client.close();
  await closed;
  await controller.reconcile();
  assert.equal(f.resources.size, 1, "Disconnect must start a new idle window");
  t.mock.timers.tick(99);
  await controller.reconcile();
  assert.equal(f.resources.size, 1);
  t.mock.timers.tick(1);
  await controller.reconcile();
  assert.equal(f.resources.size, 0);
});

test("a seeded legacy blocked temporary binding resumes automatic recovery", async (t) => {
  t.mock.timers.enable({ apis: ["Date"], now: 1000 });
  const f = await fixture(t);
  const first = await f.start({ reconciliationMs: 60000 });
  const executionId = randomUUID();
  const endpoint = await first.command({ op: "register", executionId });
  await fetch(fileUrl(first, endpoint));
  await first.stop();
  const filename = join(f.directory, `${executionId}.json`);
  const record = JSON.parse(await readFile(filename, "utf8"));
  record.data.phase = "recovery-blocked";
  record.data.recoveryAttempts = 3;
  record.data.recoveryError = "browser_docker_unavailable";
  await writeFile(filename, JSON.stringify(record));
  const second = await f.start({ reconciliationMs: 60000 });
  const list = f.backend.list;
  f.backend.list = async () => {
    throw Error("browser_docker_unavailable");
  };
  try {
    await second.reconcile();
    assert.notEqual(
      (await second.command({ op: "controller_status", executionId })).execution.phase,
      "recovery-blocked",
    );
    f.backend.list = list;
    t.mock.timers.tick(4000);
    await second.reconcile();
    assert.equal(second.capacity().workers, 0);
    assert.equal(f.resources.size, 0);
  } finally {
    f.backend.list = list;
  }
});

test("confirmed Profile finalization keeps retrying temporary cleanup failures without saving again", async (t) => {
  t.mock.timers.enable({ apis: ["Date"], now: 1000 });
  const f = await fixture(t);
  const controller = await f.start({
    profileStore: { open: () => ({ close: async () => {} }) },
    reconciliationMs: 60000,
  });
  const executionId = randomUUID();
  const endpoint = await controller.command({
    op: "register",
    executionId,
    profileId: "saved-profile",
  });
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
    throw Error("browser_kubernetes_api_failed");
  };
  try {
    await assert.rejects(
      controller.command({ op: "finish", executionId }),
      /browser_kubernetes_api_failed/,
    );
    for (const elapsed of [0, 1000, 2000]) {
      t.mock.timers.tick(elapsed);
      await controller.reconcile();
    }
    assert.equal(
      (await controller.command({ op: "controller_status", executionId })).execution
        .phase,
      "cleanup-pending",
    );
    assert.equal(finalizations, 1);
    assert.equal(controller.capacity().workers, 1);
    await assert.rejects(
      controller.command({
        op: "register",
        executionId: randomUUID(),
        profileId: "saved-profile",
      }),
      /browser_profile_busy/,
    );
    f.backend.remove = remove;
    t.mock.timers.tick(4000);
    await controller.reconcile();
    assert.equal(finalizations, 1);
    assert.equal(f.resources.size, 0);
    assert.equal(controller.capacity().workers, 0);
    await controller.command({
      op: "register",
      executionId: randomUUID(),
      profileId: "saved-profile",
    });
  } finally {
    f.backend.remove = remove;
    worker.runtime.request = request;
  }
});

test("journal failure recovery preserves a mismatched published record", async (t) => {
  const f = await fixture(t);
  const controller = await f.start({ reconciliationMs: 60000 });
  const executionId = randomUUID();
  await controller.command({ op: "register", executionId });
  const filename = join(f.directory, `${executionId}.json`);
  const directory = await open(f.directory, "r");
  const prototype = Object.getPrototypeOf(directory);
  await directory.close();
  const sync = prototype.sync;
  let inject = true;
  const mock = t.mock.method(prototype, "sync", async function () {
    if (inject && (await this.stat()).isDirectory()) {
      inject = false;
      const record = JSON.parse(await readFile(filename, "utf8"));
      record.data.grants = [];
      await writeFile(filename, JSON.stringify(record));
      throw Object.assign(Error("Injected changed publication"), { code: "EIO" });
    }
    return sync.call(this);
  });
  try {
    await assert.rejects(controller.command({ op: "endpoint", executionId }), {
      code: "EIO",
    });
    const unknown = await readFile(filename, "utf8");
    await assert.rejects(
      controller.command({ op: "endpoint", executionId }),
      /browser_execution_record_changed/,
    );
    assert.equal(await readFile(filename, "utf8"), unknown);
  } finally {
    mock.mock.restore();
    await controller.stop({ finalize: false }).catch(() => {});
  }
});

test("a shared snapshot cannot confirm absence for a worker created later in the pass", {
  timeout: 5000,
}, async (t) => {
  const f = await fixture(t);
  const profileStore = { open: () => ({ close: async () => {} }) };
  const first = await f.start({ profileStore, reconciliationMs: 60000 });
  const idA = randomUUID(),
    idB = randomUUID();
  const endpointA = await first.command({
    op: "register",
    executionId: idA,
    profileId: "snapshot-a",
  });
  await fetch(fileUrl(first, endpointA));
  await first.stop();
  const workerA = [...f.resources.values()][0];
  const requestA = workerA.runtime.request;
  let startup = true,
    entered = false,
    recoverB = false;
  const held = Promise.withResolvers(),
    release = Promise.withResolvers();
  workerA.runtime.request = async (input) => {
    if (input.op === "worker_status" && startup)
      throw Error("browser_control_disconnected");
    if (input.op === "finalize") {
      entered = true;
      held.resolve();
      await release.promise;
    }
    return requestA(input);
  };
  let pass;
  try {
    const second = await f.start({ profileStore, reconciliationMs: 60000 });
    startup = false;
    const endpointB = await second.command({
      op: "register",
      executionId: idB,
      profileId: "snapshot-b",
    });
    pass = second.reconcile();
    await Promise.race([held.promise, pass]);
    assert(entered, "The first execution must hold the pass after discovery");
    await fetch(fileUrl(second, endpointB));
    const workerB = [...f.resources.values()].find(
      (worker) => worker.executionId === idB,
    );
    const requestB = workerB.runtime.request;
    let savesB = 0;
    workerB.runtime.request = async (input) => {
      if (input.op === "finalize") {
        savesB++;
        if (!recoverB) throw Error("browser_profile_storage_unavailable");
      }
      return requestB(input);
    };
    await assert.rejects(
      second.command({ op: "finish", executionId: idB }),
      /browser_profile_storage_unavailable/,
    );
    release.resolve();
    await pass;
    assert.equal(
      (await second.command({ op: "controller_status", executionId: idB })).execution
        .phase,
      "finalizing",
    );
    assert.equal(savesB, 1);
    assert.equal(second.capacity().workers, 1);
    assert.equal(f.resources.size, 1);
    await assert.rejects(
      second.command({
        op: "register",
        executionId: randomUUID(),
        profileId: "snapshot-b",
      }),
      /browser_profile_busy/,
    );
    recoverB = true;
    await second.reconcile();
    assert.equal(savesB, 2);
    assert.equal(f.resources.size, 0);
    assert.equal(second.capacity().workers, 0);
  } finally {
    recoverB = true;
    release.resolve();
    await pass?.catch(() => {});
    workerA.runtime.request = requestA;
  }
});

test("temporary protocol skew does not fence capacity after authoritative absence", async (t) => {
  t.mock.timers.enable({ apis: ["Date"], now: 1000 });
  const f = await fixture(t);
  const first = await f.start({ reconciliationMs: 60000 });
  const executionId = randomUUID();
  const endpoint = await first.command({ op: "register", executionId });
  await fetch(fileUrl(first, endpoint));
  await first.stop();
  const worker = [...f.resources.values()][0];
  const request = worker.runtime.request;
  let operations = 0;
  worker.runtime.request = async (input) => {
    operations++;
    if (input.op === "worker_status") return { protocol: "browser-sandbox.worker/v2" };
    throw Error("An incompatible worker must not receive commands");
  };
  const second = await f.start({ reconciliationMs: 60000 });
  operations = 0;
  try {
    await assert.rejects(
      second.command({ op: "finish", executionId }),
      /browser_worker_protocol_unsupported/,
    );
    await second.reconcile();
    assert.equal(second.capacity().workers, 1);
    f.resources.clear();
    t.mock.timers.tick(1000);
    await second.reconcile();
    assert.equal(
      (await second.command({ op: "controller_status", executionId })).execution.phase,
      "finished",
    );
    assert.equal(second.capacity().workers, 0);
    assert.equal(operations, 0);
  } finally {
    worker.runtime.request = request;
    await second.stop({ finalize: false }).catch(() => {});
  }
});

test("temporary backends without discovery still finalize a known revoked worker", async (t) => {
  const f = await fixture(t);
  const controller = await f.start({
    stateDirectory: undefined,
    backend: { create: f.backend.create, remove: f.backend.remove },
    reconciliationMs: 60000,
  });
  const executionId = randomUUID();
  const endpoint = await controller.command({ op: "register", executionId });
  await fetch(fileUrl(controller, endpoint));
  await controller.command({ op: "session", executionId, action: "status" });
  const worker = [...f.resources.values()][0];
  const request = worker.runtime.request;
  worker.runtime.request = async (input) => {
    if (input.op === "worker_status") {
      const status = await request(input);
      return { ...status, session: { ...status.session, state: "lost" } };
    }
    return request(input);
  };
  await assert.rejects(
    controller.command({
      op: "session",
      executionId,
      action: "renew",
      durationMs: 1000,
    }),
    /browser_session_lost/,
  );
  worker.runtime.request = request;
  await controller.reconcile();
  assert.equal(f.resources.size, 0);
  assert.equal(controller.capacity().workers, 0);
});

test("finish recovers the previous published revision across consecutive read failures", async (t) => {
  const f = await fixture(t);
  const controller = await f.start({ reconciliationMs: 60000 });
  const executionId = randomUUID();
  const endpoint = await controller.command({ op: "register", executionId });
  await fetch(fileUrl(controller, endpoint));
  const directory = await open(f.directory, "r");
  const prototype = Object.getPrototypeOf(directory);
  await directory.close();
  const sync = prototype.sync,
    read = prototype.readFile;
  let inject = true,
    failRead = 0;
  const syncMock = t.mock.method(prototype, "sync", async function () {
    if (inject && (await this.stat()).isDirectory()) {
      inject = false;
      failRead = 3;
      throw Object.assign(Error("Injected published write failure"), { code: "EIO" });
    }
    return sync.call(this);
  });
  const readMock = t.mock.method(prototype, "readFile", async function (...args) {
    if (failRead) {
      failRead--;
      throw Object.assign(Error("Injected recovery read failure"), { code: "EIO" });
    }
    return read.apply(this, args);
  });
  try {
    await assert.rejects(controller.command({ op: "endpoint", executionId }), {
      code: "EIO",
    });
    await assert.rejects(controller.command({ op: "access_revoke", executionId }), {
      code: "EIO",
    });
    await assert.rejects(controller.command({ op: "finish", executionId }), {
      code: "EIO",
    });
    await controller.command({ op: "finish", executionId });
    assert.equal(controller.capacity().workers, 0);
    assert.equal(f.resources.size, 0);
  } finally {
    syncMock.mock.restore();
    readMock.mock.restore();
    await controller.stop({ finalize: false }).catch(() => {});
  }
});

for (const failure of [null, Error("browser_profile_save_failed")]) {
  test(`idle finalization preserves ${failure === null ? "a non-Error rejection" : "its error when journaling fails"}`, async (t) => {
    const f = await fixture(t);
    const controller = await f.start({ reconciliationMs: 60000 });
    const executionId = randomUUID();
    const endpoint = await controller.command({ op: "register", executionId });
    await fetch(fileUrl(controller, endpoint));
    const worker = [...f.resources.values()][0];
    const request = worker.runtime.request;
    worker.runtime.request = async (input) => {
      if (input.op === "finalize" && input.ifIdle) throw failure;
      return request(input);
    };
    const directory = await open(f.directory, "r");
    const prototype = Object.getPrototypeOf(directory);
    await directory.close();
    const sync = prototype.sync;
    const mock = t.mock.method(prototype, "sync", async function () {
      if (failure !== null && (await this.stat()).isDirectory())
        throw Object.assign(Error("Injected idle journal failure"), { code: "EIO" });
      return sync.call(this);
    });
    try {
      await assert.rejects(
        controller.command({ op: "finalize", executionId, ifIdle: true }),
        (error) => error === failure,
      );
      assert.equal((await fetch(fileUrl(controller, endpoint))).status, 403);
    } finally {
      mock.mock.restore();
      worker.runtime.request = request;
    }
  });
}

test("a blocked persistent finalization accepts an absent worker after confirmed commit", async (t) => {
  t.mock.timers.enable({ apis: ["Date"], now: 1000 });
  const f = await fixture(t);
  let adapter,
    finalizes = 0,
    commits = 0;
  const create = f.backend.create;
  const result = {
    used: true,
    archive: { sha256: "a".repeat(64), byteSize: 1 },
    previousSha256: null,
  };
  f.backend.create = async (...args) => {
    const worker = await create(...args);
    const request = worker.runtime.request;
    worker.runtime.attachStorage = async (_registration, value) => {
      adapter = value;
      return { close() {} };
    };
    worker.runtime.request = async (input) => {
      if (input.op !== "finalize") return request(input);
      finalizes++;
      if (finalizes === 1) await adapter.commit(result);
      if (finalizes <= 4) throw Error("browser_control_disconnected");
      return { status: "absent" };
    };
    return worker;
  };
  // Synthetic commit confirmation isolates blocked recovery from archive transport.
  const profileStore = {
    open: () => ({
      commit: async () => {
        commits++;
        return { confirmedSha: result.archive.sha256 };
      },
      close: async () => {},
    }),
  };
  const controller = await f.start({ profileStore, reconciliationMs: 60000 });
  const executionId = randomUUID();
  const endpoint = await controller.command({
    op: "register",
    executionId,
    profileId: "blocked-commit",
  });
  await fetch(fileUrl(controller, endpoint));
  try {
    await assert.rejects(
      controller.command({ op: "finish", executionId }),
      /browser_control_disconnected/,
    );
    for (const delay of [0, 1000, 2000]) {
      t.mock.timers.tick(delay);
      await controller.reconcile();
    }
    const status = await controller.command({ op: "controller_status", executionId });
    assert.equal(status.execution.phase, "recovery-blocked");
    assert.equal(controller.capacity().workers, 1);
    await assert.rejects(
      controller.command({
        op: "register",
        executionId: randomUUID(),
        profileId: "blocked-commit",
      }),
      /browser_profile_busy/,
    );
    const saved = await controller.command({ op: "finish", executionId });
    assert.equal(saved.stopped, true);
    assert.equal(saved.archive.sha256, result.archive.sha256);
    assert.equal(commits, 1);
    assert.equal(f.resources.size, 0);
    assert.equal(controller.capacity().workers, 0);
    await controller.command({
      op: "register",
      executionId: randomUUID(),
      profileId: "blocked-commit",
    });
  } finally {
    for (const worker of f.resources.values())
      worker.runtime.request = async () => ({ stopped: true });
  }
});

test("HTTP renewal preserves detected session loss while revoking the old endpoint", async (t) => {
  const f = await fixture(t);
  const controller = await f.start({ reconciliationMs: 60000 });
  const executionId = randomUUID();
  const endpoint = await controller.command({ op: "register", executionId });
  await fetch(fileUrl(controller, endpoint));
  const sessionUrl = `${controller.origin + new URL(endpoint.wsEndpoint).pathname}/session`;
  assert.equal((await (await fetch(sessionUrl)).json()).generation, "original");
  const worker = [...f.resources.values()][0];
  const request = worker.runtime.request;
  worker.runtime.request = async (input) => {
    const result = await request(input);
    if (input.op === "worker_status")
      result.session = { ...result.session, state: "expired" };
    return result;
  };
  try {
    const response = await fetch(sessionUrl, {
      method: "POST",
      body: JSON.stringify({ action: "renew", durationMs: 10000 }),
    });
    assert.deepEqual(await response.json(), { error: "browser_session_lost" });
    assert.equal(response.status, 502);
    assert.equal((await fetch(sessionUrl)).status, 403);
    assert.equal(f.created, 1);
  } finally {
    worker.runtime.request = request;
  }
});

test("confirmed commit records reject corrupt witnesses without changing recoverable data", async (t) => {
  const changes = [
    (commit) => {
      commit.incarnation = randomUUID();
    },
    (commit) => {
      commit.result.archive.sha256 = ["a".repeat(64)];
    },
    (commit) => {
      commit.result.archive.byteSize = 0;
    },
    (commit) => {
      commit.result.used = "true";
    },
    (commit) => {
      commit.result.archive.url = "https://fixture.invalid/archive";
    },
  ];
  for (const change of changes) {
    const f = await fixture(t);
    const profileStore = { open: () => ({ close: async () => {} }) };
    const first = await f.start({ profileStore, reconciliationMs: 60000 });
    const executionId = randomUUID();
    const endpoint = await first.command({
      op: "register",
      executionId,
      profileId: "witness",
    });
    await fetch(fileUrl(first, endpoint));
    await first.stop();
    const filename = join(f.directory, `${executionId}.json`);
    const record = JSON.parse(await readFile(filename, "utf8"));
    record.data.confirmedCommit = {
      incarnation: record.data.incarnation,
      result: { used: true, archive: { sha256: "a".repeat(64), byteSize: 1 } },
    };
    change(record.data.confirmedCommit);
    const corrupt = JSON.stringify(record);
    await writeFile(filename, corrupt);
    await assert.rejects(
      f.start({ profileStore }),
      /browser_execution_journal_corrupt/,
    );
    assert.equal(await readFile(filename, "utf8"), corrupt);
    assert.equal(f.resources.size, 1);
  }
});

test("a journal failure prevents acknowledging a confirmed storage commit to the worker", async (t) => {
  const f = await fixture(t);
  let storageCommitted = false,
    acknowledged = false,
    adapter;
  // Synthetic archive metadata isolates the controller's durability boundary.
  const result = { used: true, archive: { sha256: "b".repeat(64), byteSize: 1 } };
  const profileStore = {
    open: () => ({
      commit: async () => {
        storageCommitted = true;
        return { confirmedSha: result.archive.sha256 };
      },
      close: async () => {},
    }),
  };
  const create = f.backend.create;
  f.backend.create = async (...args) => {
    const worker = await create(...args);
    worker.runtime.attachStorage = async (_registration, value) => {
      adapter = value;
      return { close() {} };
    };
    return worker;
  };
  const controller = await f.start({ profileStore, reconciliationMs: 60000 });
  const executionId = randomUUID();
  const endpoint = await controller.command({
    op: "register",
    executionId,
    profileId: "ack-boundary",
  });
  await fetch(fileUrl(controller, endpoint));
  const worker = [...f.resources.values()][0],
    request = worker.runtime.request;
  worker.runtime.request = async (input) => {
    if (input.op !== "finalize") return request(input);
    await adapter.commit(result);
    acknowledged = true;
    return { stopped: true };
  };
  const folder = await open(f.directory, "r");
  const inode = (await folder.stat()).ino;
  const prototype = Object.getPrototypeOf(folder),
    sync = prototype.sync;
  await folder.close();
  const mock = t.mock.method(prototype, "sync", async function () {
    if (storageCommitted && (await this.stat()).ino === inode)
      throw Object.assign(Error("Injected commit journal failure"), { code: "EIO" });
    return sync.call(this);
  });
  try {
    await assert.rejects(controller.command({ op: "finish", executionId }), {
      code: "EIO",
    });
    assert.equal(storageCommitted, true);
    assert.equal(acknowledged, false);
    assert.equal(controller.capacity().workers, 1);
    await assert.rejects(
      controller.command({
        op: "register",
        executionId: randomUUID(),
        profileId: "ack-boundary",
      }),
      /browser_profile_busy/,
    );
  } finally {
    mock.mock.restore();
    worker.runtime.request = request;
  }
});
