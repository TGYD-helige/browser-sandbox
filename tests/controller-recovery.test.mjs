import assert from "node:assert/strict";
import { randomBytes, randomUUID } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { setImmediate as yieldTurn } from "node:timers/promises";
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
  await second.reconcile();
  assert.equal(f.resources.size, 1);
  await second.reconcile();
  assert.equal(f.resources.size, 0);
  assert.equal(second.capacity().workers, 0);
  assert.equal(
    (await second.command({ op: "controller_status", executionId })).execution.phase,
    "finished",
  );
});

test("unconfirmed persistent recovery becomes visible and retains its lease until explicit finish", async (t) => {
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
  for (let i = 0; i < 3; i++) await second.reconcile();
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
  await second.reconcile();
  assert.equal(
    (await second.command({ op: "controller_status", executionId })).execution.phase,
    "recovery-blocked",
  );
  await second.stop();
  worker.runtime.request = request;
  const third = await f.start({ reconciliationMs: 60000 });
  assert.equal(await (await fetch(fileUrl(third, endpoint))).text(), "same-worker");
  assert.equal(f.created, 1);
});
