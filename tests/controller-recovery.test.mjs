import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
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
  assert.equal(controller.bindings.get(executionId).revoked, true);
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
