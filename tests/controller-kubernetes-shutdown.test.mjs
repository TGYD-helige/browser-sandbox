import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { createController } from "../dist/controller.js";

test("Kubernetes rollout disconnects clients while preserving an allocated worker", async () => {
  let removed = 0;
  let finalized = 0;
  let closed = false;
  const controller = await createController({
    port: 0,
    backend: {
      preserveWorkersOnShutdown: true,
      async create(placement) {
        return {
          ...placement,
          id: "pod/uid",
          origin: "http://127.0.0.1:1",
          runtime: {
            async request(request) {
              if (request.op === "finalize") finalized++;
              return {
                cdpUrl: `ws://127.0.0.1:1/execution/${"a".repeat(64)}`,
                viewerToken: "b".repeat(64),
              };
            },
            close() {
              closed = true;
            },
          },
        };
      },
      async remove() {
        removed++;
      },
    },
  });
  try {
    const executionId = randomUUID();
    const endpoint = await controller.command({ op: "register", executionId });
    await fetch(endpoint.wsEndpoint.replace("ws:", "http:") + "/files/test");
  } finally {
    await controller.stop();
  }
  assert.equal(removed, 0);
  assert.equal(finalized, 0);
  assert.equal(closed, true);
});

for (const code of [
  "browser_worker_cleanup_unconfirmed",
  "browser_kubernetes_unavailable",
])
  test(`rollout shutdown reports failed creation cleanup: ${code}`, async () => {
    let acquired;
    const started = new Promise((resolve) => {
      acquired = resolve;
    });
    const controller = await createController({
      port: 0,
      backend: {
        async create(_placement, signal) {
          acquired();
          await new Promise((resolve) =>
            signal.addEventListener("abort", resolve, { once: true }),
          );
          throw Error(code);
        },
        async remove() {},
      },
    });
    const { wsEndpoint } = await controller.command({
      op: "register",
      executionId: randomUUID(),
    });
    const request = fetch(`${wsEndpoint.replace("ws:", "http:")}/files/test`).catch(
      () => {},
    );
    await started;
    await assert.rejects(controller.stop({ preserveWorkers: true }), new RegExp(code));
    await request;
  });

test("backend-preserving shutdown reports an earlier unconfirmed creation failure", async () => {
  const { WorkerCleanupFailure } = await import("../dist/lifecycle-errors.js");
  const failure = new WorkerCleanupFailure(
    Error("browser_kubernetes_operation_failed"),
    Error("browser_worker_cleanup_unconfirmed"),
  );
  const controller = await createController({
    port: 0,
    backend: {
      preserveWorkersOnShutdown: true,
      async create() {
        throw failure;
      },
      async remove() {},
    },
  });
  const { wsEndpoint } = await controller.command({
    op: "register",
    executionId: randomUUID(),
  });
  await fetch(`${wsEndpoint.replace("ws:", "http:")}/files/test`);
  await assert.rejects(controller.stop(), (error) => error === failure);
});
