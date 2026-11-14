import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer } from "node:http";
import test from "node:test";
import { createKubernetesBackend } from "../dist/kubernetes-backend.js";
import { WorkerCleanupFailure } from "../dist/lifecycle-errors.js";

const executionId = "11111111-1111-4111-8111-111111111111";
const incarnation = "22222222-2222-4222-8222-222222222222";

test("Kubernetes creates an owned single-execution Pod and fences deletion by UID", async (t) => {
  let pod;
  let deleted;
  const server = createServer(async (request, response) => {
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    const body = chunks.length ? JSON.parse(Buffer.concat(chunks)) : undefined;
    response.setHeader("content-type", "application/json");
    if (request.url.endsWith("/configmaps/trial-browser-workers")) {
      response.end(
        JSON.stringify({
          metadata: {
            uid: "release-uid",
            labels: {
              "app.kubernetes.io/name": "browser-sandbox",
              "app.kubernetes.io/instance": "trial",
            },
          },
        }),
      );
    } else if (request.method === "POST") {
      pod = { ...body, metadata: { ...body.metadata, uid: "pod-uid" } };
      response.end(JSON.stringify(pod));
    } else if (request.method === "DELETE") {
      deleted = body;
      response.end("{}");
    } else if (deleted) {
      response.writeHead(404).end("{}");
    } else {
      response.end(
        JSON.stringify({
          ...pod,
          status: {
            phase: "Running",
            podIP: "10.42.1.4",
            conditions: [{ type: "Ready", status: "True" }],
          },
        }),
      );
    }
  });
  t.after(() => {
    server.closeAllConnections();
    return new Promise((resolve) => server.close(resolve));
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const backend = createKubernetesBackend({
    apiServer: `http://127.0.0.1:${server.address().port}`,
    namespace: "test",
    ownerName: "trial-browser-workers",
    installation: "trial",
    image: `amaster/browser-sandbox@sha256:${"a".repeat(64)}`,
    environment: { TIMEOUT: "10000", PRIVATE_SECRET: "never-project" },
    template: {
      securityContext: {
        seccompProfile: { type: "Localhost", localhostProfile: "browser-sandbox.json" },
      },
    },
  });
  const worker = await backend.create(
    { executionId, incarnation },
    AbortSignal.timeout(5000),
  );
  t.after(() => worker.runtime.close());
  assert.equal(worker.origin, "http://10.42.1.4:8081");
  assert.equal(pod.spec.restartPolicy, "Never");
  assert.equal(pod.spec.automountServiceAccountToken, false);
  assert(pod.spec.activeDeadlineSeconds >= 70);
  assert.deepEqual(pod.metadata.ownerReferences, [
    {
      apiVersion: "v1",
      kind: "ConfigMap",
      name: "trial-browser-workers",
      uid: "release-uid",
      controller: true,
      blockOwnerDeletion: false,
    },
  ]);
  const env = Object.fromEntries(
    pod.spec.containers[0].env.map((item) => [item.name, item.value]),
  );
  assert.equal(env.PRIVATE_SECRET, undefined);
  assert.equal(env.TIMEOUT, "10000");
  assert.equal(env.BROWSER_EXECUTION_ID, executionId);
  assert.equal(env.CONCURRENT, "1");
  assert.equal(
    pod.spec.containers[0].image,
    `amaster/browser-sandbox@sha256:${"a".repeat(64)}`,
  );
  await backend.remove(worker, AbortSignal.timeout(5000));
  assert.deepEqual(deleted.preconditions, { uid: "pod-uid" });
});

async function fixture(t, { pending = false, deleteRace = false, createStatus } = {}) {
  const state = {
    pod: undefined,
    deleted: false,
    replacement: false,
    observed: undefined,
    podReads: 0,
  };
  const server = createServer(async (request, response) => {
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    const body = chunks.length ? JSON.parse(Buffer.concat(chunks)) : undefined;
    response.setHeader("content-type", "application/json");
    if (request.url.includes("/configmaps/")) {
      response.end(
        JSON.stringify({
          metadata: {
            uid: "release-uid",
            labels: {
              "app.kubernetes.io/name": "browser-sandbox",
              "app.kubernetes.io/instance": "trial",
            },
          },
        }),
      );
    } else if (request.method === "POST") {
      if (createStatus) {
        response
          .writeHead(createStatus)
          .end(JSON.stringify({ message: "private admission diagnostic" }));
        return;
      }
      state.pod = { ...body, metadata: { ...body.metadata, uid: "original" } };
      response.end(JSON.stringify(state.pod));
    } else if (request.method === "DELETE") {
      state.deleted = true;
      if (deleteRace) response.writeHead(404);
      response.end("{}");
    } else if (state.deleted || !state.pod) {
      state.podReads++;
      response.writeHead(404).end("{}");
    } else {
      state.podReads++;
      response.end(
        JSON.stringify({
          ...state.pod,
          metadata: {
            ...state.pod.metadata,
            uid: state.replacement ? "replacement" : "original",
          },
          status: pending
            ? { phase: "Pending" }
            : {
                phase: "Running",
                podIP: "10.42.1.4",
                conditions: [{ type: "Ready", status: "True" }],
              },
        }),
      );
      state.observed?.();
    }
  });
  t.after(() => {
    server.closeAllConnections();
    return new Promise((resolve) => server.close(resolve));
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const backend = createKubernetesBackend({
    apiServer: `http://127.0.0.1:${server.address().port}`,
    namespace: "test",
    installation: "trial",
    ownerName: "trial-browser-workers",
    image: `amaster/browser-sandbox@sha256:${"a".repeat(64)}`,
    template: {
      terminationGracePeriodSeconds: 1,
      securityContext: {
        seccompProfile: { type: "Localhost", localhostProfile: "browser-sandbox.json" },
      },
    },
  });
  return { backend, state };
}

test("stale Kubernetes cleanup cannot delete a replacement Pod", async (t) => {
  const { backend, state } = await fixture(t);
  const worker = await backend.create(
    { executionId, incarnation },
    AbortSignal.timeout(5000),
  );
  t.after(() => worker.runtime.close());
  state.replacement = true;
  await assert.rejects(
    backend.remove(worker, AbortSignal.timeout(5000)),
    /browser_worker_ownership_mismatch/,
  );
  assert.equal(state.deleted, false);
});

test("cancelling Pending creation deletes its exact partial Pod before returning", async (t) => {
  const { backend, state } = await fixture(t, { pending: true });
  const controller = new AbortController();
  state.observed = () => controller.abort();
  await assert.rejects(
    backend.create({ executionId, incarnation }, controller.signal),
    /browser_worker_creation_cancelled/,
  );
  assert.equal(state.deleted, true);
});

test("cleanup is idempotent when eviction removes the Pod immediately before DELETE", async (t) => {
  const { backend } = await fixture(t, { deleteRace: true });
  const worker = await backend.create(
    { executionId, incarnation },
    AbortSignal.timeout(5000),
  );
  t.after(() => worker.runtime.close());
  await backend.remove(worker, AbortSignal.timeout(5000));
});

for (const status of [400, 401, 403, 404, 409, 422, 429]) {
  test(`rejected Pod creation (${status}) reports the API failure without cleanup`, async (t) => {
    const { backend, state } = await fixture(t, { createStatus: status });
    await assert.rejects(
      backend.create({ executionId, incarnation }, AbortSignal.timeout(5000)),
      (error) => {
        assert.equal(error.message, "browser_kubernetes_operation_failed");
        assert.equal(error.status, status);
        return true;
      },
    );
    assert.equal(state.podReads, 0);
    assert.equal(state.deleted, false);
  });
}

test("an ambiguous Pod create failure retains the unconfirmed-cleanup contract", async (t) => {
  const { backend } = await fixture(t, { createStatus: 500 });
  await assert.rejects(
    backend.create({ executionId, incarnation }, AbortSignal.timeout(5000)),
    (error) => {
      assert(error instanceof WorkerCleanupFailure);
      assert.equal(error.errors[0].message, "browser_kubernetes_operation_failed");
      return true;
    },
  );
});

test("matching labels and Pod UID cannot authorize deletion under a foreign owner UID", async (t) => {
  const { backend, state } = await fixture(t);
  const worker = await backend.create(
    { executionId, incarnation },
    AbortSignal.timeout(5000),
  );
  t.after(() => worker.runtime.close());
  state.pod.metadata.ownerReferences[0].uid = "foreign-owner";
  await assert.rejects(
    backend.remove(worker, AbortSignal.timeout(5000)),
    /browser_worker_ownership_mismatch/,
  );
  assert.equal(state.deleted, false);
});
