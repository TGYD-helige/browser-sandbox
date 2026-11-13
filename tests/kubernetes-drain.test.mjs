import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer } from "node:http";
import test from "node:test";
import { drainKubernetesWorkers } from "../dist/kubernetes-drain.js";

async function fixture(t) {
  let controllerRead, deletionRead;
  const controllerObserved = new Promise((resolve) => {
    controllerRead = resolve;
  });
  const deletionObserved = new Promise((resolve) => {
    deletionRead = resolve;
  });
  const state = {
    scaled: false,
    controllerGone: false,
    workerGone: false,
    deleted: false,
    owner: "release-uid",
    workerReads: 0,
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
    } else if (request.url.endsWith("/scale")) {
      if (request.method === "PATCH") {
        assert.equal(body.spec.replicas, 0);
        state.scaled = true;
      }
      response.end(
        JSON.stringify({
          metadata: { name: "trial-browser", resourceVersion: "1" },
          spec: { replicas: state.scaled ? 0 : 1 },
        }),
      );
    } else if (request.method === "DELETE") {
      assert(state.controllerGone, "Worker deletion preceded controller exit");
      assert.deepEqual(body.preconditions, { uid: "worker-uid" });
      state.deleted = true;
      response.end("{}");
      deletionRead();
    } else if (decodeURIComponent(request.url).includes("component=controller")) {
      assert(state.scaled);
      response.end(
        JSON.stringify({
          items: state.controllerGone ? [] : [{ metadata: { uid: "controller-uid" } }],
        }),
      );
      controllerRead();
    } else {
      state.workerReads++;
      response.end(
        JSON.stringify({
          items: state.workerGone
            ? []
            : [
                {
                  metadata: {
                    name: "owned-worker",
                    uid: "worker-uid",
                    deletionTimestamp: state.deleted ? "now" : undefined,
                    labels: {
                      "app.kubernetes.io/name": "browser-sandbox",
                      "app.kubernetes.io/instance": "trial",
                      "app.kubernetes.io/component": "worker",
                    },
                    ownerReferences: [
                      {
                        apiVersion: "v1",
                        kind: "ConfigMap",
                        name: "trial-browser-workers",
                        uid: state.owner,
                        controller: true,
                      },
                    ],
                  },
                },
              ],
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
  return {
    state,
    controllerObserved,
    deletionObserved,
    options: {
      apiServer: `http://127.0.0.1:${server.address().port}`,
      namespace: "test",
      installation: "trial",
      controllerName: "trial-browser",
      ownerName: "trial-browser-workers",
    },
  };
}

test("uninstall drain stops admission before workers and waits for confirmed deletion", async (t) => {
  const { state, options, controllerObserved, deletionObserved } = await fixture(t);
  let settled = false;
  const draining = drainKubernetesWorkers(options, AbortSignal.timeout(5000)).finally(
    () => {
      settled = true;
    },
  );
  await controllerObserved;
  assert.equal(state.workerReads, 0);
  state.controllerGone = true;
  await deletionObserved;
  assert.equal(settled, false);
  state.workerGone = true;
  await draining;
});

test("uninstall drain refuses a worker owned by another release incarnation", async (t) => {
  const { state, options } = await fixture(t);
  state.controllerGone = true;
  state.owner = "foreign-release-uid";
  await assert.rejects(
    drainKubernetesWorkers(options, AbortSignal.timeout(5000)),
    /browser_worker_ownership_mismatch/,
  );
  assert.equal(state.deleted, false);
});

test("unconfirmed worker deletion times out instead of completing uninstall", async (t) => {
  const { state, options } = await fixture(t);
  state.controllerGone = true;
  await assert.rejects(
    drainKubernetesWorkers(options, AbortSignal.timeout(250)),
    /browser_kubernetes_drain_timeout/,
  );
  assert.equal(state.deleted, true);
  assert.equal(state.workerGone, false);
});
