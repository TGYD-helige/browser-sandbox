import assert from "node:assert/strict";
import { createServer } from "node:http";
import test from "node:test";
import {
  createKubernetesClient,
  kubernetesOwnerReference,
  listKubernetesPages,
  readKubernetesAccess,
  workerOwnerCheck,
} from "../dist/kubernetes-api.js";

test("missing in-cluster API host fails clearly before reading credentials or connecting", async (t) => {
  const saved = process.env.KUBERNETES_SERVICE_HOST;
  delete process.env.KUBERNETES_SERVICE_HOST;
  t.after(() => {
    if (saved === undefined) delete process.env.KUBERNETES_SERVICE_HOST;
    else process.env.KUBERNETES_SERVICE_HOST = saved;
  });
  assert.throws(
    () => createKubernetesClient({ namespace: "test" }),
    /browser_kubernetes_api_host_required/,
  );
  await assert.rejects(readKubernetesAccess(), /browser_kubernetes_api_host_required/);
  assert.equal(
    typeof createKubernetesClient({
      namespace: "test",
      apiServer: "http://127.0.0.1:1234",
    }),
    "function",
  );
});

test("empty Kubernetes pages with continuations count towards the page limit", async () => {
  let requests = 0;
  await assert.rejects(async () => {
    for await (const _page of listKubernetesPages(
      async () => ({ items: [], metadata: { continue: String(++requests) } }),
      "/leases",
      "execution",
      AbortSignal.timeout(5000),
      300,
      "too_many_pages",
    )) {
    }
  }, /too_many_pages/);
  assert.equal(requests, 4);
});

test("worker owner checks read a supplied Lease cache without listing Leases", async () => {
  const id = "11111111-1111-4111-8111-111111111111";
  const parent = kubernetesOwnerReference("ConfigMap", "workers", "release-uid");
  const lease = {
    uid: "lease-uid",
    labels: {
      "browser-sandbox.io/state": "execution",
      "browser-sandbox.io/execution": id,
    },
    ownerReferences: [parent],
  };
  const check = workerOwnerCheck(
    () => assert.fail("a cached check must not call the API"),
    { namespace: "test", installation: "test" },
    parent,
    AbortSignal.timeout(1000),
    undefined,
    (execution) => (execution === id ? lease : undefined),
  );
  const pod = (uid, execution = id) => ({
    metadata: {
      labels: { "browser-sandbox.io/execution": execution },
      ownerReferences: [kubernetesOwnerReference("Lease", "bs-lease", uid)],
    },
  });
  assert.equal(await check(pod("lease-uid")), true);
  assert.equal(await check(pod("replaced-uid")), false);
  assert.equal(
    await check(pod("lease-uid", "22222222-2222-4222-8222-222222222222")),
    false,
  );
});

test("a watch event the caller rejects fails the request instead of resolving or escaping", async (t) => {
  // The complete stream arrives before the handler throws, as with a final ERROR event.
  const server = createServer((_req, res) =>
    res.writeHead(200).end('{"type":"ERROR"}\n'),
  );
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(() => new Promise((resolve) => server.close(resolve)));
  const api = createKubernetesClient({
    namespace: "test",
    apiServer: `http://127.0.0.1:${server.address().port}`,
  });
  const escaped = [];
  const capture = (error) => escaped.push(error);
  process.on("uncaughtException", capture);
  t.after(() => process.off("uncaughtException", capture));
  await assert.rejects(
    api("/watch?watch=true", "GET", undefined, AbortSignal.timeout(5000), () => {
      throw Error("rejected");
    }),
    /browser_kubernetes_response_invalid/,
  );
  await new Promise(setImmediate);
  assert.deepEqual(escaped, []);
});
