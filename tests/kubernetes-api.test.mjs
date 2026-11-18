import assert from "node:assert/strict";
import test from "node:test";
import {
  createKubernetesClient,
  listKubernetesPages,
  readKubernetesAccess,
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
