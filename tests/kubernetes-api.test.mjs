import assert from "node:assert/strict";
import test from "node:test";
import {
  createKubernetesClient,
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
