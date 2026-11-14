import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { createDockerBackend } from "../dist/docker-backend.js";

async function fixture(t) {
  const directory = await mkdtemp(join(tmpdir(), "docker-recovery-"));
  const executionId = randomUUID(),
    incarnation = randomUUID(),
    id = "a".repeat(64);
  const labels = {
    "browser-sandbox.installation": "unit",
    "browser-sandbox.execution": executionId,
    "browser-sandbox.incarnation": incarnation,
    "browser-sandbox.profile": "",
  };
  let exists = true,
    removed = 0;
  const network = {
    Labels: { ...labels },
    Containers: { controller: {} },
    exists: true,
    failDelete: false,
  };
  const server = createServer((req, res) => {
    res.setHeader("content-type", "application/json");
    const path = req.url.replace("/v1.47", "");
    if (path.startsWith("/containers/json?")) {
      const filters = JSON.parse(
        new URL(req.url, "http://docker").searchParams.get("filters"),
      );
      assert.deepEqual(filters, { label: ["browser-sandbox.installation=unit"] });
      res.end(JSON.stringify(exists ? [{ Id: id }] : []));
    } else if (path.startsWith("/networks?"))
      res.end(
        JSON.stringify(
          network.exists ? [{ Id: "network", Labels: network.Labels }] : [],
        ),
      );
    else if (path === `/containers/${id}/json`) {
      if (!exists) res.writeHead(404).end("{}");
      else
        res.end(
          JSON.stringify({
            Config: {
              Labels: {
                ...labels,
                "browser-sandbox.network": "network",
                "browser-sandbox.origin": `http://bs-unit-${incarnation}:8081`,
              },
            },
          }),
        );
    } else if (path === "/containers/controller/json")
      res.end(JSON.stringify({ Id: "controller" }));
    else if (path === "/networks/network" && req.method === "GET") {
      if (network.exists) res.end(JSON.stringify(network));
      else res.writeHead(404).end("{}");
    } else if (
      path === `/containers/${id}?force=true&v=true` &&
      req.method === "DELETE"
    ) {
      exists = false;
      removed++;
      res.end("{}");
    } else if (path === "/networks/network" && req.method === "DELETE") {
      if (network.failDelete) res.writeHead(500).end("{}");
      else {
        network.exists = false;
        res.end("{}");
      }
    } else if (path === "/networks/network/disconnect") {
      network.Containers = {};
      res.end("{}");
    } else res.writeHead(500).end("{}");
  });
  const socketPath = join(directory, "docker.sock");
  await new Promise((resolve) => server.listen(socketPath, resolve));
  t.after(async () => {
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
    await rm(directory, { recursive: true, force: true });
  });
  const options = {
    installation: "unit",
    controllerContainer: "controller",
    image: `sha256:${"1".repeat(64)}`,
    seccomp: {},
    socketPath,
  };
  const backend = createDockerBackend(options);
  return {
    reopen: () => createDockerBackend(options),
    backend,
    network,
    get removed() {
      return removed;
    },
  };
}

test("Docker discovery scopes installation and recovers exact worker/network ownership", async (t) => {
  const f = await fixture(t);
  const workers = await f.backend.list(AbortSignal.timeout(5000));
  t.after(() => {
    for (const worker of workers) worker.runtime.close();
  });
  assert.equal(workers.length, 1);
  await f.backend.remove(workers[0], AbortSignal.timeout(5000));
  assert.equal(f.removed, 1);
});

test("a foreign network blocks discovery and cannot be deleted as an orphan", async (t) => {
  const f = await fixture(t);
  f.network.Labels["browser-sandbox.incarnation"] = randomUUID();
  await assert.rejects(
    f.backend.list(AbortSignal.timeout(5000)),
    /browser_worker_ownership_mismatch/,
  );
  assert.equal(f.removed, 0);
});

test("Docker restart discovers and deletes an owned network after its container was already removed", async (t) => {
  const f = await fixture(t);
  const workers = await f.backend.list(AbortSignal.timeout(5000));
  t.after(() => {
    for (const worker of workers) worker.runtime.close();
  });
  f.network.failDelete = true;
  await assert.rejects(
    f.backend.remove(workers[0], AbortSignal.timeout(5000)),
    /browser_docker_operation_failed/,
  );
  assert.equal(f.removed, 1);
  assert.equal(f.network.exists, true);
  const restarted = f.reopen();
  const pending = await restarted.list(AbortSignal.timeout(5000));
  assert.equal(pending.length, 1);
  assert.equal(pending[0].infrastructureOnly, true);
  f.network.failDelete = false;
  await restarted.remove(pending[0], AbortSignal.timeout(5000));
  assert.equal(f.network.exists, false);
  assert.equal(f.removed, 1);
});
