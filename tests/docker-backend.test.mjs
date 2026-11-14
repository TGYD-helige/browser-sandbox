import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { createDockerBackend } from "../dist/docker-backend.js";

test("Docker removal checks ownership and is idempotent after confirmed absence", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "docker-control-"));
  const socketPath = join(root, "docker.sock");
  let absent = false;
  const calls = [];
  const server = createServer((req, res) => {
    calls.push(`${req.method} ${req.url}`);
    if (absent) {
      res.writeHead(404).end("{}");
      return;
    }
    res.setHeader("content-type", "application/json");
    res.end(
      JSON.stringify({
        Config: {
          Labels: {
            "browser-sandbox.installation": "different-installation",
            "browser-sandbox.network": "private",
          },
        },
      }),
    );
  });
  await new Promise((resolve) => server.listen(socketPath, resolve));
  t.after(async () => {
    await new Promise((resolve) => server.close(resolve));
    await rm(root, { recursive: true, force: true });
  });
  const backend = createDockerBackend({
    installation: "test",
    controllerContainer: "controller",
    image: "sha256:" + "a".repeat(64),
    socketPath,
    seccomp: {},
  });
  const worker = {
    id: "worker",
    executionId: "execution",
    incarnation: "incarnation",
    origin: "http://worker",
    runtime: {},
  };
  await assert.rejects(
    backend.remove(worker, AbortSignal.timeout(1000)),
    /browser_worker_ownership_mismatch/,
  );
  assert(
    calls.every((call) => call.startsWith("GET ")),
    "Mismatched ownership mutated Docker",
  );
  absent = true;
  await backend.remove(worker, AbortSignal.timeout(1000));
});

test("Docker creation failure removes a network before the controller was attached", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "docker-create-"));
  const socketPath = join(root, "docker.sock");
  let removed = false;
  const labels = {
    "browser-sandbox.installation": "test",
    "browser-sandbox.execution": "execution",
    "browser-sandbox.incarnation": "incarnation",
  };
  const server = createServer((req, res) => {
    res.setHeader("content-type", "application/json");
    if (req.url === "/v1.47/networks/create")
      res.end(JSON.stringify({ Id: "network" }));
    else if (req.url === "/v1.47/networks/network/connect")
      res.writeHead(500).end("{}");
    else if (req.url === "/v1.47/networks/network" && req.method === "GET")
      res.end(JSON.stringify({ Labels: labels, Containers: {} }));
    else if (req.url === "/v1.47/networks/network" && req.method === "DELETE") {
      removed = true;
      res.writeHead(204).end();
    } else res.writeHead(403).end("{}");
  });
  await new Promise((resolve) => server.listen(socketPath, resolve));
  t.after(async () => {
    await new Promise((resolve) => server.close(resolve));
    await rm(root, { recursive: true, force: true });
  });
  const backend = createDockerBackend({
    installation: "test",
    controllerContainer: "controller",
    image: "sha256:" + "a".repeat(64),
    socketPath,
    seccomp: {},
  });
  await assert.rejects(
    backend.create(
      { executionId: "execution", incarnation: "incarnation" },
      AbortSignal.timeout(1000),
    ),
  );
  assert(removed, "Unattached worker network was left behind after failed creation");
});

test("Docker creation reports both operation and cleanup errors", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "docker-cleanup-"));
  const socketPath = join(root, "docker.sock");
  const server = createServer((req, res) => {
    res.setHeader("content-type", "application/json");
    if (req.url === "/v1.47/networks/create")
      res.end(JSON.stringify({ Id: "network" }));
    else res.writeHead(500).end("{}");
  });
  await new Promise((resolve) => server.listen(socketPath, resolve));
  t.after(async () => {
    await new Promise((resolve) => server.close(resolve));
    await rm(root, { recursive: true, force: true });
  });
  const backend = createDockerBackend({
    installation: "test",
    controllerContainer: "controller",
    image: "sha256:" + "a".repeat(64),
    socketPath,
    seccomp: {},
  });
  await assert.rejects(
    backend.create(
      { executionId: "execution", incarnation: "incarnation" },
      AbortSignal.timeout(1000),
    ),
    (error) => {
      assert(error instanceof AggregateError);
      assert.equal(error.errors.length, 3);
      assert.equal(error.errors[2].statusCode, 500);
      assert.equal(error.errors[0].statusCode, 500);
      assert.equal(error.errors[1].statusCode, 500);
      assert.match(error.message, /browser_worker_cleanup_unconfirmed/);
      return true;
    },
  );
});

for (const phase of ["network", "container"]) {
  test(`cancelled Docker ${phase} creation removes resources whose response was not received`, async (t) => {
    const root = await mkdtemp(join(tmpdir(), "docker-cancel-"));
    const socketPath = join(root, "docker.sock");
    let enter, release;
    const entered = new Promise((resolve) => {
      enter = resolve;
    });
    const delayed = new Promise((resolve) => {
      release = resolve;
    });
    const resources = new Map();
    const name = "bs-incarnation";
    const server = createServer(async (req, res) => {
      let input = "";
      for await (const chunk of req) input += chunk;
      const body = input ? JSON.parse(input) : {};
      res.setHeader("content-type", "application/json");
      const createNetwork = req.url === "/v1.47/networks/create";
      const createContainer = req.url.startsWith("/v1.47/containers/create?");
      if (createNetwork || createContainer) {
        if ((phase === "network") === createNetwork) {
          enter();
          await delayed;
        }
        resources.set(createNetwork ? "network" : "container", body.Labels);
        res.end(JSON.stringify({ Id: createNetwork ? "network" : "container" }));
        return;
      }
      if (req.url.endsWith("/connect")) return res.end("{}");
      const kind = req.url.includes("/containers/") ? "container" : "network";
      if (!resources.has(kind)) return res.writeHead(404).end("{}");
      assert(req.url.includes(kind) || req.url.includes(name));
      if (req.method === "DELETE") {
        resources.delete(kind);
        return res.writeHead(204).end();
      }
      res.end(
        JSON.stringify(
          kind === "container"
            ? { Config: { Labels: resources.get(kind) } }
            : { Labels: resources.get(kind), Containers: {} },
        ),
      );
    });
    await new Promise((resolve) => server.listen(socketPath, resolve));
    t.after(async () => {
      release();
      server.closeAllConnections();
      await new Promise((resolve) => server.close(resolve));
      await rm(root, { recursive: true, force: true });
    });
    const backend = createDockerBackend({
      installation: "test",
      controllerContainer: "controller",
      image: "sha256:" + "a".repeat(64),
      socketPath,
      seccomp: {},
    });
    const life = new AbortController();
    const failed = assert.rejects(
      backend.create(
        { executionId: "execution", incarnation: "incarnation" },
        life.signal,
      ),
    );
    await entered;
    life.abort();
    release();
    await failed;
    assert.deepEqual(
      [...resources.keys()],
      [],
      "Cancelled creation leaked Docker resources",
    );
  });
}
