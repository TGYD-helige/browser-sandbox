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
