import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, rm, symlink, truncate, writeFile } from "node:fs/promises";
import { createServer, get } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { transferFile } from "../dist/files.js";

test("truncating an active download fails only that transfer and leaves the server alive", {
  timeout: 10_000,
}, async () => {
  const root = await mkdtemp(join(tmpdir(), "browser-truncated-files-"));
  await writeFile(join(root, "large.bin"), Buffer.alloc(16 * 1024 * 1024, 0x61));
  const child = spawn(
    process.execPath,
    [
      "--input-type=module",
      "--eval",
      `
    import { createServer } from 'node:http';
    import { transferFile } from ${JSON.stringify(new URL("../dist/files.js", import.meta.url).href)};
    let active = 0;
    const server = createServer((req, res) => {
      if (req.url === '/health') return res.end(String(active));
      active++;
      transferFile(process.argv[1], process.getuid(), 'large.bin', req, res).finally(() => active--);
    });
    server.listen(0, '127.0.0.1', () => process.send(server.address().port));
  `,
      root,
    ],
    { stdio: ["ignore", "ignore", "pipe", "ipc"] },
  );
  let stderr = "";
  child.stderr.on("data", (chunk) => {
    stderr += chunk;
  });
  const exited = once(child, "exit");
  let request;
  try {
    const [port] = await once(child, "message", {
      signal: AbortSignal.timeout(3000),
    });
    const base = `http://127.0.0.1:${port}`;
    const response = await new Promise((resolve, reject) => {
      request = get(base + "/large.bin", { agent: false }, resolve);
      request.on("error", reject);
    });
    response.on("error", () => {});
    response.pause();
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.equal(await (await fetch(base + "/health")).text(), "1");
    await truncate(join(root, "large.bin"), 0);
    const closed = new Promise((resolve) => response.once("close", resolve));
    response.resume();
    await closed;
    assert.equal(response.complete, false, "truncated downloads must not succeed");
    assert.equal(child.exitCode, null, stderr);
    assert.equal(await (await fetch(base + "/health")).text(), "0");
    assert.equal(child.exitCode, null, stderr);
  } finally {
    request?.destroy();
    child.kill();
    await exited;
    await rm(root, { recursive: true, force: true });
  }
});

test("slow downloads stay bounded and tracked until finish or cancellation", async () => {
  const root = await mkdtemp(join(tmpdir(), "browser-slow-files-"));
  const bytes = Buffer.alloc(16 * 1024 * 1024, 0x61);
  await writeFile(join(root, "large.bin"), bytes);
  let finished = false,
    outgoing,
    transfer;
  const server = createServer((req, res) => {
    outgoing = res;
    transfer = transferFile(root, process.getuid(), "large.bin", req, res).then(() => {
      finished = true;
    });
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  let request;
  try {
    const response = await new Promise((resolve, reject) => {
      request = get(
        `http://127.0.0.1:${server.address().port}/large.bin`,
        { agent: false },
        resolve,
      );
      request.on("error", reject);
    });
    response.pause();
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.equal(
      finished,
      false,
      "returning before HTTP finish loses cancellation and the deadline",
    );
    assert(
      outgoing.writableLength <= 128 * 1024,
      "backpressure must bound queued file bytes",
    );
    const closed = once(response, "close");
    response.destroy();
    await closed;
    await transfer;
    assert.equal(finished, true);
    assert.equal(response.complete, false);
  } finally {
    request?.destroy();
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
    await rm(root, { recursive: true, force: true });
  }
});

test("file transfer round-trips bytes and refuses traversal, overwrite, and links", async () => {
  const root = await mkdtemp(join(tmpdir(), "browser-files-"));
  const server = createServer((req, res) =>
    transferFile(root, process.getuid(), req.url.slice(1), req, res),
  );
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const base = `http://127.0.0.1:${server.address().port}`;
  try {
    assert.equal(
      (await fetch(base + "/fixture.txt", { method: "PUT", body: "payload" })).status,
      201,
    );
    assert.equal(await (await fetch(base + "/fixture.txt")).text(), "payload");
    assert.equal(
      (await fetch(base + "/empty.txt", { method: "PUT", body: "" })).status,
      201,
    );
    assert.equal(await (await fetch(base + "/empty.txt")).text(), "");
    assert.equal(
      (await fetch(base + "/fixture.txt", { method: "PUT", body: "replace" })).status,
      409,
    );
    assert.equal((await fetch(base + "/%2e%2e%2fsecret")).status, 400);
    await symlink("/etc/passwd", join(root, "link"));
    assert.equal((await fetch(base + "/link")).status, 400);
    assert.equal((await fetch(base + "/missing")).status, 404);
  } finally {
    await new Promise((resolve) => server.close(resolve));
    await rm(root, { recursive: true, force: true });
  }
});
