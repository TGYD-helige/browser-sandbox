import assert from "node:assert/strict";
import { once } from "node:events";
import { mkdtemp, rm, symlink } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { transferFile } from "../dist/files.js";

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
