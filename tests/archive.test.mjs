import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { createProfileArchive } from "../dist/profiles.js";

test("runtime saves encrypted immutable data and cold restores only the same Profile", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "profile-archive-")),
    objects = new Map();
  const archive = createProfileArchive({
    browserVersion: async () => "Google Chrome 146.0.7680.177",
    fetchImpl: async (url, options) => {
      if (options?.method === "PUT") {
        const chunks = [];
        for await (const chunk of options.body) chunks.push(chunk);
        objects.set(url, Buffer.concat(chunks));
        return new Response("", { status: 200 });
      }
      return new Response(objects.get(url), {
        status: objects.has(url) ? 200 : 404,
      });
    },
  });
  const entry = async (profileId = "generic:profile-a") => {
    const journalDir = path.join(root, randomUUID());
    await fs.mkdir(journalDir);
    return {
      profileId,
      uid: process.getuid(),
      gid: process.getgid(),
      journalDir,
      profileDir: path.join(journalDir, "profile"),
    };
  };
  const target = {
    url: "object-a",
    key: "a".repeat(64),
    headers: {},
    objectKey: "object-a",
    keyMaterial: { wrapped: true },
  };
  try {
    const a = await entry();
    await archive.restore(a, { seed: null });
    await fs.mkdir(path.join(a.profileDir, "Default", "IndexedDB"), {
      recursive: true,
    });
    await fs.writeFile(
      path.join(a.profileDir, "Default", "IndexedDB", "blob"),
      "native-blob",
    );
    await fs.writeFile(
      path.join(a.profileDir, "Default", "IndexedDB", "SingletonUserData"),
      "retain",
    );
    const result = await archive.save(a, { target });
    assert.equal(result.previousSha256, null);
    assert(!objects.get("object-a").includes(Buffer.from("native-blob")));
    assert(!JSON.stringify(result).includes(target.key));
    await fs.rm(a.journalDir, { recursive: true });
    const seed = { ...result.archive, url: "object-a", key: target.key };
    const b = await entry();
    await archive.restore(b, { seed });
    assert.equal(
      await fs.readFile(
        path.join(b.profileDir, "Default", "IndexedDB", "blob"),
        "utf8",
      ),
      "native-blob",
    );
    assert.equal(
      await fs.readFile(
        path.join(b.profileDir, "Default", "IndexedDB", "SingletonUserData"),
        "utf8",
      ),
      "retain",
    );
    const foreign = await entry("generic:profile-b");
    await assert.rejects(archive.restore(foreign, { seed }), /restore_failed/);
    await assert.rejects(fs.stat(foreign.profileDir), { code: "ENOENT" });
    const upgrade = createProfileArchive({
      browserVersion: async () => "Google Chrome 147.0.7700.1",
      fetchImpl: async () => new Response(objects.get("object-a")),
    });
    const c = await entry();
    await upgrade.restore(c, { seed });
    assert.equal(c.profileVersion, "Google Chrome 147.0.7700.1");
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});
