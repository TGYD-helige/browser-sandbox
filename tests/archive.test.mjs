import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createCipheriv, createHash, randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import tar from "tar-stream";
import { createProfileArchive } from "../dist/profiles.js";

test("restore keeps the new tree private until extraction and ownership transfer complete", {
  skip: process.platform !== "linux" || process.getuid?.() !== 0,
}, async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "archive-ownership-"));
  await fs.chmod(root, 0o711);
  const outside = path.join(root, "outside");
  const journalDir = path.join(root, "journal");
  await fs.mkdir(outside, { mode: 0o700 });
  await fs.mkdir(journalDir, { mode: 0o711 });
  const entry = {
    profileId: "race",
    uid: 100001,
    journalDir,
    profileDir: path.join(journalDir, "profile"),
  };
  const pack = tar.pack();
  const chunks = [];
  const collected = (async () => {
    for await (const chunk of pack) chunks.push(chunk);
  })();
  pack.entry({ name: "profile", type: "directory" });
  pack.entry({ name: "profile/redirect", type: "directory" });
  pack.entry({ name: "profile/redirect/payload" }, "restored");
  pack.finalize();
  await collected;
  const key = Buffer.alloc(32, 1),
    iv = Buffer.alloc(12, 2);
  const browserVersion = "Google Chrome 146.0.7680.177";
  const cipher = createCipheriv("aes-256-gcm", key, iv);
  cipher.setAAD(Buffer.from(entry.profileId + "\n" + browserVersion));
  const encrypted = Buffer.concat([
    cipher.update(Buffer.concat(chunks)),
    cipher.final(),
  ]);
  const archive = createProfileArchive({
    browserVersion: async () => browserVersion,
    fetchImpl: async () => new Response(encrypted),
  });
  let attempted = false;
  const chown = fs.chown.bind(fs);
  t.mock.method(fs, "chown", async (name, uid, gid) => {
    await chown(name, uid, gid);
    if (name !== entry.profileDir || uid !== entry.uid || attempted) return;
    attempted = true;
    // Schedule the execution UID exactly when root first grants traversal.
    execFileSync(
      process.execPath,
      [
        "-e",
        `
      try { require('node:fs').symlinkSync(process.argv[1], process.argv[2]); }
      catch (error) { if (!['EEXIST', 'EACCES'].includes(error.code)) throw error; }
    `,
        outside,
        path.join(entry.profileDir, "redirect"),
      ],
      { uid: entry.uid, gid: entry.uid },
    );
  });
  try {
    await archive.restore(entry, {
      seed: {
        schema: "browser-sandbox.profile/v1",
        browserVersion,
        key: key.toString("hex"),
        iv: iv.toString("hex"),
        tag: cipher.getAuthTag().toString("hex"),
        sha256: createHash("sha256").update(encrypted).digest("hex"),
        byteSize: encrypted.length,
        url: "archive",
      },
    });
    assert(attempted);
    assert.equal((await fs.stat(outside)).uid, 0);
    assert.deepEqual(await fs.readdir(outside), []);
    assert.equal(
      await fs.readFile(path.join(entry.profileDir, "redirect", "payload"), "utf8"),
      "restored",
    );
    assert.equal(
      (await fs.stat(path.join(entry.profileDir, "redirect", "payload"))).uid,
      entry.uid,
    );
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

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
