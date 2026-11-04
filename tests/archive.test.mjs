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

test("save cancellation stops filesystem traversal and preserves the recoverable Profile", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "archive-cancel-"));
  const entry = {
    journalDir: root,
    profileDir: path.join(root, "profile"),
    profileId: "cancel",
    profileVersion: "Google Chrome 146.0.7680.177",
  };
  const controller = new AbortController();
  await fs.mkdir(entry.profileDir);
  await fs.writeFile(path.join(entry.profileDir, "saved"), "recoverable");
  const readdir = fs.readdir.bind(fs),
    lstat = fs.lstat.bind(fs);
  let continued = false;
  t.mock.method(fs, "readdir", async (name, ...args) => {
    const result = await readdir(name, ...args);
    if (name === entry.profileDir) controller.abort();
    return result;
  });
  t.mock.method(fs, "lstat", async (name, ...args) => {
    if (controller.signal.aborted && name === path.join(entry.profileDir, "saved"))
      continued = true;
    return lstat(name, ...args);
  });
  const archive = createProfileArchive({
    fetchImpl: async () => {
      throw Error("must not upload");
    },
  });
  try {
    await assert.rejects(
      archive.save(
        entry,
        { target: { key: "a".repeat(64), url: "unused" } },
        controller.signal,
      ),
      { name: "AbortError" },
    );
    assert.equal(continued, false, "traversal continued after cancellation");
    assert.equal(
      await fs.readFile(path.join(entry.profileDir, "saved"), "utf8"),
      "recoverable",
    );
    assert.deepEqual((await fs.readdir(root)).sort(), ["profile"]);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("restore cancellation drains pending writes before reporting failure and never hands off a partial tree", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "restore-cancel-"));
  const browserVersion = "Google Chrome 146.0.7680.177";
  let encrypted;
  const archive = createProfileArchive({
    browserVersion: async () => browserVersion,
    fetchImpl: async (_url, options) => {
      if (options?.method === "PUT") {
        const chunks = [];
        for await (const chunk of options.body) chunks.push(chunk);
        encrypted = Buffer.concat(chunks);
      }
      return new Response(encrypted);
    },
  });
  const entry = {
    journalDir: root,
    profileDir: path.join(root, "profile"),
    profileId: "cancel",
    uid: process.getuid(),
    gid: process.getgid(),
  };
  const target = { key: "a".repeat(64), url: "archive" };
  try {
    await archive.restore(entry, { seed: null });
    for (const name of ["first", "later"])
      await fs.mkdir(path.join(entry.profileDir, name));
    const saved = await archive.save(entry, { target });
    const controller = new AbortController(),
      reached = Promise.withResolvers(),
      pending = Promise.withResolvers();
    const mkdir = fs.mkdir.bind(fs);
    t.mock.method(fs, "mkdir", async (name, ...args) => {
      const result = await mkdir(name, ...args);
      if (name === path.join(entry.profileDir, "first")) {
        controller.abort();
        reached.resolve();
        await pending.promise;
      }
      return result;
    });
    let settled = false;
    const restoring = archive.restore(
      entry,
      { seed: { ...saved.archive, key: target.key, url: target.url } },
      controller.signal,
    );
    const rejected = assert
      .rejects(restoring, /browser_profile_restore_failed/)
      .finally(() => {
        settled = true;
      });
    try {
      await reached.promise;
      await new Promise((resolve) => setImmediate(resolve));
      assert.equal(settled, false, "restore returned before the pending write drained");
    } finally {
      pending.resolve();
    }
    await rejected;
    await assert.rejects(fs.stat(path.join(entry.profileDir, "later")), {
      code: "ENOENT",
    });
    assert.deepEqual((await fs.readdir(root)).sort(), ["profile"]);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});
