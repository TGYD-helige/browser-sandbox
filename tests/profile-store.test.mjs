import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { once } from "node:events";
import fs from "node:fs/promises";
import { createServer } from "node:http";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { createProfileStore } from "../dist/profile-store.js";

async function fixture(t, options = {}) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "profile-store-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  let store;
  const server = createServer((req, res) => {
    store
      .handle(req, res)
      .then((handled) => {
        if (!handled) res.writeHead(404).end();
      })
      .catch(() => res.destroy());
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(async () => {
    await store.close();
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
  });
  const settings = {
    root,
    baseUrl: `http://127.0.0.1:${server.address().port}`,
    ...options,
  };
  store = await createProfileStore(settings);
  return {
    root,
    get store() {
      return store;
    },
    async restart() {
      await store.close();
      store = await createProfileStore(settings);
    },
  };
}
const signal = () => AbortSignal.timeout(5000);
async function upload(adapter, bytes, previousSha256 = null) {
  const { target } = await adapter.save(signal());
  const response = await fetch(target.url, { method: "PUT", body: bytes });
  assert.equal(response.status, 200);
  return {
    used: true,
    previousSha256,
    archive: {
      schema: "browser-sandbox.profile/v1",
      browserVersion: "Google Chrome 146.0.7680.177",
      iv: "1".repeat(24),
      tag: "2".repeat(32),
      sha256: createHash("sha256").update(bytes).digest("hex"),
      byteSize: bytes.length,
      objectKey: target.objectKey,
      keyMaterial: target.keyMaterial,
    },
  };
}

test("volume adapter confirms an archive and restores it after restart", async (t) => {
  const f = await fixture(t);
  const adapter = f.store.open("account/a");
  assert.deepEqual(await adapter.load(signal()), { seed: null });
  const result = await upload(adapter, Buffer.from("encrypted archive"));
  assert.deepEqual(await adapter.commit(result, signal()), {
    confirmedSha: result.archive.sha256,
  });
  await adapter.close();
  await f.restart();
  const restored = f.store.open("account/a");
  const { seed } = await restored.load(signal());
  assert.equal(seed.sha256, result.archive.sha256);
  assert.equal(seed.key.length, 64);
  assert.equal(await (await fetch(seed.url)).text(), "encrypted archive");
});

test("upload and stale commit never replace the last confirmed archive", async (t) => {
  const f = await fixture(t);
  const adapter = f.store.open("account");
  const first = await upload(adapter, Buffer.from("first"));
  await adapter.commit(first, signal());
  const second = await upload(adapter, Buffer.from("second"), first.archive.sha256);
  const stale = await upload(adapter, Buffer.from("stale"));
  await adapter.commit(second, signal());
  await assert.rejects(adapter.commit(stale, signal()), /commit_conflict/);
  assert.deepEqual(await adapter.commit(second, signal()), {
    confirmedSha: second.archive.sha256,
  });
  const { seed } = await adapter.load(signal());
  assert.equal(await (await fetch(seed.url)).text(), "second");
});

test("closing an adapter removes uncommitted data and superseded archives", async (t) => {
  const f = await fixture(t);
  const adapter = f.store.open("account");
  const first = await upload(adapter, Buffer.from("first"));
  await adapter.commit(first, signal());
  const next = await upload(adapter, Buffer.from("next"), first.archive.sha256);
  await adapter.commit(next, signal());
  await upload(adapter, Buffer.from("unused"));
  await adapter.close();
  assert.equal(
    (await fs.readdir(f.root)).filter((name) => name.endsWith(".archive")).length,
    1,
  );
});

test("confirmed Profile count and byte quotas reject replacement without data loss", async (t) => {
  const f = await fixture(t, { maxProfiles: 1, maxBytes: 5 });
  const a = f.store.open("a");
  const first = await upload(a, Buffer.from("first"));
  await a.commit(first, signal());
  const large = await upload(a, Buffer.from("longer"), first.archive.sha256);
  await assert.rejects(a.commit(large, signal()), /capacity_exceeded/);
  const b = f.store.open("b");
  const extra = await upload(b, Buffer.from("b"));
  await assert.rejects(b.commit(extra, signal()), /capacity_exceeded/);
  assert.equal((await a.load(signal())).seed.sha256, first.archive.sha256);
});

test("deletion and idle expiry refuse a leased Profile and persist across restart", async (t) => {
  let now = 1000;
  const f = await fixture(t, { idleTtlMs: 100, now: () => now });
  const adapter = f.store.open("account");
  const result = await upload(adapter, Buffer.from("archive"));
  await adapter.commit(result, signal());
  assert.throws(() => f.store.open("account"), /browser_profile_busy/);
  await assert.rejects(f.store.delete("account"), /browser_profile_busy/);
  now = 1200;
  assert.equal(await f.store.expire(), 0);
  await adapter.load(signal());
  await adapter.close();
  now = 1250;
  assert.equal(
    await f.store.expire(signal(), (profileId) => profileId === "account"),
    0,
  );
  assert.equal(await f.store.expire(), 1);
  await f.restart();
  const next = f.store.open("account");
  assert.deepEqual(await next.load(signal()), { seed: null });
  const saved = await upload(next, Buffer.from("next"));
  await next.commit(saved, signal());
  await next.close();
  assert.equal(await f.store.delete("account"), true);
  assert.equal(await f.store.delete("account"), false);
});

test("unsupported or corrupt store metadata preserves the original data", async (t) => {
  const f = await fixture(t);
  const adapter = f.store.open("account");
  await adapter.commit(await upload(adapter, Buffer.from("original")), signal());
  await adapter.close();
  const index = path.join(f.root, "index.json");
  const before = await fs.readFile(index, "utf8");
  await fs.writeFile(index, before.replace("profile-store/v1", "profile-store/v99"));
  await assert.rejects(f.restart(), /store_invalid/);
  assert.equal(
    await fs.readFile(index, "utf8"),
    before.replace("profile-store/v1", "profile-store/v99"),
  );
  await fs.writeFile(index, before);
  const data = JSON.parse(before);
  const objectKey = Object.values(data.profiles)[0].archive.objectKey;
  await fs.unlink(path.join(f.root, `${objectKey}.archive`));
  await assert.rejects(f.restart(), /store_invalid/);
  assert.equal(await fs.readFile(index, "utf8"), before);
});

test("upload grants are method scoped, revoked on close, and reject oversized bodies before reading", async (t) => {
  const f = await fixture(t);
  const adapter = f.store.open("account");
  const { target } = await adapter.save(signal());
  assert.equal((await fetch(target.url)).status, 405);
  const { request } = await import("node:http");
  const response = await new Promise((resolve, reject) => {
    const req = request(
      target.url,
      { method: "PUT", headers: { "content-length": String(256 * 1024 * 1024 + 1) } },
      (res) => {
        res.resume();
        resolve(res.statusCode);
        req.destroy();
      },
    );
    req.on("error", reject);
    req.flushHeaders();
  });
  assert.equal(response, 413);
  await adapter.close();
  assert.equal((await fetch(target.url, { method: "PUT", body: "late" })).status, 403);
});

test("staging has a byte quota independent of confirmed Profile quotas", async (t) => {
  const f = await fixture(t, { maxBytes: 1, maxStagedBytes: 4 });
  const adapter = f.store.open("account");
  await upload(adapter, Buffer.from("1234"));
  const { target } = await adapter.save(signal());
  await assert.rejects(fetch(target.url, { method: "PUT", body: "56" }));
  await adapter.close();
  const next = f.store.open("account");
  await upload(next, Buffer.from("1234"));
});

test("a Profile cannot be reopened during durable deletion", async (t) => {
  const f = await fixture(t);
  const adapter = f.store.open("account");
  await adapter.commit(await upload(adapter, Buffer.from("archive")), signal());
  await adapter.close();
  const entered = Promise.withResolvers(),
    release = Promise.withResolvers();
  const rename = fs.rename.bind(fs);
  t.mock.method(fs, "rename", async (...args) => {
    entered.resolve();
    await release.promise;
    return rename(...args);
  });
  const deleting = f.store.delete("account");
  try {
    await entered.promise;
    assert.throws(() => f.store.open("account"), /browser_profile_busy/);
  } finally {
    release.resolve();
    await deleting;
  }
});

test("an interrupted upload keeps the confirmed archive and frees transfer admission", async (t) => {
  const f = await fixture(t, { maxTransfers: 1 });
  const adapter = f.store.open("account");
  const saved = await upload(adapter, Buffer.from("confirmed"));
  await adapter.commit(saved, signal());
  const { seed } = await adapter.load(signal());
  const { target } = await adapter.save(signal());
  const { request } = await import("node:http");
  const pending = request(target.url, {
    method: "PUT",
    headers: { expect: "100-continue", "content-length": "100" },
  });
  pending.on("error", () => {});
  const continued = once(pending, "continue");
  pending.flushHeaders();
  await continued;
  pending.write("partial");
  assert.equal((await fetch(seed.url)).status, 429);
  pending.destroy();
  const deadline = signal();
  let response;
  do {
    deadline.throwIfAborted();
    response = await fetch(seed.url, { signal: deadline });
    if (response.status === 429) await response.arrayBuffer();
  } while (response.status === 429);
  assert.equal(response.status, 200);
  assert.equal(await response.text(), "confirmed");
  await assert.rejects(
    adapter.commit(
      {
        ...saved,
        archive: {
          ...saved.archive,
          objectKey: target.objectKey,
          sha256: createHash("sha256").update("partial").digest("hex"),
        },
      },
      signal(),
    ),
    /store_invalid/,
  );
});

test("the unchanged worker archive restores native Profile data through the volume adapter", async (t) => {
  const { createProfileArchive } = await import("../dist/profiles.js");
  const f = await fixture(t);
  const workerRoot = await fs.mkdtemp(path.join(os.tmpdir(), "store-worker-"));
  t.after(() => fs.rm(workerRoot, { recursive: true, force: true }));
  const worker = createProfileArchive({
    browserVersion: async () => "Google Chrome 146.0.7680.177",
  });
  async function entry(name) {
    const journalDir = path.join(workerRoot, name);
    await fs.mkdir(journalDir);
    return {
      profileId: "account",
      journalDir,
      profileDir: path.join(journalDir, "profile"),
      uid: process.getuid(),
      gid: process.getgid(),
    };
  }
  const adapter = f.store.open("account");
  const first = await entry("first");
  await worker.restore(first, await adapter.load(signal()), signal());
  await fs.mkdir(path.join(first.profileDir, "Default"));
  await fs.writeFile(
    path.join(first.profileDir, "Default", "Preferences"),
    '{"test":"retained"}',
  );
  const saved = await worker.save(first, await adapter.save(signal()), signal());
  await adapter.commit(saved, signal());
  await adapter.close();
  await f.restart();
  const next = f.store.open("account");
  const second = await entry("second");
  await worker.restore(second, await next.load(signal()), signal());
  assert.equal(
    await fs.readFile(path.join(second.profileDir, "Default", "Preferences"), "utf8"),
    '{"test":"retained"}',
  );
});

test("restart discards interrupted staging only after validating the durable index", async (t) => {
  const f = await fixture(t);
  const adapter = f.store.open("account");
  const first = await upload(adapter, Buffer.from("confirmed"));
  await adapter.commit(first, signal());
  await upload(adapter, Buffer.from("uncommitted"));
  await f.restart();
  assert.equal(
    (await fs.readdir(f.root)).filter((name) => name.endsWith(".archive")).length,
    1,
  );
  assert.equal(
    (await f.store.open("account").load(signal())).seed.sha256,
    first.archive.sha256,
  );
});

test("lost store metadata is unknown state and cannot erase existing archives", async (t) => {
  const f = await fixture(t);
  const adapter = f.store.open("account");
  await adapter.commit(await upload(adapter, Buffer.from("confirmed")), signal());
  await fs.unlink(path.join(f.root, "index.json"));
  await assert.rejects(f.restart(), /store_invalid/);
  assert.equal(
    (await fs.readdir(f.root)).filter((name) => name.endsWith(".archive")).length,
    1,
  );
});

test("same-digest confirmation remains idempotent after adapter or controller restart", async (t) => {
  let now = 1000;
  const f = await fixture(t, { now: () => now, idleTtlMs: 100 });
  const adapter = f.store.open("account");
  const saved = await upload(adapter, Buffer.from("confirmed"));
  await adapter.commit(saved, signal());
  await adapter.close();
  await f.restart();
  now = 1090;
  const next = f.store.open("account");
  assert.deepEqual(await next.commit(saved, signal()), {
    confirmedSha: saved.archive.sha256,
  });
  await next.close();
  now = 1100;
  assert.equal(await f.store.expire(), 0);
  now = 1190;
  assert.equal(await f.store.expire(), 1);
});

test("failed metadata durability never lets adapter cleanup delete the last confirmed archive", async (t) => {
  const f = await fixture(t);
  const adapter = f.store.open("account");
  const saved = await upload(adapter, Buffer.from("confirmed"));
  await adapter.commit(saved, signal());
  await adapter.load(signal());
  const pending = await upload(
    adapter,
    Buffer.from("unconfirmed"),
    saved.archive.sha256,
  );
  const open = fs.open.bind(fs);
  t.mock.method(fs, "open", async (filename, ...args) => {
    const handle = await open(filename, ...args);
    if (filename === f.root)
      t.mock.method(handle, "sync", async () => {
        throw Error("synthetic_directory_sync_failed");
      });
    return handle;
  });
  await assert.rejects(
    adapter.commit(pending, signal()),
    /synthetic_directory_sync_failed/,
  );
  await adapter.close();
  assert.equal(
    await fs.readFile(path.join(f.root, `${saved.archive.objectKey}.archive`), "utf8"),
    "confirmed",
  );
  assert.equal(
    await fs.readFile(
      path.join(f.root, `${pending.archive.objectKey}.archive`),
      "utf8",
    ),
    "unconfirmed",
  );
});

test("concurrent closes cannot release a replacement Profile lease", async (t) => {
  const f = await fixture(t);
  const adapter = f.store.open("account");
  const first = adapter.close();
  const second = adapter.close();
  const replacement = await first.then(() => f.store.open("account"));
  await second;
  await replacement.save(signal());
  assert.throws(() => f.store.open("account"), /browser_profile_busy/);
});

test("store admission bounds open Profile sessions before any transfer grant", async (t) => {
  const f = await fixture(t, { maxGrants: 1 });
  const first = f.store.open("first");
  assert.throws(() => f.store.open("second"), /browser_profile_capacity_exceeded/);
  await first.close();
  await f.store.open("second").close();
});

test("load does not renew the confirmed idle timestamp or rewrite store metadata", async (t) => {
  let now = 1000;
  const f = await fixture(t, { idleTtlMs: 100, now: () => now });
  const adapter = f.store.open("account");
  const saved = await upload(adapter, Buffer.from("confirmed"));
  await adapter.commit(saved, signal());
  const before = await fs.readFile(path.join(f.root, "index.json"), "utf8");
  now = 1090;
  await adapter.load(signal());
  assert(
    (await fs.readFile(path.join(f.root, "index.json"), "utf8")) === before,
    "load rewrote the confirmed metadata",
  );
  await adapter.close();
  now = 1100;
  assert.equal(await f.store.expire(), 1);
});

test("archive GET fails before publishing success when its file cannot open", async (t) => {
  const f = await fixture(t);
  const adapter = f.store.open("account");
  const saved = await upload(adapter, Buffer.from("confirmed"));
  await adapter.commit(saved, signal());
  const { seed } = await adapter.load(signal());
  await fs.unlink(path.join(f.root, `${saved.archive.objectKey}.archive`));
  assert.equal((await fetch(seed.url)).status, 503);
});
