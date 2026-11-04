import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { clearWindowState, createLocalProfiles } from "../dist/local-profiles.js";

const linux = process.platform === "linux" && process.getuid?.() === 0;
const readAs = (uid, file) =>
  execFileSync(
    "setpriv",
    ["--reuid=" + uid, "--regid=" + uid, "--clear-groups", "cat", file],
    { stdio: ["ignore", "pipe", "pipe"] },
  ).toString();

test("window cleanup cannot traverse an execution-owned Default symlink", {
  skip: !linux,
}, async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "profile-window-links-"));
  await fs.chmod(root, 0o711);
  const profile = path.join(root, "profile"),
    outside = path.join(root, "outside");
  await fs.mkdir(profile, { mode: 0o700 });
  await fs.chown(profile, 100001, 100001);
  await fs.mkdir(path.join(outside, "Sessions"), { recursive: true, mode: 0o700 });
  await fs.writeFile(path.join(outside, "Sessions", "sentinel"), "keep");
  execFileSync(
    process.execPath,
    [
      "-e",
      "require('node:fs').symlinkSync(process.argv[1], process.argv[2])",
      outside,
      path.join(profile, "Default"),
    ],
    { uid: 100001, gid: 100001 },
  );
  try {
    await assert.rejects(clearWindowState(profile));
    assert.equal(
      await fs.readFile(path.join(outside, "Sessions", "sentinel"), "utf8"),
      "keep",
    );
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("window cleanup pins nested directories against execution rename races", {
  skip: !linux,
}, async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "profile-window-race-"));
  await fs.chmod(root, 0o711);
  const profile = path.join(root, "profile"),
    defaults = path.join(profile, "Default"),
    state = path.join(defaults, "Sessions"),
    outside = path.join(root, "outside");
  await fs.mkdir(state, { recursive: true, mode: 0o700 });
  for (const name of [profile, defaults, state]) await fs.chown(name, 100001, 100001);
  await fs.mkdir(outside, { mode: 0o700 });
  await fs.writeFile(path.join(state, "sentinel"), "old");
  await fs.writeFile(path.join(outside, "sentinel"), "keep");
  const readdir = fs.readdir.bind(fs);
  let replaced = false;
  t.mock.method(fs, "readdir", async (name, ...args) => {
    const entries = await readdir(name, ...args);
    if (!replaced && (await fs.realpath(name)) === state) {
      replaced = true;
      execFileSync(
        process.execPath,
        [
          "-e",
          `
        const fs = require('node:fs'); fs.renameSync(process.argv[1], process.argv[1] + '-old');
        fs.symlinkSync(process.argv[2], process.argv[1]);
      `,
          state,
          outside,
        ],
        { uid: 100001, gid: 100001 },
      );
    }
    return entries;
  });
  try {
    await assert.rejects(clearWindowState(profile));
    assert(replaced);
    assert.equal(await fs.readFile(path.join(outside, "sentinel"), "utf8"), "keep");
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});
test("a private Profile survives execution cleanup and securely transfers across UIDs and processes", {
  skip: !linux,
}, async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "local-profile-"));
  await fs.chmod(root, 0o711);
  const store = createLocalProfiles({ root: path.join(root, "profiles") });
  let a, b, other;
  try {
    a = await store.acquire("other-harness:tenant/a", "execution-a", 100001);
    const file = path.join(a.profileDir, "native-data");
    await fs.writeFile(file, "saved");
    await fs.chmod(file, 0o600);
    await fs.chown(file, 100001, 100001);
    assert.equal(readAs(100001, file), "saved");
    assert.throws(() => readAs(100002, file));
    const probe = `import {createLocalProfiles} from ${JSON.stringify(new URL("../dist/local-profiles.js", import.meta.url).href)};try{await createLocalProfiles({root:process.argv[1]}).acquire('other-harness:tenant/a','competitor',100002);process.exit(2)}catch(e){console.log(e.message)}`;
    assert.equal(
      execFileSync(process.execPath, [
        "--input-type=module",
        "-e",
        probe,
        path.join(root, "profiles"),
      ])
        .toString()
        .trim(),
      "browser_profile_busy",
    );
    await a.release();
    a = null;
    assert.throws(() => readAs(100001, file));
    b = await store.acquire("other-harness:tenant/a", "execution-b", 100002);
    assert.equal(readAs(100002, file), "saved");
    assert.throws(() => readAs(100001, file));
    other = await store.acquire("other-harness:tenant/b", "execution-c", 100003);
    assert.deepEqual(await fs.readdir(other.profileDir), []);
    assert.throws(() => readAs(100003, file));
    await b.release();
    b = null;
    await other.release();
    other = null;
  } finally {
    await a?.abandon();
    await b?.abandon();
    await other?.abandon();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("an unclean owner loss never steals permissions from an unverified Chrome", {
  skip: !linux,
}, async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "local-profile-fence-"));
  try {
    const store = createLocalProfiles({ root });
    const a = await store.acquire("profile-a", "execution-a", 100001);
    await a.abandon();
    const active = path.join(a.journalDir, "active.json"),
      previous = await fs.readFile(active, "utf8");
    await assert.rejects(
      store.acquire("profile-a", "execution-b", 100002),
      /browser_profile_stop_unconfirmed/,
    );
    assert.equal(await fs.readFile(active, "utf8"), previous);
    await assert.rejects(
      store.acquire("profile-a", "execution-c", 100003),
      /browser_profile_stop_unconfirmed/,
    );
    assert.equal((await fs.stat(a.profileDir)).uid, 100001);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

for (const fault of ["mkdir", "ownership", "marker-write"])
  test(`Profile ${fault} preparation failure rolls back only its own marker and permits retry`, {
    skip: !linux,
  }, async (t) => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "local-profile-retry-"));
    const directory = path.join(
        root,
        createHash("sha256").update("profile-retry").digest("hex"),
      ),
      profileDir = path.join(directory, "profile"),
      active = path.join(directory, "active.json");
    const store = createLocalProfiles({ root });
    let lease,
      injected = false;
    const method = fault === "mkdir" ? "mkdir" : "open",
      original = fs[method].bind(fs);
    t.mock.method(fs, method, async (file, ...args) => {
      if (fault !== "marker-write" && file === profileDir && !injected) {
        assert.equal(
          JSON.parse(await fs.readFile(active, "utf8")).executionId,
          "failed-preparation",
        );
        injected = true;
        throw Object.assign(Error("temporary preparation failure"), {
          code: fault === "mkdir" ? "ENOSPC" : "EIO",
        });
      }
      const result = await original(file, ...args);
      if (fault === "marker-write" && file === active && !injected)
        t.mock.method(result, "writeFile", async () => {
          injected = true;
          await result.write("partial");
          throw Object.assign(Error("temporary marker write failure"), {
            code: "EIO",
          });
        });
      return result;
    });
    try {
      await assert.rejects(
        store.acquire("profile-retry", "failed-preparation", 100001).then((value) => {
          lease = value;
          return value;
        }),
        { code: fault === "mkdir" ? "ENOSPC" : "EIO" },
      );
      assert.equal(injected, true);
      await assert.rejects(fs.stat(active), { code: "ENOENT" });
      lease = await store.acquire("profile-retry", "retry", 100002);
      assert.equal((await fs.stat(lease.profileDir)).uid, 100002);
      assert.equal(JSON.parse(await fs.readFile(active, "utf8")).executionId, "retry");
      await lease.release();
      lease = null;
      await assert.rejects(fs.stat(active), { code: "ENOENT" });
    } finally {
      await lease?.abandon();
      await fs.rm(root, { recursive: true, force: true });
    }
  });
