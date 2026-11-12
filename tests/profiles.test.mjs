import assert from "node:assert/strict";
import childProcess, { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs/promises";
import { syncBuiltinESMExports } from "node:module";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import {
  clearWindowState,
  createLocalProfiles,
  removeTree,
} from "../dist/local-profiles.js";

const linux = process.platform === "linux" && process.getuid?.() === 0;
const readAs = (uid, file) =>
  execFileSync(
    "setpriv",
    ["--reuid=" + uid, "--regid=" + uid, "--clear-groups", "cat", file],
    { stdio: ["ignore", "pipe", "pipe"] },
  ).toString();

test("tree removal never follows a directory swapped for a link after it was opened", {
  skip: process.platform !== "linux",
}, async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "remove-swap-"));
  try {
    const tree = path.join(root, "tree"),
      outside = path.join(root, "outside");
    await fs.mkdir(path.join(tree, "sub"), { recursive: true });
    await fs.writeFile(path.join(tree, "sub", "owned"), "remove");
    await fs.mkdir(outside);
    await fs.writeFile(path.join(outside, "keep"), "keep");
    const open = fs.open.bind(fs);
    let swapped = false;
    t.mock.method(fs, "open", async (name, ...args) => {
      const handle = await open(name, ...args);
      if (!swapped && path.basename(String(name)) === "sub") {
        swapped = true;
        await fs.rename(path.join(tree, "sub"), path.join(root, "moved"));
        await fs.symlink(outside, path.join(tree, "sub"));
      }
      return handle;
    });
    await removeTree(tree);
    assert(swapped);
    assert.equal(await fs.readFile(path.join(outside, "keep"), "utf8"), "keep");
    await assert.rejects(fs.lstat(tree), { code: "ENOENT" });
  } finally {
    t.mock.restoreAll();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("tree removal handles nesting deeper than its descriptor bound", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "remove-deep-"));
  try {
    const tree = path.join(root, "tree");
    let leaf = tree;
    for (let depth = 0; depth < 200; depth++) leaf = path.join(leaf, "d");
    await fs.mkdir(leaf, { recursive: true });
    await fs.writeFile(path.join(leaf, "file"), "deep");
    await removeTree(tree);
    assert.deepEqual(await fs.readdir(root), []);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("cancelled Profile handoff stops traversal, rolls back acquisition and permits retry", {
  skip: !linux,
}, async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "profile-cancel-"));
  const store = createLocalProfiles({ root });
  let lease;
  try {
    lease = await store.acquire("cancel", "seed", 100001);
    await fs.writeFile(path.join(lease.profileDir, "saved"), "keep");
    const profileDir = lease.profileDir;
    await lease.release();
    lease = null;
    const abort = new AbortController();
    const readdir = fs.readdir.bind(fs);
    let cancelled = false;
    t.mock.method(fs, "readdir", async (...args) => {
      const entries = await readdir(...args);
      if (!cancelled) {
        cancelled = true;
        abort.abort(Error("handoff_cancelled"));
      }
      return entries;
    });
    await assert.rejects(
      store.acquire("cancel", "cancelled", 100002, abort.signal).then((value) => {
        lease = value;
        return value;
      }),
      /handoff_cancelled/,
    );
    assert(cancelled);
    assert.equal((await fs.stat(path.join(profileDir, "saved"))).uid, 0);
    lease = await store.acquire("cancel", "retry", 100003);
    assert.equal(await fs.readFile(path.join(profileDir, "saved"), "utf8"), "keep");
    const releaseAbort = new AbortController();
    releaseAbort.abort(Error("release_cancelled"));
    await assert.rejects(lease.release(releaseAbort.signal), /release_cancelled/);
    await assert.rejects(store.acquire("cancel", "competitor", 100004), /profile_busy/);
    await lease.release();
    lease = null;
  } finally {
    await lease?.abandon();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("cancelling lock-helper startup waits for helper exit before retry", {
  skip: !linux,
}, async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "profile-lock-cancel-"));
  const store = createLocalProfiles({ root });
  const abort = new AbortController();
  const spawn = childProcess.spawn;
  let lease, helper;
  let started;
  const spawned = new Promise((resolve) => {
    started = resolve;
  });
  t.mock.method(childProcess, "spawn", () => {
    helper = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], {
      stdio: ["pipe", "pipe", "ignore"],
    });
    helper.once("spawn", started);
    return helper;
  });
  syncBuiltinESMExports();
  const acquisition = store
    .acquire("cancel-lock", "cancelled", 100001, abort.signal)
    .then((value) => {
      lease = value;
      return value;
    });
  const rejected = assert.rejects(acquisition, /lock_cancelled/);
  rejected.catch(() => {});
  try {
    await spawned;
    abort.abort(Error("lock_cancelled"));
    await rejected;
    assert.equal(helper.signalCode, "SIGKILL");
    assert.throws(() => process.kill(helper.pid, 0), { code: "ESRCH" });
    t.mock.restoreAll();
    syncBuiltinESMExports();
    lease = await store.acquire("cancel-lock", "retry", 100002);
    await lease.release();
    lease = null;
  } finally {
    abort.abort(Error("lock_cancelled"));
    t.mock.restoreAll();
    syncBuiltinESMExports();
    await acquisition.catch(() => {});
    await lease?.abandon();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("window cleanup cannot traverse an execution-owned Default symlink", {
  skip: !linux,
}, async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "profile-window-links-"));
  await fs.chmod(root, 0o711);
  const profile = path.join(root, "profile"),
    outside = path.join(root, "outside");
  await fs.mkdir(profile, { mode: 0o700 });
  await fs.chown(profile, 100001, 100001);
  await fs.mkdir(path.join(outside, "Sessions"), {
    recursive: true,
    mode: 0o700,
  });
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
    // Cleanup removes the swapped-in link itself and never its target.
    await clearWindowState(profile);
    assert(replaced);
    assert.equal(await fs.readFile(path.join(outside, "sentinel"), "utf8"), "keep");
    await assert.rejects(fs.lstat(state), { code: "ENOENT" });
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

test("Profile handoff bounds concurrent file work and drains failures before unlocking", {
  skip: !linux,
}, async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "profile-workers-"));
  const store = createLocalProfiles({ root });
  let lease = await store.acquire("workers", "seed", 100001);
  const directory = lease.profileDir;
  for (let i = 0; i < 12; i++)
    await fs.writeFile(path.join(directory, `file-${i}`), "keep");
  await lease.release();
  lease = null;
  const open = fs.open.bind(fs);
  let active = 0,
    peak = 0,
    fail = false;
  t.mock.method(fs, "open", async (...args) => {
    const fd = await open(...args);
    const stat = await fd.stat();
    if (stat.isFile() && stat.size === 4) {
      const original = fd.stat.bind(fd);
      t.mock.method(fd, "stat", async () => {
        active++;
        peak = Math.max(peak, active);
        try {
          await new Promise((resolve) => setTimeout(resolve, 5));
          if (fail) throw Error("handoff_injected_failure");
          return await original();
        } finally {
          active--;
        }
      });
    }
    return fd;
  });
  try {
    lease = await store.acquire("workers", "parallel", 100002);
    assert(peak > 1 && peak <= 4);
    await lease.release();
    lease = null;
    fail = true;
    await assert.rejects(
      store.acquire("workers", "failed", 100003),
      /handoff_injected_failure/,
    );
    assert.equal(active, 0);
    fail = false;
    lease = await store.acquire("workers", "retry", 100004);
    assert.equal(await fs.readFile(path.join(directory, "file-0"), "utf8"), "keep");
    await lease.release();
    lease = null;
  } finally {
    t.mock.restoreAll();
    await lease?.abandon();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("parallel Profile handoff rejects links without changing outside ownership or permissions", {
  skip: !linux,
}, async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "profile-links-"));
  const store = createLocalProfiles({ root });
  const outside = path.join(root, "outside");
  await fs.writeFile(outside, "private", { mode: 0o600 });
  try {
    for (const kind of ["symlink", "hardlink"]) {
      const lease = await store.acquire(kind, "seed", 100001);
      const profileDir = lease.profileDir;
      await lease.release();
      const link = path.join(profileDir, "unsafe");
      if (kind === "symlink") await fs.symlink(outside, link);
      else await fs.link(outside, link);
      await assert.rejects(store.acquire(kind, "blocked", 100002));
      const stat = await fs.stat(outside);
      assert.equal(stat.uid, 0);
      assert.equal(stat.mode & 0o7777, 0o600);
      await fs.unlink(link);
      const retry = await store.acquire(kind, "retry", 100002);
      await retry.release();
    }
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("Profile handoff normalizes permissive and special modes across ownership changes", {
  skip: !linux,
}, async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "profile-modes-"));
  const store = createLocalProfiles({ root });
  let lease;
  try {
    lease = await store.acquire("modes", "seed", 100001);
    const directory = path.join(lease.profileDir, "nested");
    const file = path.join(directory, "saved");
    await fs.mkdir(directory);
    await fs.writeFile(file, "keep");
    await fs.chmod(directory, 0o2777);
    await fs.chmod(file, 0o4666);
    await lease.release();
    lease = await store.acquire("modes", "next", 100002);
    for (const [name, mode] of [
      [lease.profileDir, 0o700],
      [directory, 0o700],
      [file, 0o600],
    ]) {
      const stat = await fs.stat(name);
      assert.equal(stat.uid, 100002);
      assert.equal(stat.gid, 100002);
      assert.equal(stat.mode & 0o7777, mode);
    }
    await lease.release();
    lease = null;
    assert.equal((await fs.stat(file)).uid, 0);
    assert.equal((await fs.stat(file)).mode & 0o7777, 0o600);
  } finally {
    await lease?.abandon();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("Profile release seals permissions changed by its old UID after the stat snapshot", {
  skip: !linux,
}, async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "profile-mode-race-"));
  await fs.chmod(root, 0o711);
  const store = createLocalProfiles({ root });
  let lease;
  let raced = false;
  try {
    lease = await store.acquire("mode-race", "seed", 100001);
    const profileDir = lease.profileDir;
    const open = fs.open.bind(fs);
    t.mock.method(fs, "open", async (name, ...args) => {
      const fd = await open(name, ...args);
      if (name === profileDir && !raced) {
        const stat = fd.stat.bind(fd);
        t.mock.method(fd, "stat", async () => {
          const snapshot = await stat();
          execFileSync(
            process.execPath,
            ["-e", "require('node:fs').chmodSync(process.argv[1], 0o777)", profileDir],
            { uid: 100001, gid: 100001 },
          );
          raced = true;
          return snapshot;
        });
      }
      return fd;
    });
    await lease.release();
    lease = null;
    assert(raced);
    assert.equal((await fs.stat(profileDir)).uid, 0);
    assert.equal((await fs.stat(profileDir)).mode & 0o7777, 0o700);
  } finally {
    t.mock.restoreAll();
    await lease?.abandon();
    await fs.rm(root, { recursive: true, force: true });
  }
});
