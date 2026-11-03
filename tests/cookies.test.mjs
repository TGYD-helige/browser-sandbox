import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { mkdir, mkdtemp, readdir, readFile, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { createCookieStore } from "../dist/cookies.js";

function fixture() {
  let cookies = [];
  const calls = [];
  class Socket extends EventEmitter {
    constructor() {
      super();
      queueMicrotask(() => this.emit("open"));
    }
    send(source) {
      const { id, method, params } = JSON.parse(source);
      calls.push(method);
      if (method === "Storage.clearCookies") cookies = [];
      if (method === "Storage.setCookies") cookies = params.cookies;
      queueMicrotask(() =>
        this.emit("message", JSON.stringify({ id, result: { cookies } })),
      );
    }
    terminate() {
      queueMicrotask(() => this.emit("close"));
    }
  }
  return {
    Socket,
    calls,
    set: (value) => {
      cookies = value;
    },
    get: () => cookies,
  };
}
const cookie = (name) => ({
  name,
  value: "private-value",
  domain: "example.com",
  path: "/",
  expires: Date.now() / 1000 + 3600,
  session: false,
});
test("managed closes preserve persistent cookies, deletions and profile isolation without reviving session cookies", async () => {
  const root = await mkdtemp(join(tmpdir(), "browser-cookies-")),
    f = fixture();
  const make = (executionId, profileDir) =>
    createCookieStore(f.Socket, { executionId, profileDir, dataRoot: root });
  const profileDir = join(root, "workspace", "browser-data");
  await mkdir(profileDir, { recursive: true });
  const a = make("first", profileDir);
  try {
    f.set([
      cookie("keep"),
      { ...cookie("session"), session: true },
      { ...cookie("expired"), expires: 1 },
    ]);
    await a.save("ws://browser");
    const [hash] = await readdir(join(root, ".cookies"));
    const path = join(root, ".cookies", hash, "cookies.json");
    assert.equal((await stat(path)).mode & 0o777, 0o600);
    assert.equal((await stat(join(root, ".cookies", hash))).mode & 0o777, 0o700);
    assert.equal(JSON.parse(await readFile(path, "utf8")).length, 1);
    f.set([cookie("deleted-but-not-flushed")]);
    await make("second", profileDir).restore("ws://browser");
    assert.deepEqual(
      f.get().map((c) => c.name),
      ["keep"],
    );
    await assert.rejects(stat(path), { code: "ENOENT" });
    const calls = f.calls.length;
    await a.restore("ws://browser");
    await make("third", join(root, "other", "browser-data")).restore("ws://browser");
    assert.equal(f.calls.length, calls);
    f.set([]);
    await a.save("ws://browser");
    f.set([cookie("keep")]);
    await a.restore("ws://browser");
    assert.deepEqual(f.get(), []);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
test("default-profile journals are deleted together with the execution root", async () => {
  const root = await mkdtemp(join(tmpdir(), "browser-cookies-")),
    f = fixture();
  try {
    f.set([cookie("private")]);
    const store = createCookieStore(f.Socket, {
      executionId: "id",
      profileDir: join(root, "id", "profile"),
      dataRoot: root,
    });
    await store.save("ws://browser");
    assert.equal(
      JSON.parse(await readFile(join(root, "id", "cookies.json"), "utf8")).length,
      1,
    );
    await rm(join(root, "id"), { recursive: true });
    await store.restore("ws://browser");
    assert.deepEqual(f.calls, ["Storage.getCookies"]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("deleting a profile discards its saved cookies instead of restoring removed credentials", async () => {
  const root = await mkdtemp(join(tmpdir(), "browser-cookies-")),
    f = fixture(),
    profileDir = join(root, "workspace", "browser-data");
  try {
    await mkdir(profileDir, { recursive: true });
    f.set([cookie("removed")]);
    await createCookieStore(f.Socket, {
      executionId: "first",
      profileDir,
      dataRoot: root,
    }).save("ws://browser");
    await rm(profileDir, { recursive: true });
    const next = createCookieStore(f.Socket, {
      executionId: "second",
      profileDir,
      dataRoot: root,
    });
    await mkdir(profileDir, { recursive: true }); // Chrome recreates it before the browser hook.
    f.set([]);
    await next.restore("ws://browser");
    assert.deepEqual(f.get(), []);
    assert.deepEqual(f.calls, ["Storage.getCookies"]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("a new browser session drops native session cookies even when no journal survived", async () => {
  const root = await mkdtemp(join(tmpdir(), "browser-cookie-session-")),
    f = fixture(),
    profileDir = join(root, "id", "profile");
  try {
    await mkdir(profileDir, { recursive: true });
    f.set([cookie("persistent"), { ...cookie("session"), session: true }]);
    await createCookieStore(f.Socket, {
      executionId: "id",
      profileDir,
      dataRoot: root,
      sessionBoundary: true,
    }).restore("ws://browser");
    assert.deepEqual(
      f.get().map((c) => c.name),
      ["persistent"],
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("journals preserve Cookie scope/security/partition attributes and reject opaque partitions explicitly", async () => {
  const root = await mkdtemp(join(tmpdir(), "browser-cookie-attributes-")),
    f = fixture(),
    profileDir = join(root, "id", "profile");
  try {
    await mkdir(profileDir, { recursive: true });
    const expected = {
      ...cookie("partitioned"),
      domain: ".example.com",
      path: "/private",
      httpOnly: true,
      secure: true,
      sameSite: "None",
      partitionKey: {
        topLevelSite: "https://top.example",
        hasCrossSiteAncestor: true,
      },
    };
    f.set([expected]);
    const store = createCookieStore(f.Socket, {
      executionId: "id",
      profileDir,
      dataRoot: root,
      sessionBoundary: true,
    });
    await store.save("ws://browser");
    f.set([]);
    await store.restore("ws://browser");
    assert.deepEqual(f.get(), [expected]);
    f.set([{ ...expected, partitionKeyOpaque: true }]);
    await assert.rejects(store.save("ws://browser"), /partition_unsupported/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
