import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { mkdir, mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { createServer, get } from "node:http";
import { createRequire } from "node:module";
import { connect as connectSocket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import WebSocket, { WebSocketServer } from "ws";
import { connectCdp } from "../dist/cdp.js";
import { createGateway } from "../dist/gateway.js";
import { attachStorage, hostRequest } from "../dist/host-client.js";
import {
  BrowserCleanupUnconfirmed,
  VerifiedBrowserCheckpointFailure,
} from "../dist/lifecycle-errors.js";
import { verifyTarget } from "../dist/probe.js";
import { createProfileArchive } from "../dist/profiles.js";

const ids = [
  "11111111-1111-4111-8111-111111111111",
  "22222222-2222-4222-8222-222222222222",
];

test("startup cancellation reaches Profile acquisition and prevents a late launch", async () => {
  let signal,
    launches = 0;
  const f = await fixture(
    () => {
      launches++;
      throw Error("unexpected launch");
    },
    undefined,
    {
      startupTimeoutMs: 30,
      resolvePeerUid: async () => 100001,
      profileStore: {
        async acquire(_profile, _execution, _uid, acquisitionSignal) {
          assert(acquisitionSignal instanceof AbortSignal);
          signal = acquisitionSignal;
          await new Promise((_, reject) => {
            acquisitionSignal.addEventListener(
              "abort",
              () => reject(Error("acquisition_cancelled")),
              { once: true },
            );
          });
        },
      },
    },
  );
  let client;
  try {
    const { wsEndpoint } = await f.gateway.command(
      {
        op: "register",
        executionId: ids[0],
        executorUid: 100001,
        profileId: "cancel/acquire",
      },
      true,
    );
    client = new WebSocket(wsEndpoint);
    client.on("error", () => {});
    await once(client, "close");
    assert.equal(signal?.aborted, true);
    await f.gateway.command({ op: "finish", executionId: ids[0] }, true);
    assert.equal(launches, 0);
  } finally {
    client?.terminate();
    await f.stop();
  }
});
async function fixture(
  launch,
  onMessage = (socket, data) => socket.send(data),
  options = {},
) {
  const root = await mkdtemp(join(tmpdir(), "browser-browser-"));
  const upstream = new WebSocketServer({ host: "127.0.0.1", port: 0 });
  await once(upstream, "listening");
  upstream.on("connection", (socket) =>
    socket.on("message", (data) => onMessage(socket, data)),
  );
  const endpoint = `ws://127.0.0.1:${upstream.address().port}`;
  const gateway = await createGateway({
    ...options,
    WebSocket,
    WebSocketServer,
    port: 0,
    dataRoot: join(root, "data"),
    socketPath: join(root, "control.sock"),
    healthy: async () => true,
    launch: (args) => launch({ ...args, endpoint }),
  });
  return {
    root,
    gateway,
    upstream,
    async stop() {
      try {
        await gateway.stop();
      } finally {
        for (const peer of upstream.clients) peer.terminate();
        await new Promise((resolve) => upstream.close(resolve));
        await rm(root, { recursive: true, force: true });
      }
    },
  };
}
async function connect(endpoint) {
  const socket = new WebSocket(endpoint);
  socket.on("error", () => {});
  await once(socket, "open");
  const reply = once(socket, "message");
  socket.send("hi");
  assert.equal(String((await reply)[0]), "hi");
  return socket;
}

test("file transfer admission stays occupied for slow clients and finish cancels them", {
  timeout: 10000,
}, async () => {
  const f = await fixture(async () => {
    throw Error("must not launch");
  });
  const clients = [];
  try {
    const { wsEndpoint } = await f.gateway.command({
      op: "register",
      executionId: ids[0],
    });
    const url = wsEndpoint.replace(/^ws:/, "http:") + "/files/large.bin";
    assert.equal(
      (await fetch(url, { method: "PUT", body: Buffer.alloc(16 * 1024 * 1024) }))
        .status,
      201,
    );
    for (let i = 0; i < 16; i++) {
      const response = await new Promise((resolve, reject) => {
        const request = get(url, { agent: false }, resolve);
        clients.push(request);
        request.on("error", reject);
      });
      response.on("error", () => {});
      response.pause();
      assert.equal(response.statusCode, 200);
      clients.push(response);
    }
    const denied = await fetch(url);
    assert.equal(denied.status, 429);
    await denied.arrayBuffer();
    const closed = clients
      .filter((client) => "statusCode" in client)
      .map((response) => new Promise((resolve) => response.once("close", resolve)));
    await f.gateway.command({ op: "finish", executionId: ids[0] }, true);
    for (const response of clients.filter((client) => "statusCode" in client))
      response.resume();
    await Promise.all(closed);
    for (const response of clients.filter((client) => "statusCode" in client))
      assert.equal(response.complete, false);
    const next = await f.gateway.command({ op: "register", executionId: ids[1] });
    assert.equal(
      (await fetch(next.wsEndpoint.replace(/^ws:/, "http:") + "/files/missing")).status,
      404,
    );
  } finally {
    for (const client of clients) client.destroy();
    await f.stop();
  }
});

test("generic finalization commits a remote archive before releasing its Profile", async () => {
  const events = [];
  const f = await fixture(
    async ({ endpoint }) => ({
      endpoint,
      close: async () => {
        events.push("close");
      },
    }),
    undefined,
    {
      resolvePeerUid: () => 100001,
      profileStore: {
        async acquire() {
          return {
            profileDir: join(f.root, "native"),
            journalDir: join(f.root, "private"),
            async release() {
              events.push("release");
            },
            async abandon() {},
          };
        },
      },
      profileArchive: {
        async restore() {
          events.push("restore");
        },
        async save() {
          events.push("upload");
          return { used: true, archive: { sha256: "confirmed" } };
        },
      },
    },
  );
  let storage;
  try {
    const registration = {
      executionId: ids[0],
      executorUid: 100001,
      profileId: "generic/profile",
    };
    storage = await attachStorage(join(f.root, "control.sock"), registration, {
      async load() {
        return { seed: null };
      },
      async save() {
        return { target: {} };
      },
      async commit(result) {
        events.push("commit");
        return { confirmedSha: result.archive.sha256 };
      },
    });
    const { wsEndpoint } = await hostRequest(join(f.root, "control.sock"), {
      op: "register",
      ...registration,
      storage: true,
    });
    await connect(wsEndpoint);
    const result = await hostRequest(join(f.root, "control.sock"), {
      op: "finalize",
      executionId: ids[0],
    });
    assert.equal(result.stopped, true);
    assert.deepEqual(events, ["restore", "close", "upload", "commit", "release"]);
  } finally {
    storage?.close();
    await f.stop();
  }
});

test("registration refuses a previous runtime receipt instead of treating state loss as unused", async () => {
  const f = await fixture(async () => {
    throw Error("unused");
  });
  try {
    const directory = join(f.root, "data", ids[0]);
    await mkdir(directory);
    await writeFile(join(directory, "owner.json"), "{}");
    await assert.rejects(
      f.gateway.command({ op: "register", executionId: ids[0] }),
      /recovery_required/,
    );
  } finally {
    await f.stop();
  }
});
test("execution registration rejects invalid UIDs and bounds parallel CDP peers", async () => {
  const f = await fixture(async ({ endpoint }) => ({
    endpoint,
    close: async () => {},
  }));
  const peers = [];
  try {
    for (const uid of [-1, 0, 20001, 100000.5, 2000000000]) {
      await assert.rejects(
        f.gateway.command({
          op: "register",
          executionId: ids[0],
          executorUid: uid,
        }),
        /identity_invalid/,
      );
    }
    const { wsEndpoint } = await f.gateway.command({
      op: "register",
      executionId: ids[0],
    });
    for (let i = 0; i < 16; i++) peers.push(await connect(wsEndpoint));
    const extra = new WebSocket(wsEndpoint);
    await assert.rejects(once(extra, "open"), /403/);
  } finally {
    for (const peer of peers) peer.terminate();
    await f.stop();
  }
});

async function disconnect(socket) {
  const closed = once(socket, "close");
  socket.close();
  await closed;
}

for (const cancellation of [true, false])
  test(`initialization cleanup survives retirement and reconnect (cancel=${cancellation})`, async () => {
    const restoring = Promise.withResolvers(),
      releasing = Promise.withResolvers(),
      allowRelease = Promise.withResolvers();
    let held = false,
      restores = 0,
      releases = 0,
      launches = 0;
    const f = await fixture(
      async ({ endpoint }) => {
        launches++;
        return { endpoint, close: async () => {} };
      },
      undefined,
      {
        resolvePeerUid: () => 100001,
        profileStore: {
          async acquire() {
            if (held) throw Error("browser_profile_busy");
            held = true;
            return {
              profileDir: join(f.root, "native"),
              journalDir: join(f.root, "private"),
              async release() {
                if (releases === 0) {
                  releasing.resolve();
                  await allowRelease.promise;
                }
                held = false;
                releases++;
              },
              async abandon() {
                held = false;
              },
            };
          },
        },
        profileArchive: {
          async restore(_entry, _archive, signal) {
            if (++restores === 1) {
              restoring.resolve();
              if (cancellation)
                await new Promise((_, reject) =>
                  signal.addEventListener(
                    "abort",
                    () => reject(Error("browser_profile_restore_failed")),
                    { once: true },
                  ),
                );
              else throw Error("browser_profile_restore_failed");
            }
          },
          async save() {
            return { used: true, archive: { sha256: "saved" } };
          },
        },
      },
    );
    const registration = (executionId) => ({
      op: "register",
      executionId,
      executorUid: 100001,
      profileId: "shared",
      archive: { seed: null },
    });
    let first, second;
    try {
      const { wsEndpoint } = await f.gateway.command(registration(ids[0]), true);
      first = new WebSocket(wsEndpoint);
      first.on("error", () => {});
      await once(first, "open");
      await restoring.promise;
      first.terminate();
      await releasing.promise;
      if (cancellation) {
        allowRelease.resolve();
        await waitFor(() => releases === 1);
      }
      if (!cancellation) await delay(1100);
      const nextId = cancellation ? ids[1] : ids[0];
      const next = await f.gateway.command(registration(nextId), true);
      second = new WebSocket(next.wsEndpoint);
      second.on("error", () => {});
      await once(second, "open");
      let received = false;
      second.on("message", () => {
        received = true;
      });
      second.send("probe");
      allowRelease.resolve();
      await waitFor(() => received);
      assert.equal(launches, 1);
      assert.equal(restores, 2);
      const saved = await f.gateway.command(
        { op: "profile_finalize", executionId: nextId },
        true,
      );
      await f.gateway.command(
        {
          op: "finish",
          executionId: nextId,
          confirmedSha: saved.archive.sha256,
        },
        true,
      );
    } finally {
      allowRelease.resolve();
      first?.terminate();
      second?.terminate();
      await f.stop();
    }
  });

test("failed initialization releases its Profile lease before another execution acquires it", async () => {
  let held = false,
    releases = 0,
    restores = 0,
    launches = 0;
  const f = await fixture(
    async () => {
      launches++;
      throw Error("launch failed before admission");
    },
    undefined,
    {
      resolvePeerUid: () => 100001,
      profileStore: {
        async acquire() {
          if (held) throw Error("browser_profile_busy");
          held = true;
          return {
            profileDir: join(f.root, "native"),
            journalDir: join(f.root, "private"),
            async release() {
              held = false;
              releases++;
            },
            async abandon() {
              held = false;
            },
          };
        },
      },
      profileArchive: {
        async restore() {
          if (++restores === 1) throw Error("browser_profile_restore_failed");
        },
      },
    },
  );
  try {
    for (const executionId of ids) {
      const { wsEndpoint } = await f.gateway.command(
        {
          op: "register",
          executionId,
          executorUid: 100001,
          profileId: "shared-profile",
          archive: { seed: null },
        },
        true,
      );
      const socket = new WebSocket(wsEndpoint);
      socket.on("error", () => {});
      const [, reason] = await once(socket, "close");
      assert.notEqual(String(reason), "browser_profile_busy");
    }
    assert.equal(restores, 2);
    assert.equal(launches, 1);
    assert.equal(releases, 2);
    assert.equal(held, false);
  } finally {
    await f.stop();
  }
});
async function waitFor(predicate) {
  const until = Date.now() + 3000;
  while (!predicate()) {
    assert.ok(Date.now() < until, "timed out");
    await delay(10);
  }
}
async function storageAdapter(f, request, handler) {
  const socket = connectSocket(join(f.root, "control.sock.auth"));
  socket.on("error", () => {});
  let input = "";
  const ready = Promise.withResolvers();
  socket.on("data", (chunk) => {
    input += chunk;
    while (input.includes("\n")) {
      const end = input.indexOf("\n"),
        frame = JSON.parse(input.slice(0, end));
      input = input.slice(end + 1);
      if (frame.ready) {
        ready.resolve();
        continue;
      }
      if (frame.error) {
        ready.reject(Error(frame.error));
        continue;
      }
      Promise.resolve()
        .then(() => handler(frame.op))
        .then(
          (result) =>
            socket.write(JSON.stringify({ requestId: frame.requestId, result }) + "\n"),
          () =>
            socket.write(
              JSON.stringify({
                requestId: frame.requestId,
                error: "browser_profile_storage_unavailable",
              }) + "\n",
            ),
        );
    }
  });
  socket.once("connect", () =>
    socket.write(JSON.stringify({ op: "storage_attach", ...request }) + "\n"),
  );
  await ready.promise;
  return socket;
}

test("a generic storage adapter is idle until first CDP, reads after acquiring the Profile and keeps delayed startup client-independent", async () => {
  const events = [];
  let head = "S0",
    release,
    launched = false;
  const grant = new Promise((resolve) => {
    release = resolve;
  });
  const f = await fixture(
    async ({ endpoint }) => {
      events.push("launch");
      launched = true;
      return {
        endpoint,
        close: async () => {
          events.push("close");
        },
      };
    },
    undefined,
    {
      resolvePeerUid: () => 100001,
      profileStore: {
        async acquire() {
          events.push("lock");
          head = "S1";
          return {
            profileDir: join(f.root, "native"),
            journalDir: join(f.root, "private"),
            async release() {
              events.push("release");
            },
            async abandon() {},
          };
        },
      },
      profileArchive: {
        async restore(entry, archive) {
          events.push("restore:" + archive.seed.sha256);
          entry.previousSha256 = archive.seed.sha256;
        },
        async save(entry) {
          return {
            used: true,
            previousSha256: entry.previousSha256,
            archive: { sha256: "confirmed" },
          };
        },
      },
    },
  );
  let adapter, transient, peer;
  const request = {
    executionId: ids[0],
    executorUid: 100001,
    profileId: "independent-harness/profile",
  };
  try {
    adapter = await storageAdapter(f, request, async (op) => {
      events.push("authorize:" + op);
      if (op === "load") {
        await grant;
        return { seed: { sha256: head } };
      }
      return { target: {} };
    });
    const endpoint = await hostRequest(join(f.root, "control.sock"), {
      op: "register",
      ...request,
      storage: true,
    });
    assert.equal(endpoint.storage, true);
    assert.deepEqual(events, []);
    transient = await storageAdapter(f, request, () => {
      events.push("transient");
      return new Promise(() => {});
    });
    const pending = connect(endpoint.wsEndpoint);
    await waitFor(() => events.includes("transient"));
    transient.destroy();
    await waitFor(() => events.includes("authorize:load"));
    await delay(100);
    assert.equal(launched, false);
    const other = await hostRequest(join(f.root, "control.sock"), {
      op: "register",
      executionId: ids[1],
      executorUid: 100002,
    });
    assert.ok(other.wsEndpoint);
    await hostRequest(join(f.root, "control.sock"), {
      op: "finish",
      executionId: ids[1],
    });
    release();
    peer = await pending;
    assert.deepEqual(events.filter((event) => event !== "transient").slice(0, 4), [
      "lock",
      "authorize:load",
      "restore:S1",
      "launch",
    ]);
    const saved = await hostRequest(join(f.root, "control.sock"), {
      op: "profile_finalize",
      executionId: ids[0],
    });
    assert.equal(saved.previousSha256, "S1");
    assert.ok(events.indexOf("close") < events.indexOf("authorize:save"));
    await assert.rejects(
      hostRequest(join(f.root, "control.sock"), {
        op: "finish",
        executionId: ids[0],
      }),
      /save_unconfirmed/,
    );
    await hostRequest(join(f.root, "control.sock"), {
      op: "finish",
      executionId: ids[0],
      confirmedSha: "confirmed",
    });
  } finally {
    release();
    peer?.terminate();
    transient?.destroy();
    adapter?.destroy();
    await f.stop();
  }
});

test("an unused remotely configured execution never calls its storage adapter", async () => {
  let calls = 0;
  const f = await fixture(
    async () => {
      throw Error("unused browser");
    },
    undefined,
    {
      profileStore: {
        async acquire() {
          throw Error("unused Profile");
        },
      },
    },
  );
  let adapter;
  try {
    adapter = await storageAdapter(
      f,
      {
        executionId: ids[0],
        executorUid: 100001,
        profileId: "any-harness/profile",
      },
      () => {
        calls++;
        throw Error("must not access storage");
      },
    );
    assert.deepEqual(
      await hostRequest(join(f.root, "control.sock"), {
        op: "profile_finalize",
        executionId: ids[0],
      }),
      { used: false },
    );
    await hostRequest(join(f.root, "control.sock"), {
      op: "finish",
      executionId: ids[0],
    });
    assert.equal(calls, 0);
  } finally {
    adapter?.destroy();
    await f.stop();
  }
});

test("cleanup storage attachment does not recreate a lost execution as an unused browser", async () => {
  const f = await fixture(async () => {
    throw Error("must not launch");
  });
  let adapter;
  try {
    adapter = await storageAdapter(
      f,
      {
        executionId: ids[0],
        executorUid: 100001,
        profileId: "generic/profile",
        existingOnly: true,
      },
      () => {
        throw Error("must not access storage");
      },
    );
    assert.deepEqual(
      await hostRequest(join(f.root, "control.sock"), {
        op: "profile_finalize",
        executionId: ids[0],
      }),
      { status: "absent" },
    );
  } finally {
    adapter?.destroy();
    await f.stop();
  }
});

test("temporary cleanup skips storage while persistent identity mismatches remain fenced", async () => {
  const f = await fixture(async () => {
    throw Error("must not launch");
  });
  const path = join(f.root, "control.sock");
  try {
    await hostRequest(path, {
      op: "register",
      executionId: ids[0],
      executorUid: 100001,
    });
    await assert.rejects(
      hostRequest(path, {
        op: "storage_attach",
        executionId: ids[0],
        executorUid: 100001,
        profileId: "tenant/profile",
      }),
      /binding_changed/,
    );
    await hostRequest(path, {
      op: "storage_attach",
      executionId: ids[0],
      executorUid: 100001,
      profileId: "tenant/profile",
      existingOnly: true,
    });
    assert.deepEqual(
      await hostRequest(path, { op: "profile_finalize", executionId: ids[0] }),
      { used: false },
    );
    await hostRequest(path, { op: "finish", executionId: ids[0] });
    await hostRequest(path, {
      op: "register",
      executionId: ids[1],
      executorUid: 100002,
      profileId: "persistent",
      storage: true,
    });
    for (const change of [{ profileId: "foreign" }, { executorUid: 100001 }])
      await assert.rejects(
        hostRequest(path, {
          op: "storage_attach",
          executionId: ids[1],
          executorUid: 100002,
          profileId: "persistent",
          existingOnly: true,
          ...change,
        }),
        /binding_changed/,
      );
    await hostRequest(path, { op: "finish", executionId: ids[1] });
  } finally {
    await f.stop();
  }
});

test("first CDP startup reports its own deadline before the client startup budget and never launches on a stalled grant", async () => {
  let calls = 0,
    release;
  const grant = new Promise((resolve) => {
    release = resolve;
  });
  const f = await fixture(
    async () => {
      calls++;
      throw Error("must not launch");
    },
    undefined,
    {
      startupTimeoutMs: 80,
      resolvePeerUid: () => 100001,
      profileStore: {
        async acquire() {
          return {
            profileDir: join(f.root, "native"),
            journalDir: join(f.root, "private"),
            async release() {},
            async abandon() {},
          };
        },
      },
    },
  );
  let adapter;
  try {
    const request = {
      executionId: ids[0],
      executorUid: 100001,
      profileId: "generic/profile",
    };
    adapter = await storageAdapter(f, request, () => grant);
    const { wsEndpoint } = await hostRequest(join(f.root, "control.sock"), {
      op: "register",
      ...request,
      storage: true,
    });
    const peer = new WebSocket(wsEndpoint);
    peer.on("error", () => {});
    const [code, reason] = await once(peer, "close");
    assert.equal(code, 1011);
    assert.equal(String(reason), "browser_profile_startup_timeout");
    assert.equal(calls, 0);
    await hostRequest(join(f.root, "control.sock"), {
      op: "finish",
      executionId: ids[0],
    });
  } finally {
    release({});
    adapter?.destroy();
    await f.stop();
  }
});

for (const fault of ["missing", "rejected"])
  test(`a ${fault} optional storage adapter fails only CDP without launching a blank Profile`, async () => {
    let launches = 0,
      releases = 0;
    const f = await fixture(
      async () => {
        launches++;
        throw Error("must not launch");
      },
      undefined,
      {
        resolvePeerUid: () => 100001,
        profileStore: {
          async acquire() {
            return {
              profileDir: join(f.root, "native"),
              journalDir: join(f.root, "private"),
              async release() {
                releases++;
              },
              async abandon() {},
            };
          },
        },
      },
    );
    let adapter;
    const request = {
      executionId: ids[0],
      executorUid: 100001,
      profileId: "any-harness/profile",
    };
    try {
      if (fault === "rejected")
        adapter = await storageAdapter(f, request, () => {
          throw Error("private backend error");
        });
      const { wsEndpoint } = await hostRequest(join(f.root, "control.sock"), {
        op: "register",
        ...request,
        storage: true,
      });
      const peer = new WebSocket(wsEndpoint);
      peer.on("error", () => {});
      const [code, reason] = await once(peer, "close");
      assert.equal(code, 1011);
      assert.equal(String(reason), "browser_profile_storage_unavailable");
      await hostRequest(join(f.root, "control.sock"), {
        op: "finish",
        executionId: ids[0],
      });
      assert.equal(launches, 0);
      assert.equal(releases, 1);
    } finally {
      adapter?.destroy();
      await f.stop();
    }
  });

test("unconfirmed launch cleanup fences reconnect and explicit registration while shutdown closes listeners", async () => {
  let launches = 0;
  const f = await fixture(async () => {
    launches++;
    throw new BrowserCleanupUnconfirmed(Error("private diagnostic"));
  });
  try {
    const { wsEndpoint } = await f.gateway.command({
      op: "register",
      executionId: ids[0],
    });
    for (let i = 0; i < 2; i++) {
      const peer = new WebSocket(wsEndpoint);
      peer.on("error", () => {});
      await once(peer, "close");
    }
    assert.equal(launches, 1);
    await assert.rejects(
      f.gateway.command({ op: "quiesce", executionId: ids[0] }),
      /browser_cleanup_unconfirmed/,
    );
    await assert.rejects(
      f.gateway.command({ op: "register", executionId: ids[0] }),
      /browser_cleanup_unconfirmed/,
    );
    await assert.rejects(f.gateway.stop(), /browser_lifecycle_failed/);
    await assert.rejects(fetch(`http://127.0.0.1:${f.gateway.port}/health`));
    assert.equal(launches, 1);
  } finally {
    for (const peer of f.upstream.clients) peer.terminate();
    await new Promise((resolve) => f.upstream.close(resolve));
    await rm(f.root, { recursive: true, force: true });
  }
});

test("a cancelled successor preserves the previous failed checkpoint confirmation", async () => {
  let launches = 0,
    retired = false;
  const f = await fixture(async ({ endpoint, signal }) => {
    if (++launches === 1)
      return {
        endpoint,
        close: async () => {
          retired = true;
          throw new VerifiedBrowserCheckpointFailure(Error("checkpoint failed"));
        },
      };
    await new Promise((resolve) =>
      signal.addEventListener("abort", resolve, { once: true }),
    );
    throw Error("browser_launch_cancelled");
  });
  let successor;
  try {
    const { wsEndpoint } = await f.gateway.command({
      op: "register",
      executionId: ids[0],
    });
    await disconnect(await connect(wsEndpoint));
    await waitFor(() => retired);
    successor = new WebSocket(wsEndpoint);
    successor.on("error", () => {});
    await waitFor(() => launches === 2);
    await assert.rejects(
      f.gateway.command({ op: "quiesce", executionId: ids[0] }),
      /browser_cookie_save_failed/,
    );
    await assert.rejects(f.gateway.stop(), /browser_lifecycle_failed/);
    await assert.rejects(fetch(`http://127.0.0.1:${f.gateway.port}/health`));
  } finally {
    successor?.terminate();
    await f.gateway.stop().catch(() => {});
    for (const peer of f.upstream.clients) peer.terminate();
    await new Promise((resolve) => f.upstream.close(resolve));
    await rm(f.root, { recursive: true, force: true });
  }
});

test("a Browserless timeout during idle retention invalidates the launch before reconnect", async () => {
  let launched = 0,
    closed = 0;
  const expired = Promise.withResolvers();
  const f = await fixture(
    async ({ endpoint }) => ({
      endpoint,
      closed: ++launched === 1 ? expired.promise : new Promise(() => {}),
      close: async () => {
        closed++;
      },
    }),
    undefined,
    {},
  );
  let peer;
  try {
    const entry = await f.gateway.command({
      op: "register",
      executionId: ids[0],
      workspace: join(f.root, "first"),
    });
    await disconnect(await connect(entry.wsEndpoint));
    expired.resolve();
    await waitFor(() => closed === 1);
    peer = await connect(entry.wsEndpoint);
    assert.equal(launched, 2);
  } finally {
    peer?.terminate();
    await f.stop();
  }
});

test("an upstream crash with attached clients permits a fresh browser immediately", async () => {
  let launched = 0,
    closed = 0;
  const f = await fixture(
    async ({ endpoint }) => {
      launched++;
      return {
        endpoint,
        close: async () => {
          closed++;
        },
      };
    },
    undefined,
    {},
  );
  let peer;
  try {
    const entry = await f.gateway.command({
      op: "register",
      executionId: ids[0],
      workspace: join(f.root, "first"),
    });
    peer = await connect(entry.wsEndpoint);
    const lost = once(peer, "close");
    for (const upstream of f.upstream.clients) upstream.terminate();
    await lost;
    peer = await connect(entry.wsEndpoint);
    assert.equal(launched, 2);
    assert.equal(closed, 1);
  } finally {
    peer?.terminate();
    await f.stop();
  }
});

test("a stale retained endpoint fails once and a later connection starts fresh without replay", async () => {
  let launched = 0,
    closed = 0,
    stale = false,
    messages = 0;
  const f = await fixture(
    async ({ endpoint }) => {
      const first = ++launched === 1;
      return {
        get endpoint() {
          return first && stale ? "ws://127.0.0.1:0" : endpoint;
        },
        close: async () => {
          closed++;
        },
      };
    },
    (socket, data) => {
      messages++;
      socket.send(data);
    },
    {},
  );
  let peer;
  try {
    const entry = await f.gateway.command({
      op: "register",
      executionId: ids[0],
      workspace: join(f.root, "first"),
    });
    await disconnect(await connect(entry.wsEndpoint));
    stale = true;
    const failed = new WebSocket(entry.wsEndpoint);
    failed.on("error", () => {});
    const ended = once(failed, "close");
    await once(failed, "open");
    failed.send("must-not-replay");
    await ended;
    assert.equal(launched, 1);
    assert.equal(messages, 1);
    peer = await connect(entry.wsEndpoint);
    assert.equal(launched, 2);
    assert.equal(closed, 1);
    assert.equal(messages, 2);
  } finally {
    peer?.terminate();
    await f.stop();
  }
});

test("unreachable endpoints fail without an automatic handshake retry", async () => {
  let launched = 0;
  const f = await fixture(
    async () => {
      launched++;
      return { endpoint: "ws://127.0.0.1:0", close: async () => {} };
    },
    undefined,
    {},
  );
  try {
    const entry = await f.gateway.command({
      op: "register",
      executionId: ids[0],
      workspace: join(f.root, "first"),
    });
    const peer = new WebSocket(entry.wsEndpoint);
    peer.on("error", () => {});
    await once(peer, "close");
    assert.equal(launched, 1);
  } finally {
    await f.stop();
  }
});

test("a disconnected workspace startup is cancelled after the transport grace", async () => {
  let launched = 0,
    closed = 0,
    signal;
  const ready = Promise.withResolvers();
  const f = await fixture(
    async ({ endpoint, signal: launchSignal }) => {
      launched++;
      signal = launchSignal;
      await ready.promise;
      return {
        endpoint,
        close: async () => {
          closed++;
        },
      };
    },
    undefined,
    {},
  );
  let peer;
  try {
    const entry = await f.gateway.command({
      op: "register",
      executionId: ids[0],
      workspace: join(f.root, "first"),
    });
    peer = new WebSocket(entry.wsEndpoint);
    peer.on("error", () => {});
    await once(peer, "open");
    await waitFor(() => launched === 1);
    await disconnect(peer);
    await waitFor(() => signal.aborted);
    ready.resolve();
    await delay(20);
    peer = await connect(entry.wsEndpoint);
    assert.equal(launched, 2);
    assert.equal(closed, 1);
  } finally {
    ready.resolve();
    peer?.terminate();
    await f.stop();
  }
});

for (const queuedFirst of [false, true])
  test(`active work preempts an idle workspace at capacity one (already queued=${queuedFirst})`, async () => {
    let active = 0,
      launched = 0,
      closed = 0;
    const slot = Promise.withResolvers();
    const f = await fixture(
      async ({ endpoint }) => {
        launched++;
        if (active) await slot.promise;
        assert.equal(
          active,
          0,
          "profile shutdown must complete before the slot is reused",
        );
        active++;
        return {
          endpoint,
          close: async () => {
            active--;
            closed++;
            slot.resolve();
          },
        };
      },
      undefined,
      { maxBrowsers: 1 },
    );
    let first, second;
    try {
      const a = await f.gateway.command({
        op: "register",
        executionId: ids[0],
        workspace: join(f.root, "first"),
      });
      const b = await f.gateway.command({
        op: "register",
        executionId: ids[1],
        workspace: join(f.root, "second"),
      });
      first = await connect(a.wsEndpoint);
      if (!queuedFirst) await disconnect(first);
      const connecting = connect(b.wsEndpoint);
      if (queuedFirst) {
        await waitFor(() => launched === 2);
        assert.equal(closed, 0, "never evict an attached browser");
        await disconnect(first);
      }
      second = await connecting;
      assert.equal(launched, 2);
      assert.equal(closed, 1);
      assert.equal(active, 1);
    } finally {
      slot.resolve();
      first?.terminate();
      second?.terminate();
      await f.stop();
    }
  });

test("legacy registration reuses its execution browser without storing credentials in the caller workspace", async () => {
  let launched = 0,
    closed = 0;
  const profiles = [];
  const f = await fixture(async ({ profileDir, endpoint }) => {
    launched++;
    profiles.push(profileDir);
    return {
      endpoint,
      close: async () => {
        closed++;
      },
    };
  });
  try {
    const a = await f.gateway.command({
      op: "register",
      executionId: ids[0],
      workspace: join(f.root, "first"),
    });
    const b = await f.gateway.command({
      op: "register",
      executionId: ids[1],
      workspace: join(f.root, "project", "1"),
    });
    assert.notEqual(a.wsEndpoint, b.wsEndpoint);
    assert.equal((await stat(join(f.root, "control.sock"))).mode & 0o777, 0o600);
    await disconnect(await connect(a.wsEndpoint));
    await disconnect(await connect(a.wsEndpoint));
    assert.equal(launched, 1);
    assert.equal(profiles[0], join(f.root, "data", ids[0], "profile"));
    await delay(1200);
    assert.equal(closed, 1);
    const peer = await connect(b.wsEndpoint);
    await disconnect(peer);
    assert.equal(profiles[1], join(f.root, "data", ids[1], "profile"));
    await f.gateway.command({ op: "finish", executionId: ids[0] });
    await assert.rejects(
      f.gateway.command({ op: "register", executionId: ids[0] }),
      /revoked/,
    );
  } finally {
    await f.stop();
  }
});

test("a held peer retains state inside one Run while a second Run starts an independent browser", async () => {
  let uid = 100001,
    launched = 0,
    closed = 0;
  const pages = [];
  const f = await fixture(
    async ({ endpoint }) => {
      launched++;
      const page = { loggedIn: false };
      pages.push(page);
      return {
        endpoint,
        close: async () => {
          closed++;
        },
      };
    },
    (socket, data) => socket.send(data),
    { resolvePeerUid: () => uid },
  );
  let first, second;
  try {
    const a = await f.gateway.command({
      op: "register",
      executionId: ids[0],
      executorUid: 100001,
      workspace: join(f.root, "shared"),
    });
    first = await connect(a.wsEndpoint);
    pages[0].loggedIn = true;
    await delay(1200);
    assert.equal(closed, 0);
    const b = await f.gateway.command({
      op: "register",
      executionId: ids[1],
      executorUid: 100002,
      workspace: join(f.root, "shared"),
    });
    assert.notEqual(a.wsEndpoint, b.wsEndpoint);
    uid = 100002;
    second = await connect(b.wsEndpoint);
    assert.equal(launched, 2);
    assert.equal(pages[0].loggedIn, true);
    assert.equal(pages[1].loggedIn, false);
    await disconnect(first);
    first = null;
    await disconnect(second);
    second = null;
    await waitFor(() => closed === 2);
  } finally {
    first?.terminate();
    second?.terminate();
    await f.stop();
  }
});

test("reconnecting while an image-owned browser closes waits and opens a fresh instance", async () => {
  let launched = 0,
    release;
  const closing = new Promise((resolve) => {
    release = resolve;
  });
  const f = await fixture(async ({ endpoint }) => {
    const number = ++launched;
    return {
      endpoint,
      close: async () => {
        if (number === 1) await closing;
      },
    };
  });
  let peer;
  try {
    const entry = await f.gateway.command({
      op: "register",
      executionId: ids[0],
      workspace: join(f.root, "first"),
    });
    await disconnect(await connect(entry.wsEndpoint));
    await delay(1100);
    const connecting = connect(entry.wsEndpoint);
    await delay(20);
    assert.equal(launched, 1);
    release();
    peer = await connecting;
    assert.equal(launched, 2);
  } finally {
    release();
    peer?.terminate();
    await f.stop();
  }
});

test("disconnect during pending launch preserves shutdown until reconnect can reuse the profile", async () => {
  let launched = 0,
    releaseLaunch,
    releaseClose,
    closingStarted;
  const launchReady = new Promise((resolve) => {
    releaseLaunch = resolve;
  });
  const closeReady = new Promise((resolve) => {
    releaseClose = resolve;
  });
  const closing = new Promise((resolve) => {
    closingStarted = resolve;
  });
  const f = await fixture(async ({ endpoint }) => {
    const first = ++launched === 1;
    if (first) await launchReady;
    return {
      endpoint,
      close: async () => {
        if (first) {
          closingStarted();
          await closeReady;
        }
      },
    };
  });
  let peer;
  try {
    const entry = await f.gateway.command({
      op: "register",
      executionId: ids[0],
      workspace: join(f.root, "first"),
    });
    const first = new WebSocket(entry.wsEndpoint);
    first.on("error", () => {});
    await once(first, "open");
    await disconnect(first);
    await delay(1100);
    releaseLaunch();
    await closing;
    // The disconnected client's late launch callback must not replace the pending shutdown.
    await delay(1100);
    const connecting = connect(entry.wsEndpoint);
    await delay(50);
    assert.equal(launched, 1);
    releaseClose();
    peer = await connecting;
    assert.equal(launched, 2);
  } finally {
    releaseLaunch();
    releaseClose();
    peer?.terminate();
    await f.stop();
  }
});

test("a disconnected failed launch does not poison later workspace connections", async () => {
  let launched = 0,
    rejectLaunch;
  const pending = new Promise((_, reject) => {
    rejectLaunch = reject;
  });
  const f = await fixture(async ({ endpoint }) => {
    if (++launched === 1) await pending;
    return { endpoint, close: async () => {} };
  });
  let peer;
  try {
    const entry = await f.gateway.command({
      op: "register",
      executionId: ids[0],
      workspace: join(f.root, "first"),
    });
    const first = new WebSocket(entry.wsEndpoint);
    first.on("error", () => {});
    await once(first, "open");
    await disconnect(first);
    await delay(1100);
    rejectLaunch(Error("queued launch failed"));
    await delay(20);
    peer = await connect(entry.wsEndpoint);
    assert.equal(launched, 2);
  } finally {
    peer?.terminate();
    await f.stop();
  }
});

test("a failed old launch cannot clear a reconnect already waiting for shutdown", async () => {
  let launched = 0,
    rejectLaunch;
  const pending = new Promise((_, reject) => {
    rejectLaunch = reject;
  });
  const f = await fixture(async ({ endpoint }) => {
    if (++launched === 1) await pending;
    return { endpoint, close: async () => {} };
  });
  let peer, second;
  try {
    const entry = await f.gateway.command({
      op: "register",
      executionId: ids[0],
      workspace: join(f.root, "first"),
    });
    const first = new WebSocket(entry.wsEndpoint);
    first.on("error", () => {});
    await once(first, "open");
    await disconnect(first);
    await delay(1100);
    peer = new WebSocket(entry.wsEndpoint);
    peer.on("error", () => {});
    await once(peer, "open");
    const reply = once(peer, "message");
    peer.send("hi");
    rejectLaunch(Error("queued launch failed"));
    assert.equal(String((await reply)[0]), "hi");
    second = await connect(entry.wsEndpoint);
    assert.equal(launched, 2);
  } finally {
    peer?.terminate();
    second?.terminate();
    await f.stop();
  }
});

test("a queued disconnected launch preserves an earlier browser close failure", async () => {
  const { execFileSync } = await import("node:child_process");
  const root = await mkdtemp(join(tmpdir(), "browser-browser-close-failure-"));
  // Bound this fault fixture and prove failed cleanup still closes service listeners.
  const source = `
    import assert from 'node:assert/strict';
    import { createRequire } from 'node:module';
    import { once } from 'node:events';
    import { setTimeout as delay } from 'node:timers/promises';
    import { join } from 'node:path';
    import { createGateway } from ${JSON.stringify(new URL("../dist/gateway.js", import.meta.url).href)};
    const WebSocket = createRequire(${JSON.stringify(import.meta.url)})(${JSON.stringify(createRequire(new URL("../package.json", import.meta.url).pathname).resolve("ws"))});
    const root = ${JSON.stringify(root)};
    let launched = 0, rejectClose;
    const closing = new Promise((_, reject) => { rejectClose = reject; });
    const upstream = new WebSocket.WebSocketServer({ host: '127.0.0.1', port: 0 });
    await once(upstream, 'listening');
    const gateway = await createGateway({
      WebSocket, WebSocketServer: WebSocket.WebSocketServer, port: 0,
      dataRoot: root, socketPath: join(root, 'control.sock'), healthy: async () => true,
      launch: async () => {
        launched++;
        return { endpoint: 'ws://127.0.0.1:' + upstream.address().port, close: () => closing };
      },
    });
    const entry = await gateway.command({ op: 'register', executionId: ${JSON.stringify(ids[0])} });
    async function connect() {
      const peer = new WebSocket(entry.wsEndpoint);
      peer.on('error', () => {});
      await once(peer, 'open');
      return peer;
    }
    async function disconnect(peer) {
      const closed = once(peer, 'close');
      peer.close();
      await closed;
    }
    await disconnect(await connect());
    await delay(1100);
    await disconnect(await connect());
    await delay(1100);
    rejectClose(Error('browser close failed'));
    await delay(20);
    await connect();
    await delay(50);
    assert.equal(launched, 1);
    await assert.rejects(gateway.stop(), /browser_lifecycle_failed/);
    await assert.rejects(fetch('http://127.0.0.1:' + gateway.port + '/health'));
    for (const socket of upstream.clients) socket.terminate();
    await new Promise(resolve => upstream.close(resolve));
  `;
  try {
    execFileSync(process.execPath, ["--input-type=module", "--eval", source], {
      timeout: 15000,
      stdio: "pipe",
    });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

for (const code of [
  "browser_extensions_directory_invalid",
  "browser_extensions_directory_unreadable_or_invalid",
  "browser_extensions_scan_busy",
  "browser_extensions_launch_request_too_large",
]) {
  test(`CDP and the startup probe preserve the allowlisted error ${code}`, async () => {
    const f = await fixture(async () => {
      throw Error(code);
    });
    let peer;
    try {
      const { wsEndpoint } = await f.gateway.command({
        op: "register",
        executionId: ids[0],
      });
      peer = new WebSocket(wsEndpoint);
      peer.on("error", () => {});
      const [status, reason] = await once(peer, "close");
      assert.equal(status, 1011);
      assert.equal(String(reason), code);
      await f.gateway.command({ op: "finish", executionId: ids[0] });
      await assert.rejects(verifyTarget(f.gateway, WebSocket), { message: code });
    } finally {
      peer?.terminate();
      await f.stop();
    }
  });
}

test("unlisted extension startup details remain private in CDP and probe errors", async () => {
  const f = await fixture(async () => {
    throw Error("browser_extensions_private_secret");
  });
  let peer;
  try {
    const { wsEndpoint } = await f.gateway.command({
      op: "register",
      executionId: ids[0],
    });
    peer = new WebSocket(wsEndpoint);
    peer.on("error", () => {});
    const [status, reason] = await once(peer, "close");
    assert.equal(status, 1006);
    assert.equal(String(reason), "");
    await f.gateway.command({ op: "finish", executionId: ids[0] });
    await assert.rejects(
      verifyTarget(f.gateway, WebSocket),
      /browser_probe_disconnected/,
    );
  } finally {
    peer?.terminate();
    await f.stop();
  }
});

test("the renderer probe uses the wrapper-owned default profile and removes it after probing", async () => {
  let profile;
  const f = await fixture(
    async ({ endpoint, profileDir }) => {
      profile = profileDir;
      return { endpoint, close: async () => {} };
    },
    (socket, data) => {
      const request = JSON.parse(String(data));
      const result =
        request.method === "Target.createTarget"
          ? { targetId: "target" }
          : request.method === "Target.attachToTarget"
            ? { sessionId: "session" }
            : { data: "x".repeat(100) };
      socket.send(JSON.stringify({ id: request.id, result }));
    },
  );
  try {
    await verifyTarget(f.gateway, WebSocket);
    assert.match(
      profile,
      new RegExp("^" + join(f.root, "data") + "/[a-f0-9-]{36}/profile$"),
    );
    await assert.rejects(stat(profile.replace(/\/profile$/, "")), {
      code: "ENOENT",
    });
  } finally {
    await f.stop();
  }
});

test("probe disconnect aborts a queued launch and permits a fresh reconnect", async () => {
  let launched = 0,
    firstSignal,
    aborted;
  const cancelled = new Promise((resolve) => {
    aborted = resolve;
  });
  const f = await fixture(
    async ({ endpoint, signal }) => {
      if (++launched === 1) {
        firstSignal = signal;
        await new Promise((_, reject) =>
          signal.addEventListener(
            "abort",
            () => {
              aborted();
              reject(Error("launch cancelled"));
            },
            { once: true },
          ),
        );
      }
      assert.equal(signal.aborted, false);
      return { endpoint, close: async () => {} };
    },
    undefined,
    {},
  ); // Probes still use the short timeout in production.
  let peer;
  try {
    const entry = await f.gateway.command({
      op: "register",
      executionId: ids[0],
    });
    const first = new WebSocket(entry.wsEndpoint);
    first.on("error", () => {});
    await once(first, "open");
    await disconnect(first);
    await Promise.race([
      cancelled,
      delay(2000).then(() => {
        throw Error("launch was not cancelled");
      }),
    ]);
    assert.equal(firstSignal.aborted, true);
    peer = await connect(entry.wsEndpoint);
    assert.equal(launched, 2);
  } finally {
    peer?.terminate();
    await f.stop();
  }
});

test("explicit host Profile survives finish, changes UID safely and keeps a stable fingerprint", async () => {
  let uid = 100001;
  const events = [],
    fingerprints = [],
    directories = [];
  const store = {
    async acquire(profileId) {
      const { mkdir } = await import("node:fs/promises");
      const profileDir = join(f.root, "persistent", profileId);
      await mkdir(profileDir, { recursive: true });
      events.push("acquire");
      return {
        profileDir,
        journalDir: join(f.root, "journal"),
        async release() {
          events.push("release");
        },
        async abandon() {},
      };
    },
  };
  const f = await fixture(
    async ({ endpoint, profileDir, fingerprint }) => {
      directories.push(profileDir);
      fingerprints.push(fingerprint);
      await (await import("node:fs/promises")).writeFile(
        join(profileDir, "site-data"),
        "saved",
      );
      return {
        endpoint,
        close: async () => {
          events.push("close");
        },
      };
    },
    undefined,
    { profileStore: store, resolvePeerUid: () => uid },
  );
  try {
    for (const executionId of ids) {
      const request = {
        op: "register",
        executionId,
        executorUid: uid,
        profileId: "generic-profile-a",
      };
      await assert.rejects(f.gateway.command(request), /host_authority_required/);
      const registered = await f.gateway.command(request, true);
      assert.equal(registered.profileId, "generic-profile-a");
      await assert.rejects(
        f.gateway.command({ ...request, profileId: "other" }, true),
        /binding_changed/,
      );
      const before = events.filter((e) => e === "acquire").length;
      await f.gateway.command(
        { op: "endpoint", executionId, profileId: "generic-profile-a" },
        true,
      );
      assert.equal(events.filter((e) => e === "acquire").length, before);
      const peer = await connect(registered.wsEndpoint);
      await f.gateway.command({ op: "finish", executionId }, true);
      peer.terminate();
      assert.ok(events.lastIndexOf("close") < events.lastIndexOf("release"));
      assert.equal(
        await readFile(join(directories.at(-1), "site-data"), "utf8"),
        "saved",
      );
      await assert.rejects(stat(join(f.root, "data", executionId)), {
        code: "ENOENT",
      });
      await assert.rejects(f.gateway.command(request, true), /revoked/);
      uid++;
    }
    assert.equal(directories[0], directories[1]);
    assert.equal(fingerprints[0], fingerprints[1]);
  } finally {
    await f.stop();
  }
});

test("unbound execution stays temporary and never acquires persistent storage", async () => {
  let profileDir;
  const f = await fixture(
    async ({ endpoint, profileDir: dir }) => {
      profileDir = dir;
      const { mkdir, writeFile } = await import("node:fs/promises");
      await mkdir(dir, { recursive: true });
      await writeFile(join(dir, "temporary"), "x");
      return { endpoint, close: async () => {} };
    },
    undefined,
    {
      resolvePeerUid: () => 100001,
      profileStore: {
        async acquire() {
          throw Error("temporary execution must not acquire a Profile");
        },
      },
    },
  );
  try {
    const entry = await f.gateway.command({
      op: "register",
      executionId: ids[0],
      executorUid: 100001,
    });
    const peer = await connect(entry.wsEndpoint);
    await f.gateway.command({ op: "finish", executionId: ids[0] }, true);
    peer.terminate();
    await assert.rejects(stat(profileDir), { code: "ENOENT" });
  } finally {
    await f.stop();
  }
});

test("remote Profile cleanup requires runtime upload and explicit host publication confirmation", async () => {
  let saves = 0,
    releases = 0,
    allowUpload = false;
  const f = await fixture(
    async ({ endpoint }) => ({ endpoint, close: async () => {} }),
    undefined,
    {
      resolvePeerUid: () => 100001,
      profileStore: {
        async acquire() {
          return {
            profileDir: join(f.root, "persistent"),
            journalDir: join(f.root, "private"),
            async release() {
              releases++;
            },
            async abandon() {},
          };
        },
      },
      profileArchive: {
        async restore() {},
        async save() {
          saves++;
          if (!allowUpload) throw Error("browser_profile_upload_failed");
          return {
            used: true,
            previousSha256: null,
            archive: { sha256: "confirmed" },
          };
        },
      },
    },
  );
  try {
    const request = {
      op: "register",
      executionId: ids[0],
      executorUid: 100001,
      profileId: "generic-profile",
      archive: { target: {} },
    };
    const { wsEndpoint } = await f.gateway.command(request, true);
    const peer = await connect(wsEndpoint);
    await assert.rejects(
      f.gateway.command({ op: "finish", executionId: ids[0] }, true),
      /save_unconfirmed/,
    );
    await assert.rejects(
      f.gateway.command({ op: "profile_finalize", executionId: ids[0] }, true),
      /upload_failed/,
    );
    assert.equal(releases, 0);
    allowUpload = true;
    const result = await f.gateway.command(
      { op: "profile_finalize", executionId: ids[0] },
      true,
    );
    assert.equal(result.archive.sha256, "confirmed");
    await f.gateway.command({ op: "profile_finalize", executionId: ids[0] }, true);
    assert.equal(saves, 2);
    await assert.rejects(f.gateway.command(request, true), /profile_finalizing/);
    await assert.rejects(
      f.gateway.command(
        { op: "finish", executionId: ids[0], confirmedSha: "wrong" },
        true,
      ),
      /save_unconfirmed/,
    );
    await f.gateway.command(
      { op: "finish", executionId: ids[0], confirmedSha: "confirmed" },
      true,
    );
    peer.terminate();
    assert.equal(releases, 1);
  } catch (error) {
    console.error("profile-contract:", error.message);
    throw error;
  } finally {
    await f.stop();
  }
});

test("parallel socket registrations reserve distinct Chrome debug ports", async () => {
  const f = await fixture(async () => {
    throw Error("registration must not launch Chrome");
  });
  const path = join(f.root, "control.sock");
  try {
    await Promise.all(
      ids.map((executionId, i) =>
        hostRequest(path, {
          op: "register",
          executionId,
          executorUid: 100001 + i,
        }),
      ),
    );
    const owners = await Promise.all(
      ids.map(async (id) =>
        JSON.parse(await readFile(join(f.root, "data", id, "owner.json"), "utf8")),
      ),
    );
    assert.notEqual(owners[0].debugPort, owners[1].debugPort);
    await Promise.all(
      ids.map((executionId) => hostRequest(path, { op: "finish", executionId })),
    );
  } finally {
    await f.stop();
  }
});

for (const cancel of [false, true])
  test(`authenticated control socket supports slow remote upload without blocking other executions (disconnect=${cancel})`, async () => {
    let releaseUpload,
      started = false,
      disconnected = false,
      uploads = 0;
    const gate = new Promise((resolve) => {
      releaseUpload = resolve;
    });
    const upload = createServer(async (req, res) => {
      for await (const _chunk of req) {
      }
      uploads++;
      started = true;
      if (uploads === 1) {
        await Promise.race([
          gate,
          new Promise((resolve) =>
            res.once("close", () => {
              disconnected = true;
              resolve();
            }),
          ),
        ]);
      }
      if (!res.destroyed) res.end();
    });
    await new Promise((resolve) => upload.listen(0, "127.0.0.1", resolve));
    const archive = {
      target: {
        url: `http://127.0.0.1:${upload.address().port}/archive`,
        key: "a".repeat(64),
        objectKey: "immutable",
        keyMaterial: { wrapped: true },
      },
    };
    const realArchive = createProfileArchive();
    const f = await fixture(
      async ({ endpoint }) => ({ endpoint, close: async () => {} }),
      undefined,
      {
        controlTimeoutMs: 250,
        profileFinalizeTimeoutMs: 5000,
        resolvePeerUid: () => 100001,
        profileStore: {
          async acquire() {
            const journalDir = join(f.root, "private"),
              profileDir = join(journalDir, "profile");
            await mkdir(profileDir, { recursive: true });
            await writeFile(join(profileDir, "native-data"), "preserved");
            return {
              profileDir,
              journalDir,
              async release() {},
              async abandon() {},
            };
          },
        },
        profileArchive: {
          async restore(entry) {
            entry.previousSha256 = null;
            entry.profileVersion = "Google Chrome 146.0.0.0";
          },
          save: realArchive.save,
        },
      },
    );
    const path = join(f.root, "control.sock"),
      request = {
        op: "register",
        executionId: ids[0],
        executorUid: 100001,
        profileId: "agent-profile",
        archive,
      };
    let raw;
    try {
      const registered = await hostRequest(path, request);
      const peer = await connect(registered.wsEndpoint);
      let saving;
      if (cancel) {
        raw = connectSocket(path + ".auth");
        raw.on("error", () => {});
        await once(raw, "connect");
        raw.write(
          JSON.stringify({ op: "profile_finalize", executionId: ids[0] }) + "\n",
        );
      } else
        saving = hostRequest(
          path,
          { op: "profile_finalize", executionId: ids[0] },
          6000,
        );
      await waitFor(() => started);
      await delay(300); // Exceeds the ordinary socket timeout on the actual .auth listener.
      const other = await hostRequest(path, {
        op: "register",
        executionId: ids[1],
        executorUid: 100002,
      });
      assert.ok(other.wsEndpoint);
      await hostRequest(path, { op: "finish", executionId: ids[1] });
      const sameExecution = hostRequest(path, request);
      const fenced = assert.rejects(sameExecution, /browser_profile_finalizing/);
      if (cancel) {
        raw.destroy();
        await waitFor(() => disconnected);
      } else releaseUpload();
      await fenced;
      if (cancel) {
        await assert.rejects(
          hostRequest(path, {
            op: "finish",
            executionId: ids[0],
            confirmedSha: "unconfirmed",
          }),
          /save_unconfirmed/,
        );
        saving = hostRequest(
          path,
          { op: "profile_finalize", executionId: ids[0] },
          6000,
        );
      }
      const result = await saving;
      assert.match(result.archive.sha256, /^[a-f0-9]{64}$/);
      assert.equal(uploads, cancel ? 2 : 1);
      await hostRequest(path, {
        op: "finish",
        executionId: ids[0],
        confirmedSha: result.archive.sha256,
      });
      peer.terminate();
    } finally {
      releaseUpload();
      raw?.destroy();
      await f.stop();
      upload.closeAllConnections();
      await new Promise((resolve) => upload.close(resolve));
    }
  });

test("execution session retains pages across disconnect, renews, and stops without revoking registration", async () => {
  const { sessionRequest } = await import("../dist/session.js");
  let launched = 0,
    closed = 0;
  const f = await fixture(
    async ({ endpoint }) => {
      launched++;
      return {
        endpoint,
        close: async () => {
          closed++;
        },
      };
    },
    undefined,
    { maxSessionMs: 5000 },
  );
  try {
    const { wsEndpoint } = await f.gateway.command({
      op: "register",
      executionId: ids[0],
    });
    assert.equal((await sessionRequest(wsEndpoint)).state, "unused");
    await assert.rejects(sessionRequest(wsEndpoint, "retain"), /not_live/);
    assert.equal(launched, 0);
    const first = await connect(wsEndpoint);
    const retained = await sessionRequest(wsEndpoint, "retain", 2500);
    assert.equal(retained.state, "retained");
    const duplicate = await sessionRequest(wsEndpoint, "retain", 4000);
    assert.equal(duplicate.expiresAt, retained.expiresAt);
    const renewed = await sessionRequest(wsEndpoint, "renew", 4000);
    assert(renewed.expiresAt >= retained.expiresAt);
    assert(renewed.expiresAt <= retained.hardExpiresAt);
    first.terminate();
    await delay(1150);
    assert.equal(closed, 0);
    const second = await connect(wsEndpoint);
    assert.equal((await sessionRequest(wsEndpoint)).generation, retained.generation);
    assert.equal(launched, 1);
    assert.equal((await sessionRequest(wsEndpoint, "stop")).state, "stopped");
    assert.equal(closed, 1);
    assert.equal((await sessionRequest(wsEndpoint, "stop")).state, "stopped");
    second.terminate();
    const third = await connect(wsEndpoint);
    assert.equal(launched, 2);
    assert.notEqual((await sessionRequest(wsEndpoint)).generation, retained.generation);
    third.terminate();
  } finally {
    await f.stop();
  }
});

test("expired retained sessions close without silently renewing or launching replacements", async () => {
  const { sessionRequest } = await import("../dist/session.js");
  let closed = 0;
  const f = await fixture(
    async ({ endpoint }) => ({
      endpoint,
      close: async () => {
        closed++;
      },
    }),
    undefined,
    { maxSessionMs: 2000 },
  );
  try {
    const { wsEndpoint } = await f.gateway.command({
      op: "register",
      executionId: ids[0],
    });
    const peer = await connect(wsEndpoint);
    await assert.rejects(sessionRequest(wsEndpoint, "renew", 100), /not_retained/);
    await assert.rejects(
      sessionRequest(wsEndpoint, "retain", 2001),
      /duration_invalid/,
    );
    await sessionRequest(wsEndpoint, "retain", 100);
    peer.terminate();
    await delay(160);
    assert.equal((await sessionRequest(wsEndpoint)).state, "expired");
    assert.equal(closed, 1);
    await assert.rejects(sessionRequest(wsEndpoint, "renew", 100), /not_live/);
    await f.gateway.command({ op: "finish", executionId: ids[0] });
    await assert.rejects(sessionRequest(wsEndpoint), /session_failed/);
  } finally {
    await f.stop();
  }
});

test("session controls require the execution kernel UID and cannot address another execution", async () => {
  const { sessionRequest } = await import("../dist/session.js");
  const f = await fixture(
    async () => {
      throw Error("must not launch");
    },
    undefined,
    { resolvePeerUid: async () => 100002 },
  );
  try {
    const { wsEndpoint } = await f.gateway.command(
      { op: "register", executionId: ids[0], executorUid: 100001 },
      true,
    );
    await assert.rejects(sessionRequest(wsEndpoint), /session_failed/);
  } finally {
    await f.stop();
  }
});

test("human takeover cancels a pending idle retirement until control is released", async () => {
  let closed = 0;
  const f = await fixture(
    async ({ endpoint }) => ({
      endpoint,
      close: async () => {
        closed++;
      },
    }),
    (socket, data) => {
      let message;
      try {
        message = JSON.parse(String(data));
      } catch {
        socket.send(data);
        return;
      }
      const result =
        message.method === "Target.getTargets"
          ? {
              targetInfos: [
                { type: "page", targetId: "page", url: "https://example.com/login" },
              ],
            }
          : message.method === "Target.attachToTarget"
            ? { sessionId: "session" }
            : {};
      socket.send(JSON.stringify({ id: message.id, result }));
    },
  );
  try {
    const { wsEndpoint } = await f.gateway.command({
      op: "register",
      executionId: ids[0],
    });
    const peer = await connect(wsEndpoint);
    const disconnected = once(peer, "close");
    peer.close();
    await disconnected;
    await delay(50);
    const lease = await f.gateway.command(
      {
        op: "takeover",
        executionId: ids[0],
        action: {
          op: "acquire",
          actorId: "operator",
          leaseId: "lease",
          automationStopped: true,
        },
      },
      true,
    );
    assert.equal(lease.state, "human");
    await delay(1150);
    assert.equal(closed, 0);
    assert.equal(
      (
        await f.gateway.command(
          { op: "session", executionId: ids[0], action: "status" },
          true,
        )
      ).state,
      "live",
    );
    await f.gateway.command(
      {
        op: "takeover",
        executionId: ids[0],
        action: {
          op: "release",
          actorId: "operator",
          leaseId: "lease",
          epoch: lease.epoch,
          origin: lease.origin,
        },
      },
      true,
    );
    await delay(1150);
    assert.equal(closed, 1);
  } finally {
    await f.stop();
  }
});

test("invalid host JSON values do not crash the gateway subprocess", async () => {
  const { execFileSync } = await import("node:child_process");
  const root = await mkdtemp(join(tmpdir(), "browser-invalid-control-"));
  const source = `
    import assert from 'node:assert/strict';
    import { createRequire } from 'node:module';
    import { createGateway } from ${JSON.stringify(new URL("../dist/gateway.js", import.meta.url).href)};
    import { hostRequest } from ${JSON.stringify(new URL("../dist/host-client.js", import.meta.url).href)};
    const WebSocket = createRequire(${JSON.stringify(import.meta.url)})('ws');
    const socketPath = ${JSON.stringify(join(root, "control.sock"))};
    const gateway = await createGateway({
      WebSocket, WebSocketServer: WebSocket.WebSocketServer, port: 0,
      dataRoot: ${JSON.stringify(join(root, "data"))}, socketPath,
      healthy: async () => true, launch: async () => { throw Error('unused'); },
    });
    try {
      for (const request of [null, [], '中文', 42, true, {}])
        await assert.rejects(hostRequest(socketPath, request), /browser_request_invalid/);
      assert.equal((await fetch('http://127.0.0.1:' + gateway.port + '/health')).status, 204);
    } finally { await gateway.stop(); }
  `;
  try {
    execFileSync(process.execPath, ["--input-type=module", "-e", source], {
      timeout: 10_000,
      stdio: ["ignore", "pipe", "pipe"],
    });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("failed readback restores idle retirement after disconnecting automation", async () => {
  let closed = 0;
  const f = await fixture(
    async ({ endpoint }) => ({
      endpoint,
      close: async () => {
        closed++;
      },
    }),
    (socket, data) => {
      let message;
      try {
        message = JSON.parse(String(data));
      } catch {
        socket.send(data);
        return;
      }
      let result = {};
      if (message.method === "Target.getTargets")
        result = {
          targetInfos: [
            { type: "page", targetId: "page", url: "https://example.com/login" },
          ],
        };
      if (message.method === "Target.attachToTarget") result = { sessionId: "session" };
      socket.send(JSON.stringify({ id: message.id, result }));
    },
  );
  try {
    const { wsEndpoint } = await f.gateway.command({
      op: "register",
      executionId: ids[0],
    });
    const peer = await connect(wsEndpoint);
    await assert.rejects(
      f.gateway.command(
        {
          op: "takeover",
          executionId: ids[0],
          action: { op: "readback", url: "https://other.example/me" },
        },
        true,
      ),
      /origin_mismatch/,
    );
    await delay(1150);
    assert.equal(closed, 1);
    peer.terminate();
  } finally {
    await f.stop();
  }
});

test("application credential binding is unsupported and CDP commands pass through", async () => {
  const f = await fixture(async ({ endpoint }) => ({
    endpoint,
    close: async () => {},
  }));
  try {
    const path = join(f.root, "control.sock");
    await assert.rejects(
      hostRequest(path, { op: "authenticate", executionId: ids[0] }),
      /browser_request_invalid/,
    );
    const { wsEndpoint } = await hostRequest(path, {
      op: "register",
      executionId: ids[0],
    });
    const socket = await connect(wsEndpoint);
    try {
      for (const method of ["Fetch.enable", "Browser.close"]) {
        const request = { id: 1, method };
        const reply = once(socket, "message");
        socket.send(JSON.stringify(request));
        assert.deepEqual(JSON.parse(String((await reply)[0])), request);
      }
    } finally {
      socket.terminate();
    }
  } finally {
    await f.stop();
  }
});

test("revocation capacity fences new identities while existing executions can finish", async () => {
  const f = await fixture(async () => {
    throw Error("must not launch");
  });
  try {
    const live = await f.gateway.command({ op: "register", executionId: ids[0] });
    for (let index = 0; index < 65536; index++)
      await f.gateway.command(
        {
          op: "finish",
          executionId: `aaaaaaaa-aaaa-4aaa-8aaa-${index.toString(16).padStart(12, "0")}`,
        },
        true,
      );
    await assert.rejects(
      f.gateway.command({ op: "register", executionId: ids[1] }),
      /browser_execution_capacity_exceeded/,
    );
    assert.equal(
      (await f.gateway.command({ op: "register", executionId: ids[0] })).wsEndpoint,
      live.wsEndpoint,
    );
    await f.gateway.command({ op: "finish", executionId: ids[0] }, true);
    await assert.rejects(
      f.gateway.command({ op: "register", executionId: ids[0] }),
      /browser_execution_capacity_exceeded/,
    );
    await assert.rejects(
      f.gateway.command({
        op: "register",
        executionId: "aaaaaaaa-aaaa-4aaa-8aaa-000000000000",
      }),
      /browser_execution_revoked/,
    );
  } finally {
    await f.stop();
  }
});

test("a failed finish remains irrevocable when the tombstone budget is full", async () => {
  let releases = 0;
  const f = await fixture(
    async ({ endpoint }) => ({ endpoint, close: async () => {} }),
    undefined,
    {
      resolvePeerUid: () => 100001,
      profileStore: {
        async acquire() {
          return {
            profileDir: join(f.root, "persistent"),
            journalDir: join(f.root, "journal"),
            async release() {
              if (++releases === 1) throw Error("profile cleanup failed");
            },
            async abandon() {},
          };
        },
      },
    },
  );
  const registration = {
    op: "register",
    executionId: ids[0],
    executorUid: 100001,
    profileId: "finish-retry",
  };
  try {
    const { wsEndpoint } = await f.gateway.command(registration, true);
    await connect(wsEndpoint);
    for (let index = 0; index < 65536; index++)
      await f.gateway.command(
        {
          op: "finish",
          executionId: `aaaaaaaa-aaaa-4aaa-8aaa-${index.toString(16).padStart(12, "0")}`,
        },
        true,
      );
    await assert.rejects(
      f.gateway.command({ op: "finish", executionId: ids[0] }, true),
      /profile cleanup failed/,
    );
    await assert.rejects(
      f.gateway.command(registration, true),
      /browser_execution_revoked/,
    );
    await assert.rejects(
      f.gateway.command({ op: "quiesce", executionId: ids[0] }, true),
      /browser_execution_revoked/,
    );
    await f.gateway.command({ op: "finish", executionId: ids[0] }, true);
    assert.equal(releases, 2);
  } finally {
    await f.stop();
  }
});

test("a quiesce already closing cannot reopen access after a concurrent finish fails", async () => {
  const closing = Promise.withResolvers(),
    proceed = Promise.withResolvers();
  let releases = 0;
  const f = await fixture(
    async ({ endpoint }) => ({
      endpoint,
      async close() {
        closing.resolve();
        await proceed.promise;
      },
    }),
    undefined,
    {
      resolvePeerUid: () => 100001,
      profileStore: {
        async acquire() {
          return {
            profileDir: join(f.root, "persistent"),
            journalDir: join(f.root, "journal"),
            async release() {
              if (++releases === 1) throw Error("profile cleanup failed");
            },
            async abandon() {},
          };
        },
      },
    },
  );
  try {
    const { wsEndpoint } = await f.gateway.command(
      {
        op: "register",
        executionId: ids[0],
        executorUid: 100001,
        profileId: "finish-race",
      },
      true,
    );
    await connect(wsEndpoint);
    const quiescing = f.gateway.command({ op: "quiesce", executionId: ids[0] }, true);
    await closing.promise;
    const finishing = assert.rejects(
      f.gateway.command({ op: "finish", executionId: ids[0] }, true),
      /profile cleanup failed/,
    );
    proceed.resolve();
    await assert.rejects(quiescing, /browser_execution_revoked/);
    await finishing;
    const response = await fetch(wsEndpoint.replace(/^ws:/, "http:") + "/session");
    await response.arrayBuffer();
    assert.equal(response.status, 403);
    await f.gateway.command({ op: "finish", executionId: ids[0] }, true);
  } finally {
    proceed.resolve();
    await f.stop();
  }
});

test("Profile preparation is bounded before browser admission and queued work is cancellable", async () => {
  const gate = Promise.withResolvers();
  let acquisitions = 0;
  const f = await fixture(
    async ({ endpoint }) => ({ endpoint, close: async () => {} }),
    undefined,
    {
      maxBrowsers: 1,
      resolvePeerUid: () => 100001,
      profileStore: {
        async acquire(_profile, executionId) {
          acquisitions++;
          await gate.promise;
          const profileDir = join(f.root, "data", executionId, "profile");
          await mkdir(profileDir);
          return {
            profileDir,
            journalDir: join(profileDir, ".."),
            release: async () => {},
            abandon: async () => {},
          };
        },
      },
    },
  );
  const entries = [],
    clients = [];
  try {
    for (let i = 0; i < 4; i++) {
      const executionId = randomUUID();
      entries.push(executionId);
      const { wsEndpoint } = await f.gateway.command(
        { op: "register", executionId, executorUid: 100001, profileId: "bounded/" + i },
        true,
      );
      const client = new WebSocket(wsEndpoint);
      client.on("error", () => {});
      clients.push(client);
      const closed = once(client, "close");
      await once(client, "open");
      if (i === 3) {
        const [code, reason] = await closed;
        assert.equal(code, 1011);
        assert.equal(String(reason), "browser_profile_work_queue_full");
      }
    }
    await waitFor(() => acquisitions > 0);
    await delay(30);
    assert.equal(acquisitions, 1);
    await f.gateway.command({ op: "finish", executionId: entries[1] }, true);
    assert.equal(acquisitions, 1);
    gate.resolve();
    await waitFor(() => acquisitions === 2);
    assert.equal(acquisitions, 2);
    for (const executionId of [entries[0], entries[2]])
      await f.gateway.command({ op: "finish", executionId }, true);
  } finally {
    gate.resolve();
    for (const client of clients) client.terminate();
    for (const executionId of entries)
      await f.gateway.command({ op: "quiesce", executionId }, true).catch(() => {});
    await f.stop();
  }
});

test("the first CDP reply can wait for cold startup while later calls retain their normal timeout", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const launch = Promise.withResolvers();
  const f = await fixture(
    async ({ endpoint }) => {
      await launch.promise;
      return { endpoint, close: async () => {} };
    },
    (socket, data) => {
      const { id, method } = JSON.parse(String(data));
      if (method === "Browser.getVersion")
        socket.send(JSON.stringify({ id, result: { product: "fixture" } }));
    },
  );
  let cdp;
  try {
    const { wsEndpoint } = await f.gateway.command({
      op: "register",
      executionId: ids[0],
    });
    cdp = await connectCdp(wsEndpoint);
    let outcome;
    const first = cdp.call("Browser.getVersion").then(
      (value) => {
        outcome = value;
        return value;
      },
      (error) => {
        outcome = error;
        throw error;
      },
    );
    first.catch(() => {});
    t.mock.timers.tick(15001);
    await new Promise(setImmediate);
    assert.equal(outcome, undefined);
    launch.resolve();
    assert.equal((await first).product, "fixture");
    const later = assert.rejects(cdp.call("Runtime.evaluate"), /browser_cdp_timeout/);
    t.mock.timers.tick(15001);
    await later;
  } finally {
    launch.resolve();
    cdp?.close();
    await f.stop();
    t.mock.timers.reset();
  }
});

test("Profile saves share bounded admission, cancel queued work and keep ordinary control responsive", async () => {
  const gate = Promise.withResolvers();
  let saves = 0;
  const sha = "a".repeat(64);
  const f = await fixture(
    async ({ endpoint }) => ({ endpoint, close: async () => {} }),
    undefined,
    {
      maxBrowsers: 1,
      resolvePeerUid: () => 100001,
      profileStore: {
        async acquire(_profile, executionId) {
          const profileDir = join(f.root, "data", executionId, "profile");
          await mkdir(profileDir);
          return {
            profileDir,
            journalDir: join(profileDir, ".."),
            release: async () => {},
            abandon: async () => {},
          };
        },
      },
      profileArchive: {
        async restore() {},
        async save() {
          saves++;
          await gate.promise;
          return { used: true, archive: { sha256: sha } };
        },
      },
    },
  );
  const entries = [],
    clients = [],
    saving = [];
  const abort = new AbortController();
  try {
    for (let i = 0; i < 3; i++) {
      const executionId = randomUUID();
      entries.push(executionId);
      const { wsEndpoint } = await f.gateway.command(
        {
          op: "register",
          executionId,
          executorUid: 100001,
          profileId: `save/${i}`,
          archive: {},
        },
        true,
      );
      clients.push(await connect(wsEndpoint));
    }
    saving.push(
      f.gateway.command({ op: "profile_finalize", executionId: entries[0] }, true),
    );
    await waitFor(() => saves === 1);
    const cancelled = assert.rejects(
      f.gateway.command(
        { op: "profile_finalize", executionId: entries[1] },
        true,
        false,
        abort.signal,
      ),
      /browser_profile_work_cancelled/,
    );
    saving.push(
      f.gateway.command({ op: "profile_finalize", executionId: entries[2] }, true),
    );
    await delay(30);
    assert.equal(saves, 1);
    const ordinary = randomUUID();
    await hostRequest(join(f.root, "control.sock"), {
      op: "register",
      executionId: ordinary,
    });
    await hostRequest(join(f.root, "control.sock"), {
      op: "finish",
      executionId: ordinary,
    });
    abort.abort();
    await cancelled;
    gate.resolve();
    await Promise.all(saving);
    assert.equal(saves, 2);
    await f.gateway.command({ op: "profile_finalize", executionId: entries[1] }, true);
    for (const executionId of entries)
      await f.gateway.command({ op: "finish", executionId, confirmedSha: sha }, true);
  } finally {
    gate.resolve();
    abort.abort();
    await Promise.allSettled(saving);
    for (const client of clients) client.terminate();
    await f.stop();
  }
});

test("Profile releases share admission with preparation even when its ordinary queue is full", async () => {
  const gate = Promise.withResolvers();
  let releases = 0,
    active = 0,
    peak = 0;
  const f = await fixture(
    async ({ endpoint }) => ({ endpoint, close: async () => {} }),
    undefined,
    {
      maxBrowsers: 1,
      resolvePeerUid: () => 100001,
      profileStore: {
        async acquire(_profile, executionId) {
          active++;
          peak = Math.max(peak, active);
          try {
            const profileDir = join(f.root, "data", executionId, "profile");
            await mkdir(profileDir);
            return {
              profileDir,
              journalDir: join(profileDir, ".."),
              async release() {
                active++;
                peak = Math.max(peak, active);
                releases++;
                try {
                  await gate.promise;
                } finally {
                  active--;
                }
              },
              async abandon() {},
            };
          } finally {
            active--;
          }
        },
      },
    },
  );
  const entries = [],
    clients = [],
    finishing = [];
  try {
    for (let i = 0; i < 3; i++) {
      const executionId = randomUUID();
      entries.push(executionId);
      const { wsEndpoint } = await f.gateway.command(
        { op: "register", executionId, executorUid: 100001, profileId: `release/${i}` },
        true,
      );
      clients.push(await connect(wsEndpoint));
    }
    finishing.push(f.gateway.command({ op: "finish", executionId: entries[0] }, true));
    await waitFor(() => releases === 1);
    for (let i = 3; i < 5; i++) {
      const executionId = randomUUID();
      entries.push(executionId);
      const { wsEndpoint } = await f.gateway.command(
        { op: "register", executionId, executorUid: 100001, profileId: `release/${i}` },
        true,
      );
      const client = new WebSocket(wsEndpoint);
      client.on("error", () => {});
      clients.push(client);
      await once(client, "open");
    }
    finishing.push(f.gateway.command({ op: "finish", executionId: entries[1] }, true));
    finishing.push(f.gateway.command({ op: "finish", executionId: entries[2] }, true));
    await delay(30);
    assert.equal(peak, 1);
    assert.equal(releases, 1);
    gate.resolve();
    await Promise.all(finishing);
    assert.equal(releases, 3);
  } finally {
    gate.resolve();
    await Promise.allSettled(finishing);
    for (const client of clients) client.terminate();
    await f.stop();
  }
});

test("failed Profile restores retain admission for ownership recovery before later preparation", async () => {
  const gate = Promise.withResolvers();
  let active = 0,
    peak = 0,
    restores = 0,
    releases = 0;
  const track = async (work) => {
    active++;
    peak = Math.max(peak, active);
    try {
      return await work();
    } finally {
      active--;
    }
  };
  const f = await fixture(
    async () => {
      throw Error("must not launch");
    },
    undefined,
    {
      maxBrowsers: 1,
      resolvePeerUid: () => 100001,
      profileStore: {
        acquire(_profile, executionId) {
          return track(async () => {
            const profileDir = join(f.root, "data", executionId, "profile");
            await mkdir(profileDir);
            return {
              profileDir,
              journalDir: join(profileDir, ".."),
              release: () =>
                track(async () => {
                  releases++;
                  await gate.promise;
                }),
              async abandon() {},
            };
          });
        },
      },
      profileArchive: {
        restore: () =>
          track(async () => {
            restores++;
            throw Error("browser_profile_restore_failed");
          }),
      },
    },
  );
  const clients = [],
    closed = [];
  try {
    for (let i = 0; i < 3; i++) {
      const { wsEndpoint } = await f.gateway.command(
        {
          op: "register",
          executionId: randomUUID(),
          executorUid: 100001,
          profileId: `failed/${i}`,
          archive: {},
        },
        true,
      );
      const client = new WebSocket(wsEndpoint);
      client.on("error", () => {});
      clients.push(client);
      closed.push(once(client, "close"));
      await once(client, "open");
      if (i === 0) await waitFor(() => releases === 1);
    }
    await delay(30);
    assert.equal(restores, 1);
    assert.equal(peak, 1);
    gate.resolve();
    await Promise.all(closed);
    assert.equal(releases, 3);
    assert.equal(peak, 1);
  } finally {
    gate.resolve();
    for (const client of clients) client.terminate();
    await f.stop();
  }
});
