import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { cp, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createServer, request } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import WebSocket from "ws";
import { createController, validateControllerRecord } from "../dist/controller.js";
import { importExecutionJournal } from "../dist/kubernetes-migration.js";
import { createKubernetesOwnership } from "../dist/kubernetes-ownership.js";
import { createControllerRuntime } from "../dist/network-control.js";

async function fixture(t, extra = {}) {
  const objects = new Map();
  const cleanups = [];
  const state = { requests: 0 };
  let version = 0;
  const server = createServer(async (req, res) => {
    if (!req.url.includes("watch=true")) state.requests++;
    let input = "";
    for await (const chunk of req) input += chunk;
    const body = input ? JSON.parse(input) : undefined;
    if (body?.spec?.renewTime && !/\.[0-9]{6}Z$/.test(body.spec.renewTime)) {
      res
        .writeHead(400)
        .end(
          JSON.stringify({ message: "Lease renewTime requires Kubernetes MicroTime" }),
        );
      return;
    }
    const path = req.url.split("?")[0];
    let previous = objects.get(path);
    const reply = (status, value = {}) =>
      res
        .writeHead(status, { "content-type": "application/json" })
        .end(JSON.stringify(value));
    if (req.method === "GET" && path.endsWith("/leases")) {
      if (req.url.includes("watch=true")) return reply(200);
      return reply(200, {
        items: [...objects.entries()]
          .filter(
            ([key, value]) =>
              key.startsWith(path + "/") &&
              value.metadata.labels["browser-sandbox.io/state"] === "execution",
          )
          .map(([, value]) => value),
        metadata: { resourceVersion: String(version) },
      });
    }
    if (req.method === "GET") {
      await state.beforeGet?.(path);
      if (state.readAfterGet) previous = objects.get(path);
      return reply(previous ? 200 : 404, previous);
    }
    if (req.method === "POST") {
      if (path.endsWith("/secrets") && state.failSecretCreate) {
        state.failSecretCreate = false;
        return reply(503);
      }
      const key = `${path}/${body.metadata.name}`;
      if (objects.has(key)) return reply(409);
      body.metadata.resourceVersion = String(++version);
      objects.set(key, body);
      return reply(201, body);
    }
    if (!previous) return reply(404);
    if (req.method === "PUT") {
      await state.beforeWrite?.(path, body);
      previous = objects.get(path);
      if (body.metadata.resourceVersion !== previous.metadata.resourceVersion)
        return reply(409);
      body.metadata.resourceVersion = String(++version);
      objects.set(path, body);
      return reply(200, body);
    }
    if (req.method === "DELETE") {
      if (path.includes("/leases/") && state.failLeaseDelete) {
        state.failLeaseDelete = false;
        return reply(503);
      }
      if (body.preconditions.resourceVersion !== previous.metadata.resourceVersion)
        return reply(409);
      objects.delete(path);
      return reply(200);
    }
    reply(400);
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(async () => {
    for (const cleanup of cleanups) await cleanup();
    await new Promise((resolve) => {
      server.closeAllConnections();
      server.close(resolve);
    });
  });
  let now = 0;
  const options = {
    namespace: "test",
    installation: "test",
    apiServer: `http://127.0.0.1:${server.address().port}`,
    leaseMs: 15000,
    now: () => now,
    ...extra,
  };
  const ports = [];
  for (let i = 0; i < 2; i++) {
    const listener = createServer();
    await new Promise((resolve) => listener.listen(0, "127.0.0.1", resolve));
    ports.push(listener.address().port);
    await new Promise((resolve) => listener.close(resolve));
  }
  const a = createKubernetesOwnership({
    ...options,
    replica: "a",
    peerOrigin: `http://127.0.0.1:${ports[0]}`,
  });
  const b = createKubernetesOwnership({
    ...options,
    replica: "b",
    peerOrigin: `http://127.0.0.1:${ports[1]}`,
  });
  cleanups.push(async () => {
    await a.close();
    await b.close();
  });
  return {
    a,
    b,
    objects,
    state,
    ports,
    cleanups,
    advance: (ms) => {
      now += ms;
    },
  };
}
const id = "11111111-1111-4111-8111-111111111111";
const other = "22222222-2222-4222-8222-222222222222";

test("conditional ownership selects one replica and fences old journal and Profile writes after takeover", async (t) => {
  const { a, b, advance } = await fixture(t);
  const owners = await Promise.all([a.resolve(id, true), b.resolve(id, true)]);
  assert.equal(owners[0].replica, owners[1].replica);
  const owner = owners[0].replica === "a" ? a : b;
  const peer = owner === a ? b : a;
  await owner.journal.create(id, { phase: "registered", profileId: "profile-1" });
  await owner.acquireProfile(id, "profile-1");
  await peer.resolve(other, true);
  await peer.journal.create(other, { phase: "registered", profileId: "profile-1" });
  await assert.rejects(peer.acquireProfile(other, "profile-1"), /browser_profile_busy/);
  await peer.resolve(id);
  advance(15001);
  const next = await peer.resolve(id);
  assert.equal(next.epoch, 2);
  assert.equal((await peer.journal.read(id)).data.phase, "registered");
  await peer.acquireProfile(id, "profile-1");
  await assert.rejects(
    owner.journal.update(id, 1, { phase: "finished" }),
    /browser_controller_fenced/,
  );
  await assert.rejects(
    owner.releaseProfile(id, "profile-1"),
    /browser_controller_fenced/,
  );
  const record = await peer.journal.update(id, 1, {
    phase: "finished",
    profileId: "profile-1",
  });
  assert.equal(record.revision, 2);
  await peer.releaseProfile(id, "profile-1");
});

test("the previous release's journal imports idempotently without rewriting the source", async (t) => {
  const { a } = await fixture(t);
  const directory = await mkdtemp(join(tmpdir(), "journal-import-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  await cp(
    new URL("../.github/scripts/managed/fixtures/journal-v0.2.0/", import.meta.url),
    directory,
    { recursive: true },
  );
  const filename = join(directory, `${id}.json`);
  const original = await readFile(filename);
  const pending = join(directory, ".pending-33333333-3333-4333-8333-333333333333");
  const pendingOriginal = await readFile(pending);
  const options = {
    directory,
    installationId: "fixture",
    ownership: a,
    validate: validateControllerRecord,
  };
  assert.equal(await importExecutionJournal(options), 1);
  assert.equal(
    (await a.journal.read(id)).data.workerProtocol,
    "browser-sandbox.worker/v1",
  );
  assert.equal(await importExecutionJournal(options), 0);
  assert.deepEqual(await readFile(filename), original);
  assert.deepEqual(await readFile(pending), pendingOriginal);
});

test("corrupt and unsupported migration input preserves every source byte and publishes nothing", async (t) => {
  for (const corrupt of [
    "{",
    JSON.stringify({ protocol: "browser-sandbox.execution/v9" }),
  ]) {
    const { a } = await fixture(t);
    const directory = await mkdtemp(join(tmpdir(), "journal-corrupt-"));
    t.after(() => rm(directory, { recursive: true, force: true }));
    await cp(
      new URL("../.github/scripts/managed/fixtures/journal-v0.2.0/", import.meta.url),
      directory,
      { recursive: true },
    );
    await writeFile(join(directory, `${other}.json`), corrupt);
    const original = await readFile(join(directory, `${id}.json`));
    await assert.rejects(
      importExecutionJournal({
        directory,
        installationId: "fixture",
        ownership: a,
        validate: validateControllerRecord,
      }),
    );
    assert.equal(await a.resolve(id), undefined);
    assert.deepEqual(await readFile(join(directory, `${id}.json`)), original);
    assert.equal(await readFile(join(directory, `${other}.json`), "utf8"), corrupt);
  }
});

test("host and HTTP requests forward once, and the same capability URL survives ownership transfer", async (t) => {
  const { a, b, ports, advance, cleanups } = await fixture(t);
  const controllers = [];
  const runtimes = [];
  cleanups.unshift(async () => {
    for (const runtime of runtimes) runtime.close();
    for (const controller of controllers) await controller.stop({ finalize: false });
  });
  for (const [i, ownership] of [a, b].entries()) {
    const controller = await createController({
      ownership,
      peerPort: ports[i],
      peerHost: "127.0.0.1",
      port: 0,
      reconciliationMs: 60000,
      backend: {
        list: async () => [],
        create: async () => {
          throw Error("unexpected");
        },
        remove: async () => {
          throw Error("unexpected");
        },
      },
    });
    controllers.push(controller);
    runtimes.push(createControllerRuntime(controller.controlOrigin));
  }
  const endpoints = await Promise.all(
    runtimes.map((runtime) => runtime.request({ op: "register", executionId: id })),
  );
  assert.equal(
    new URL(endpoints[0].wsEndpoint).pathname,
    new URL(endpoints[1].wsEndpoint).pathname,
  );
  const ownerIndex = a.owns(id) ? 0 : 1;
  const peerIndex = 1 - ownerIndex;
  const path = new URL(endpoints[0].wsEndpoint).pathname;
  const response = await fetch(`${controllers[peerIndex].origin}${path}/session`);
  assert.equal(response.status, 200);
  assert.equal((await response.json()).state, "unused");
  await [a, b][ownerIndex].indexLegacy(await [a, b][ownerIndex].journal.read(id));
  await [a, b][peerIndex].refresh();
  const legacyPath = `/execution/${path.split("/").at(-1)}/session`;
  assert.equal(
    (await fetch(`${controllers[peerIndex].origin}${legacyPath}`)).status,
    200,
  );
  const twice = await fetch(`http://127.0.0.1:${ports[peerIndex]}${path}/session`);
  assert.equal(twice.status, 503);
  assert.equal((await twice.json()).error, "browser_controller_owner_moved");
  await [a, b][peerIndex].resolve(id);
  advance(15001);
  await controllers[peerIndex].reconcile();
  const recovered = await fetch(`${controllers[peerIndex].origin}${path}/session`);
  assert.equal(recovered.status, 200);
  assert.equal((await recovered.json()).state, "unused");
  await assert.rejects(
    controllers[ownerIndex].command({ op: "access_create", executionId: id }),
    /browser_host_authority_required|browser_controller_fenced/,
  );
  const forwarded = await runtimes[ownerIndex].request({
    op: "session",
    executionId: id,
    action: "status",
  });
  assert.equal(forwarded.state, "unused");
  await runtimes[peerIndex].request({ op: "finalize", executionId: id });
  await runtimes[peerIndex].request({ op: "finalize", executionId: id });
  assert.equal(a.owns(id), false);
  assert.equal(b.owns(id), false);
});

test("an in-flight old-epoch Secret write loses its conditional update across takeover", async (t) => {
  const { a, b, state, advance } = await fixture(t);
  await a.resolve(id, true);
  await a.journal.create(id, { phase: "registered" });
  await b.resolve(id);
  const entered = Promise.withResolvers();
  const resume = Promise.withResolvers();
  state.beforeWrite = async (path) => {
    if (!path.includes("/secrets/")) return;
    state.beforeWrite = undefined;
    entered.resolve();
    await resume.promise;
  };
  const pending = a.journal.update(id, 1, { phase: "finished" });
  const rejected = assert.rejects(pending, /browser_controller_fenced/);
  await entered.promise;
  advance(15001);
  await b.resolve(id);
  resume.resolve();
  await rejected;
  assert.equal((await b.journal.read(id)).data.phase, "registered");
});

test("finished recovery releases a stranded Profile lease and bounds shared finished fences", async (t) => {
  const { a, b, advance, objects } = await fixture(t, { maxFinished: 1 });
  await a.resolve(id, true);
  await a.journal.create(id, { phase: "registered", profileId: "stranded" });
  await a.acquireProfile(id, "stranded");
  await a.journal.update(id, 1, { phase: "finished", profileId: "stranded" });
  await b.resolve(id);
  advance(15001);
  await b.resolve(id);
  await b.complete(id);
  await b.resolve(other, true);
  await b.journal.create(other, { phase: "registered", profileId: "stranded" });
  await b.acquireProfile(other, "stranded");
  await b.journal.update(other, 1, { phase: "finished", profileId: "stranded" });
  await b.complete(other);
  await b.refresh();
  assert.equal(await b.journal.read(id), undefined);
  assert.equal((await b.journal.read(other)).data.phase, "finished");
  const state = [...objects.values()].filter(
    (object) => object.metadata.labels["browser-sandbox.io/state"] === "execution",
  );
  assert.equal(
    state.length,
    2,
    "One retained finished fence has one Lease and one Secret",
  );
});

test("lease renewal passes share one bounded operation", async (t) => {
  const { a, state } = await fixture(t);
  await a.resolve(id, true);
  const entered = Promise.withResolvers();
  const resume = Promise.withResolvers();
  state.beforeWrite = async (path) => {
    if (!path.includes("/leases/")) return;
    state.beforeWrite = undefined;
    entered.resolve();
    await resume.promise;
  };
  const first = a.renew();
  await entered.promise;
  const second = a.renew();
  assert.equal(first, second);
  resume.resolve();
  await Promise.all([first, second]);
  assert.equal(a.owns(id), true);
});

test("a failed Secret handoff retries before ownership becomes usable", async (t) => {
  const { a, state } = await fixture(t);
  state.failSecretCreate = true;
  await assert.rejects(a.resolve(id, true));
  assert.equal(a.owns(id), false);
  await a.renew();
  await a.resolve(id, true);
  assert.equal(a.owns(id), true);
  assert.equal((await a.journal.create(id, { phase: "registered" })).revision, 1);
});

test("a stale completion cannot use a watch-updated Lease version to release the new owner", async (t) => {
  const { a, b, state, advance } = await fixture(t);
  await a.resolve(id, true);
  await a.journal.create(id, { phase: "finished" });
  await b.resolve(id);
  const entered = Promise.withResolvers();
  const resume = Promise.withResolvers();
  state.beforeGet = async (path) => {
    if (!path.includes("/secrets/")) return;
    state.beforeGet = undefined;
    entered.resolve();
    await resume.promise;
  };
  const pending = a.complete(id);
  const rejected = assert.rejects(
    pending,
    /browser_kubernetes_operation_failed|browser_controller_fenced/,
  );
  await entered.promise;
  advance(15001);
  await b.resolve(id);
  await a.refresh();
  resume.resolve();
  await rejected;
  assert.equal(b.owns(id), true);
  assert.equal((await a.resolve(id)).epoch, 2);
});

test("interrupted finished-fence compaction resumes after its Secret was deleted", async (t) => {
  const { a, b, state, advance, objects } = await fixture(t, { maxFinished: 1 });
  for (const executionId of [id, other]) {
    await a.resolve(executionId, true);
    await a.journal.create(executionId, { phase: "finished" });
    await a.complete(executionId);
  }
  state.failLeaseDelete = true;
  await assert.rejects(a.refresh());
  await b.refresh();
  advance(15001);
  await b.refresh();
  assert.equal(await b.journal.read(id), undefined);
  const remaining = [...objects.values()].filter(
    (object) => object.metadata.labels["browser-sandbox.io/state"] === "execution",
  );
  assert.equal(remaining.length, 2);
  await b.resolve(other);
  await b.complete(other);
  assert.equal(b.owns(other), false);
});

test("an old absent-Profile POST cannot recreate ownership after takeover completes", async (t) => {
  const { a, b, state, advance } = await fixture(t);
  await a.resolve(id, true);
  await a.journal.create(id, { phase: "registered", profileId: "post-race" });
  await b.resolve(id);
  const entered = Promise.withResolvers();
  const resume = Promise.withResolvers();
  state.beforeGet = async (path) => {
    if (!path.includes("-p-")) return;
    state.beforeGet = undefined;
    entered.resolve();
    await resume.promise;
  };
  const old = a.acquireProfile(id, "post-race");
  const rejected = assert.rejects(
    old,
    /browser_profile_busy|browser_controller_fenced/,
  );
  await entered.promise;
  advance(15001);
  await b.resolve(id);
  await b.acquireProfile(id, "post-race");
  await b.journal.update(id, 1, { phase: "finished", profileId: "post-race" });
  await b.complete(id);
  resume.resolve();
  await rejected;
  await b.resolve(other, true);
  await b.journal.create(other, { phase: "registered", profileId: "post-race" });
  await b.acquireProfile(other, "post-race");
});

test("a controller takeover can finalize durable intent interrupted before Profile acquisition", async (t) => {
  const { a, b, ports, advance, cleanups } = await fixture(t);
  const backend = {
    list: async () => [],
    create: async () => {
      throw Error("unexpected");
    },
    remove: async () => {
      throw Error("unexpected");
    },
  };
  const first = await createController({
    ownership: a,
    backend,
    peerPort: ports[0],
    port: 0,
    reconciliationMs: 60000,
  });
  const peer = { signal: new AbortController().signal, callback: async () => {} };
  // Persist exactly the pending registration contract before any Profile mutation.
  await a.resolve(id, true);
  await a.journal.create(id, {
    phase: "registered",
    profileId: "pending-intent",
    profileAcquired: false,
    storageMode: "host",
    incarnation: other,
    allowedDomains: [],
    registeredAt: Date.now(),
    lastActivityAt: Date.now(),
    grants: [],
    workerProtocol: "browser-sandbox.worker/v2",
  });
  await b.resolve(id);
  advance(15001);
  const second = await createController({
    ownership: b,
    backend,
    peerPort: ports[1],
    port: 0,
    reconciliationMs: 60000,
  });
  cleanups.unshift(async () => {
    await first.stop({ finalize: false });
    await second.stop({ finalize: false });
  });
  await second.command({ op: "finalize", executionId: id }, peer);
  assert.equal((await b.journal.read(id)).data.phase, "finished");
  await b.resolve(other, true);
  await b.journal.create(other, { phase: "registered", profileId: "pending-intent" });
  await b.acquireProfile(other, "pending-intent");
});

test("a stale owner cannot acquire a free Profile reused by another completed execution", async (t) => {
  const { a, b, state, advance } = await fixture(t);
  await a.resolve(id, true);
  await a.journal.create(id, { phase: "registered", profileId: "reused" });
  await b.resolve(id);
  const entered = Promise.withResolvers();
  const resume = Promise.withResolvers();
  state.readAfterGet = true;
  state.beforeGet = async (path) => {
    if (!path.includes("-p-")) return;
    state.beforeGet = undefined;
    entered.resolve();
    await resume.promise;
  };
  const old = a.acquireProfile(id, "reused");
  const rejected = assert.rejects(old, /browser_controller_fenced/);
  await entered.promise;
  advance(15001);
  await b.resolve(id);
  await b.acquireProfile(id, "reused");
  await b.journal.update(id, 1, { phase: "finished", profileId: "reused" });
  await b.complete(id);
  await b.resolve(other, true);
  await b.journal.create(other, { phase: "registered", profileId: "reused" });
  await b.acquireProfile(other, "reused");
  await b.journal.update(other, 1, { phase: "finished", profileId: "reused" });
  await b.complete(other);
  resume.resolve();
  await rejected;
});

test("a stale orphan discovery after another owner finished does not reclaim twice", async (t) => {
  const { a, b, ports, cleanups } = await fixture(t);
  const worker = {
    executionId: id,
    incarnation: other,
    id: "pod/owned-worker",
    origin: "http://127.0.0.1:1",
    runtime: {
      close() {},
      async request(request) {
        return request.op === "worker_status"
          ? {
              protocol: "browser-sandbox.worker/v2",
              session: { state: "unused", connectedClients: 0 },
              takeover: "automation",
              pendingAuth: 0,
            }
          : { stopped: true, used: false };
      },
    },
  };
  let present = true;
  let stale = false;
  let removals = 0;
  const events = [];
  const backend = {
    async list() {
      if (stale) {
        stale = false;
        return [worker];
      }
      return present ? [worker] : [];
    },
    async create() {
      throw Error("unexpected");
    },
    async remove() {
      removals++;
      present = false;
    },
  };
  await a.resolve(id, true);
  await a.journal.create(id, { phase: "finished" });
  await a.complete(id);
  const first = await createController({
    ownership: a,
    backend,
    peerPort: ports[0],
    port: 0,
    reconciliationMs: 60000,
    onWorkerEvent: (event) => events.push(event),
  });
  await first.reconcile();
  assert.equal(removals, 1);
  stale = true;
  const second = await createController({
    ownership: b,
    backend,
    peerPort: ports[1],
    port: 0,
    reconciliationMs: 60000,
    onWorkerEvent: (event) => events.push(event),
  });
  cleanups.unshift(async () => {
    await first.stop({ finalize: false });
    await second.stop({ finalize: false });
  });
  assert.equal((await second.command({ op: "controller_status" })).executions, 0);
  assert.equal(removals, 1);
  assert.equal(events.filter((event) => event.type === "removed").length, 1);
});

test("controller startup rejects a volume Profile store with multiple replicas before reading cluster credentials", () => {
  const child = spawnSync(
    process.execPath,
    [new URL("../dist/controller-main.js", import.meta.url).pathname],
    {
      env: {
        PATH: process.env.PATH,
        BROWSER_BACKEND: "kubernetes",
        BROWSER_CONTROLLER_REPLICAS: "3",
        BROWSER_PROFILE_STORE_ROOT: "/unused-profile-store",
      },
      encoding: "utf8",
      timeout: 5000,
    },
  );
  assert.equal(child.status, 1);
  assert.match(child.stderr, /browser_profile_store_replicas_unsupported/);
});

test("restarting migration accepts an imported record while its previous owner Lease expires", async (t) => {
  const { a, b } = await fixture(t);
  const directory = await mkdtemp(join(tmpdir(), "journal-restart-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  await cp(
    new URL("../.github/scripts/managed/fixtures/journal-v0.2.0/", import.meta.url),
    directory,
    { recursive: true },
  );
  await importExecutionJournal({
    directory,
    installationId: "fixture",
    ownership: a,
    validate: validateControllerRecord,
  });
  assert.equal(
    await importExecutionJournal({
      directory,
      installationId: "fixture",
      ownership: b,
      validate: validateControllerRecord,
    }),
    0,
  );
  assert.equal(b.owns(id), false);
  assert.equal((await b.journal.read(id)).revision, 1);
});

test("unauthenticated execution routes cannot amplify requests to Kubernetes", async (t) => {
  const { a, ports, state, cleanups } = await fixture(t);
  const controller = await createController({
    ownership: a,
    peerPort: ports[0],
    port: 0,
    reconciliationMs: 60000,
    backend: {
      list: async () => [],
      create: async () => {
        throw Error("unexpected");
      },
      remove: async () => {},
    },
  });
  cleanups.unshift(() => controller.stop({ finalize: false }));
  const before = state.requests;
  for (const route of ["session", "files", "auth"]) {
    const response = await fetch(
      `${controller.origin}/execution/${id}/${"a".repeat(64)}/${route}`,
    );
    assert.equal(response.status, 403);
    await response.body.cancel();
  }
  const status = await new Promise((resolve, reject) => {
    const req = request(`${controller.origin}/execution/${other}/view/socket`, {
      headers: { Connection: "Upgrade", Upgrade: "websocket" },
    });
    req.on("response", (res) => {
      res.resume();
      resolve(res.statusCode);
    });
    req.on("error", reject);
    req.end();
  });
  assert.equal(status, 403);
  assert.equal(
    state.requests,
    before,
    "denied agent requests must not reach the cluster API",
  );
});

test("completed migration cannot resurrect compacted v1 grants on a later single-replica startup", async (t) => {
  const { a } = await fixture(t, { maxFinished: 1 });
  const directory = await mkdtemp(join(tmpdir(), "migration-once-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  await cp(
    new URL("../.github/scripts/managed/fixtures/journal-v0.2.0/", import.meta.url),
    directory,
    { recursive: true },
  );
  const options = {
    directory,
    installationId: "fixture",
    ownership: a,
    validate: validateControllerRecord,
  };
  await importExecutionJournal(options);
  const record = await a.journal.read(id);
  await a.journal.update(id, record.revision, { ...record.data, phase: "finished" });
  await a.complete(id);
  await a.resolve(other, true);
  await a.journal.create(other, { phase: "finished" });
  await a.complete(other);
  await a.refresh();
  assert.equal(await a.journal.read(id), undefined);
  assert.equal(await importExecutionJournal(options), 0);
  assert.equal(
    await a.journal.read(id),
    undefined,
    "compacted grants must stay absent",
  );
});

test("watched peer addresses reject credentials, paths and non-HTTP origins", async (t) => {
  const { a, objects } = await fixture(t);
  await a.resolve(id, true);
  const lease = [...objects.values()].find(
    (o) => o.metadata.labels?.["browser-sandbox.io/execution"] === id,
  );
  for (const origin of [
    "https://peer:8084",
    "http://user:secret@peer:8084",
    "http://peer:8084/path",
  ]) {
    lease.metadata.annotations["browser-sandbox.io/peer"] = origin;
    await assert.rejects(a.refresh(), /browser_controller_state_corrupt/);
  }
});

test("watch refresh replaces the complete legacy capability set for an execution", async (t) => {
  const { a } = await fixture(t);
  await a.resolve(id, true);
  const first = await a.journal.create(id, {
    phase: "registered",
    grants: [{ token: "a".repeat(64) }],
  });
  await a.indexLegacy(first);
  assert.equal(a.findCapability("a".repeat(64)), id);
  const next = await a.journal.update(id, first.revision, {
    phase: "registered",
    grants: [{ token: "b".repeat(64) }],
  });
  await a.indexLegacy(next);
  assert.equal(a.findCapability("a".repeat(64)), undefined);
  assert.equal(a.findCapability("b".repeat(64)), id);
  await a.indexLegacy({ ...next, data: { ...next.data, grants: [] } });
  assert.equal(a.findCapability("b".repeat(64)), undefined);
});

test("startup renews ownership during delayed discovery and lists workers once per refresh", async (t) => {
  t.mock.timers.enable({ apis: ["setInterval"] });
  const { a, ports, state, advance, cleanups } = await fixture(t);
  const data = JSON.parse(
    await readFile(
      new URL(
        "../.github/scripts/managed/fixtures/journal-v0.2.0/11111111-1111-4111-8111-111111111111.json",
        import.meta.url,
      ),
      "utf8",
    ),
  ).data;
  for (const executionId of [id, other]) {
    await a.resolve(executionId, true);
    await a.journal.create(executionId, { ...data, grants: [] });
  }
  const entered = Promise.withResolvers(),
    resume = Promise.withResolvers(),
    renewed = Promise.withResolvers();
  let lists = 0;
  state.beforeWrite = async (path, body) => {
    if (path.includes("/leases/") && body.spec?.renewTime) renewed.resolve();
  };
  const starting = createController({
    ownership: a,
    peerPort: ports[0],
    port: 0,
    reconciliationMs: 60000,
    backend: {
      list: async () => {
        lists++;
        entered.resolve();
        await resume.promise;
        return [];
      },
      create: async () => {
        throw Error("unexpected");
      },
      remove: async () => {},
    },
  });
  cleanups.unshift(async () => {
    resume.resolve();
    const c = await starting;
    await c.stop({ finalize: false });
  });
  await entered.promise;
  advance(5000);
  t.mock.timers.tick(5000);
  await Promise.race([
    renewed.promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(Error("startup did not renew before listening")), 300),
    ),
  ]);
  resume.resolve();
  await starting;
  assert.equal(
    lists,
    1,
    "one discovery snapshot is shared across all recovered records",
  );
});

test("takeover fences a Profile PUT already past the old owner's authorization checks", async (t) => {
  const { a, b, state, advance } = await fixture(t);
  await a.resolve(id, true);
  await a.journal.create(id, { phase: "registered", profileId: "put-race" });
  await a.acquireProfile(id, "put-race");
  await a.journal.update(id, 1, { phase: "finished", profileId: "put-race" });
  await b.resolve(id);
  const entered = Promise.withResolvers(),
    resume = Promise.withResolvers();
  state.beforeWrite = async (path, body) => {
    if (
      path.includes("-p-") &&
      body.metadata.annotations["browser-sandbox.io/epoch"] === "1"
    ) {
      entered.resolve();
      await resume.promise;
    }
  };
  const old = a.releaseProfile(id, "put-race");
  old.catch(() => {});
  await entered.promise;
  advance(15001);
  await b.resolve(id);
  resume.resolve();
  await assert.rejects(old, /browser_profile_busy|browser_controller_fenced/);
});

test("failed controller construction closes ownership and its background work", async (t) => {
  const { a, ports } = await fixture(t);
  await assert.rejects(
    createController({
      ownership: a,
      peerPort: ports[0],
      port: 0,
      backend: {
        list: async () => {
          throw Error("discovery_failed");
        },
        create: async () => {
          throw Error("unexpected");
        },
        remove: async () => {},
      },
    }),
    /discovery_failed/,
  );
  await assert.rejects(a.resolve(id, true), /abort|cancel/i);
});

test("slow graceful shutdown keeps renewing until it conditionally releases ownership", async (t) => {
  t.mock.timers.enable({ apis: ["setInterval"] });
  const { a, ports, state, advance, cleanups } = await fixture(t);
  const controller = await createController({
    ownership: a,
    peerPort: ports[0],
    port: 0,
    reconciliationMs: 60000,
    backend: {
      preserveWorkersOnShutdown: true,
      list: async () => [],
      create: async () => {
        throw Error("unexpected");
      },
      remove: async () => {},
    },
  });
  cleanups.unshift(() => controller.stop({ finalize: false }));
  await controller.command(
    { op: "register", executionId: id },
    { signal: new AbortController().signal, callback: async () => {} },
  );
  const resume = Promise.withResolvers(),
    renewed = Promise.withResolvers();
  controller.bindings.get(id).persisted = resume.promise;
  const stopping = controller.stop({ finalize: false });
  state.beforeWrite = async (_path, body) => {
    if (body.spec?.holderIdentity === "a" && body.spec?.renewTime) renewed.resolve();
  };
  advance(5000);
  t.mock.timers.tick(5000);
  try {
    await Promise.race([
      renewed.promise,
      new Promise((_, reject) =>
        setTimeout(() => reject(Error("shutdown stopped renewal before cleanup")), 300),
      ),
    ]);
  } finally {
    resume.resolve();
    await stopping;
  }
  assert.equal(a.owns(id), false);
});

for (const deletionFails of [false, true])
  test(`post-takeover discovery rejects a late lower-epoch Pod (${deletionFails ? "retry deletion" : "normal deletion"})`, async (t) => {
    const { a, b, ports, advance, cleanups } = await fixture(t);
    const data = JSON.parse(
      await readFile(
        new URL(
          "../.github/scripts/managed/fixtures/journal-v0.2.0/11111111-1111-4111-8111-111111111111.json",
          import.meta.url,
        ),
        "utf8",
      ),
    ).data;
    await a.resolve(id, true);
    await a.journal.create(id, {
      ...data,
      registeredAt: Date.now(),
      lastActivityAt: Date.now(),
      grants: [],
      workerProtocol: "browser-sandbox.worker/v2",
    });
    await b.resolve(id);
    advance(15001);
    let late,
      removed = 0;
    const backend = {
      list: async () => (late ? [late] : []),
      create: async () => {
        throw Error("unexpected");
      },
      fence: async (worker, epoch, _signal, stopped) => {
        assert.equal(stopped, true);
        worker.epoch = epoch;
      },
      remove: async (worker) => {
        assert.equal(worker.epoch, 2);
        removed++;
        if (deletionFails && removed === 1) throw Error("transient_delete_failure");
        late = undefined;
      },
    };
    const controller = await createController({
      ownership: b,
      backend,
      peerPort: ports[1],
      port: 0,
      reconciliationMs: 60000,
    });
    cleanups.unshift(() => controller.stop({ finalize: false }));
    late = {
      id: "late/uid",
      executionId: id,
      incarnation: data.incarnation,
      epoch: 1,
      runtime: {
        request: async () => {
          throw Error("late Pod must never run commands");
        },
        close: () => {},
      },
    };
    if (deletionFails)
      await assert.rejects(controller.reconcile(), /transient_delete_failure/);
    await controller.reconcile();
    assert.equal(removed, deletionFails ? 2 : 1);
    assert.equal(controller.bindings.get(id).worker, undefined);
    await controller.reconcile();
    assert.equal(removed, deletionFails ? 2 : 1);
  });

test("the old controller itself reports fencing and performs no destructive action after ownership moves", async (t) => {
  const { a, b, ports, advance, cleanups } = await fixture(t);
  const events = [];
  const controller = await createController({
    ownership: a,
    peerPort: ports[0],
    port: 0,
    reconciliationMs: 60000,
    onWorkerEvent: (e) => events.push(e),
    backend: {
      list: async () => [],
      create: async () => {
        throw Error("unexpected");
      },
      remove: async () => {
        throw Error("stale cleanup");
      },
    },
  });
  cleanups.unshift(() => controller.stop({ finalize: false }));
  await controller.command(
    { op: "register", executionId: id },
    {
      signal: new AbortController().signal,
      callback: async () => {
        throw Error("stale commit");
      },
    },
  );
  await b.resolve(id);
  advance(15001);
  await b.resolve(id);
  await a.refresh();
  assert.equal(controller.bindings.has(id), false);
  assert.equal(
    events.filter((e) => e.type === "fenced" && e.executionId === id).length,
    1,
  );
  assert.equal(
    events.some((e) => ["removed", "finalized", "commit"].includes(e.type)),
    false,
  );
  assert.equal((await b.journal.read(id)).data.phase, "registered");
});

test("a pending unacquired Profile intent cannot block takeover while another execution holds it", async (t) => {
  const { a, b, advance } = await fixture(t);
  await a.resolve(other, true);
  await a.journal.create(other, { phase: "registered", profileId: "held" });
  await a.acquireProfile(other, "held");
  await a.resolve(id, true);
  await a.journal.create(id, {
    phase: "registered",
    profileId: "held",
    profileAcquired: false,
  });
  await b.resolve(id);
  advance(15001);
  assert.equal((await b.resolve(id)).epoch, 2);
  await assert.rejects(b.acquireProfile(id, "held"), /browser_profile_busy/);
});

test("journal parser errors never expose malformed persisted data", async (t) => {
  const { a } = await fixture(t);
  const directory = await mkdtemp(join(tmpdir(), "journal-redaction-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  await writeFile(
    join(directory, `${id}.json`),
    '{"secret":"sensitive-capability","x":bad}',
  );
  await assert.rejects(
    importExecutionJournal({
      directory,
      installationId: "fixture",
      ownership: a,
      validate: validateControllerRecord,
    }),
    { message: "browser_execution_journal_corrupt" },
  );
});

test("an expired legacy viewer capability cannot trigger cluster reads before authorization", async (t) => {
  const { a, ports, state, cleanups } = await fixture(t);
  const controller = await createController({
    ownership: a,
    peerPort: ports[0],
    port: 0,
    reconciliationMs: 60000,
    backend: {
      list: async () => [],
      create: async () => {
        throw Error("unexpected");
      },
      remove: async () => {},
    },
  });
  cleanups.unshift(() => controller.stop({ finalize: false }));
  await a.resolve(id, true);
  const data = JSON.parse(
    await readFile(
      new URL(
        "../.github/scripts/managed/fixtures/journal-v0.2.0/11111111-1111-4111-8111-111111111111.json",
        import.meta.url,
      ),
      "utf8",
    ),
  ).data;
  const token = "c".repeat(64);
  const record = await a.journal.create(id, {
    ...data,
    registeredAt: Date.now(),
    lastActivityAt: Date.now(),
    grants: [
      { token, kind: "view", expiresAt: Date.now() - 1, control: false, resize: false },
    ],
  });
  await a.indexLegacy(record);
  const before = state.requests;
  const socket = new WebSocket(
    `${controller.origin.replace(/^http/, "ws")}/view/socket`,
  );
  socket.once("open", () =>
    socket.send(JSON.stringify({ type: "authenticate", token })),
  );
  socket.on("error", () => {});
  await new Promise((resolve) => socket.once("close", resolve));
  assert.equal(state.requests, before);
});

test("finished orphan takeovers share one fresh discovery after all claims complete", async (t) => {
  const { a, b, ports, cleanups } = await fixture(t);
  for (const executionId of [id, other]) {
    await a.resolve(executionId, true);
    await a.journal.create(executionId, { phase: "finished" });
    await a.complete(executionId);
  }
  let lists = 0;
  const removed = [];
  const workers = [id, other].map((executionId) => ({
    id: `${executionId}/uid`,
    executionId,
    incarnation: executionId,
    runtime: {
      request: async (request) =>
        request.op === "finalize"
          ? { stopped: true, used: false }
          : { protocol: "browser-sandbox.worker/v2", session: { state: "unused" } },
      close: () => {},
    },
  }));
  let remaining = workers[1];
  const backend = {
    list: async () => {
      lists++;
      if (lists === 1) return workers;
      if (lists === 2)
        assert(
          b.owns(id) && b.owns(other),
          "fresh discovery must follow every candidate takeover",
        );
      return remaining ? [remaining] : [];
    },
    create: async () => {
      throw Error("unexpected");
    },
    remove: async (worker) => {
      removed.push(worker.executionId);
      remaining = undefined;
    },
  };
  const controller = await createController({
    ownership: b,
    backend,
    peerPort: ports[1],
    port: 0,
    reconciliationMs: 60000,
  });
  cleanups.unshift(() => controller.stop({ finalize: false }));
  assert.equal(
    lists,
    2,
    "one initial discovery and one shared post-takeover revalidation",
  );
  assert.equal((await b.journal.read(id)).data.phase, "finished");
  await controller.reconcile();
  assert.deepEqual(
    removed,
    [other],
    "already absent worker must not be reclaimed again",
  );
});

test("an unrecorded orphan absent from post-takeover discovery becomes a finished fence", async (t) => {
  const { b, ports, cleanups } = await fixture(t);
  let lists = 0;
  const worker = {
    id: "vanished/uid",
    executionId: id,
    incarnation: other,
    runtime: {
      request: async () => {
        throw Error("vanished worker cannot be contacted");
      },
      close: () => {},
    },
  };
  const controller = await createController({
    ownership: b,
    peerPort: ports[1],
    port: 0,
    reconciliationMs: 60000,
    backend: {
      list: async () => (++lists === 1 ? [worker] : []),
      create: async () => {
        throw Error("unexpected");
      },
      remove: async () => {
        throw Error("vanished worker cannot be removed twice");
      },
    },
  });
  cleanups.unshift(() => controller.stop({ finalize: false }));
  assert.equal(lists, 2);
  assert.equal((await b.journal.read(id)).data.phase, "finished");
  await assert.rejects(
    controller.command(
      { op: "register", executionId: id },
      { signal: new AbortController().signal, callback: async () => {} },
    ),
    /browser_execution_revoked/,
  );
});

test("non-owner forwarding cannot normalize an execution path into private host control", async (t) => {
  const { a, b, ports, cleanups } = await fixture(t);
  const controllers = [];
  cleanups.unshift(async () => {
    for (const c of controllers) await c.stop({ finalize: false });
  });
  for (const [i, ownership] of [a, b].entries())
    controllers.push(
      await createController({
        ownership,
        peerPort: ports[i],
        peerHost: "127.0.0.1",
        port: 0,
        reconciliationMs: 60000,
        backend: {
          list: async () => [],
          create: async () => {
            throw Error("unexpected");
          },
          remove: async () => {},
        },
      }),
    );
  await controllers[0].command(
    { op: "register", executionId: id },
    { signal: new AbortController().signal, callback: async () => {} },
  );
  await b.refresh();
  const url = new URL(controllers[1].origin);
  for (const suffix of ["../../host", "%2e%2e/%2e%2e/host"]) {
    const status = await new Promise((resolve, reject) => {
      const req = request({
        hostname: url.hostname,
        port: url.port,
        path: `/execution/${id}/${suffix}`,
        headers: {
          Connection: "Upgrade",
          Upgrade: "websocket",
          "Sec-WebSocket-Key": "dGhlIHNhbXBsZSBub25jZQ==",
          "Sec-WebSocket-Version": "13",
        },
      });
      req.once("upgrade", (res, socket) => {
        socket.destroy();
        resolve(res.statusCode);
      });
      req.once("response", (res) => {
        res.resume();
        resolve(res.statusCode);
      });
      req.once("error", reject);
      req.end();
    });
    assert.equal(
      status,
      403,
      "execution routing must not reach the trusted peer /host route",
    );
  }
});
