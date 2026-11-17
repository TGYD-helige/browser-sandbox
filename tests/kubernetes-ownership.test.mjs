import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { cp, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { createController, validateControllerRecord } from "../dist/controller.js";
import { importExecutionJournal } from "../dist/kubernetes-migration.js";
import { createKubernetesOwnership } from "../dist/kubernetes-ownership.js";
import { createControllerRuntime } from "../dist/network-control.js";

async function fixture(t, extra = {}) {
  const objects = new Map();
  const cleanups = [];
  const state = {};
  let version = 0;
  const server = createServer(async (req, res) => {
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
  assert.equal(await importExecutionJournal(options), 1);
  assert.deepEqual(await readFile(filename), original);
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
    1,
  );
  assert.equal(b.owns(id), false);
  assert.equal((await b.journal.read(id)).revision, 1);
});
