import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { cp, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createServer, request } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import WebSocket from "ws";
import { waitForControl } from "../dist/automation-control.js";
import { createController, validateControllerRecord } from "../dist/controller.js";
import { createKubernetesBackend } from "../dist/kubernetes-backend.js";
import { importExecutionJournal } from "../dist/kubernetes-migration.js";
import { createKubernetesOwnership } from "../dist/kubernetes-ownership.js";
import { createControllerRuntime } from "../dist/network-control.js";

async function fixture(t, extra = {}) {
  const objects = new Map();
  const cleanups = [];
  const state = { requests: 0 };
  const gc = () => {
    let changed;
    do {
      changed = false;
      for (const [path, object] of objects) {
        const refs = object.metadata.ownerReferences;
        if (
          !refs?.length ||
          refs.some((ref) => {
            const base =
              ref.kind === "Lease"
                ? "/apis/coordination.k8s.io/v1/namespaces/test/leases/"
                : "/api/v1/namespaces/test/configmaps/";
            return objects.get(base + ref.name)?.metadata.uid === ref.uid;
          })
        )
          continue;
        objects.delete(path);
        changed = true;
      }
    } while (changed);
  };
  objects.set("/api/v1/namespaces/test/configmaps/workers", {
    metadata: {
      name: "workers",
      uid: "release-uid",
      labels: {
        "app.kubernetes.io/name": "browser-sandbox",
        "app.kubernetes.io/instance": "test",
      },
    },
  });
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
      if (req.url.includes("watch=true")) {
        if (!state.watchEvents) return reply(200);
        const events = state.watchEvents;
        state.watchEvents = undefined;
        return res
          .writeHead(200)
          .end(events.map((event) => JSON.stringify(event)).join("\n") + "\n");
      }
      const items = [...objects.entries()]
        .filter(
          ([key, value]) =>
            key.startsWith(path + "/") &&
            value.metadata.labels["browser-sandbox.io/state"] === "execution",
        )
        .map(([, value]) => value);
      const snapshot = await state.beforeList?.(items);
      return reply(200, {
        items: snapshot ?? items,
        metadata: { resourceVersion: String(version) },
      });
    }

    if (req.method === "GET" && path.endsWith("/pods"))
      return reply(200, {
        items: [...objects.entries()]
          .filter(([key]) => key.startsWith(path + "/"))
          .map(([, value]) => value),
        metadata: {},
      });
    if (req.method === "GET") {
      if (state.failGet && path.endsWith(state.failGet)) return reply(503);
      await state.beforeGet?.(path);
      if (state.readAfterGet) previous = objects.get(path);
      return reply(previous ? 200 : 404, previous);
    }
    if (req.method === "POST") {
      await state.beforePost?.(path, body);
      if (path.endsWith("/secrets") && state.failSecretCreate) {
        state.failSecretCreate = false;
        return reply(503);
      }
      const key = `${path}/${body.metadata.name}`;
      if (objects.has(key)) return reply(409);
      body.metadata.resourceVersion = String(++version);
      body.metadata.uid = `object-${version}`;
      if (path.endsWith("/pods")) {
        body.metadata.uid = `pod-${version}`;
        body.status = {
          phase: "Running",
          podIP: "127.0.0.1",
          conditions: [{ type: "Ready", status: "True" }],
        };
      }
      objects.set(key, body);
      return reply(201, body);
    }
    if (!previous) return reply(404);
    if (req.method === "PUT") {
      await state.beforeWrite?.(path, body);
      previous = objects.get(path);
      if (body.metadata.resourceVersion !== previous.metadata.resourceVersion)
        return reply(409);
      body.metadata.uid = previous.metadata.uid;
      body.metadata.resourceVersion = String(++version);
      objects.set(path, body);
      return reply(200, body);
    }
    if (req.method === "PATCH") {
      if (body.metadata.resourceVersion !== previous.metadata.resourceVersion)
        return reply(409);
      const updated = {
        ...previous,
        metadata: {
          ...previous.metadata,
          resourceVersion: String(++version),
          annotations: {
            ...previous.metadata.annotations,
            ...body.metadata.annotations,
          },
        },
      };
      objects.set(path, updated);
      return reply(200, updated);
    }
    if (req.method === "DELETE") {
      if (path.includes("/leases/") && state.failLeaseDelete) {
        state.failLeaseDelete = false;
        return reply(503);
      }
      if (
        body.preconditions.resourceVersion
          ? body.preconditions.resourceVersion !== previous.metadata.resourceVersion
          : body.preconditions.uid !== previous.metadata.uid
      )
        return reply(409);
      objects.delete(path);
      if (!state.pauseGc) gc();
      return reply(200);
    }
    reply(400);
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(async () => {
    let failure;
    for (const cleanup of cleanups) {
      try {
        await cleanup();
      } catch (error) {
        failure ??= error;
      }
    }
    await new Promise((resolve) => {
      server.closeAllConnections();
      server.close(resolve);
    });
    if (failure) throw failure;
  });
  let now = 0;
  const options = {
    namespace: "test",
    installation: "test",
    ownerName: "workers",
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
  const a = await createKubernetesOwnership({
    ...options,
    replica: "a",
    peerOrigin: `http://127.0.0.1:${ports[0]}`,
  });
  const b = await createKubernetesOwnership({
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
    gc,
    state,
    ports,
    cleanups,
    apiServer: options.apiServer,
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

test("interrupted conditional Lease deletion retries maintenance without blocking startup", async (t) => {
  const { a, b, state, advance, objects } = await fixture(t, { maxFinished: 1 });
  for (const executionId of [id, other]) {
    await a.resolve(executionId, true);
    await a.journal.create(executionId, { phase: "finished" });
    await a.complete(executionId);
  }
  state.failLeaseDelete = true;
  await a.refresh();
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
  await waitForControl(renewed.promise, AbortSignal.timeout(5000));
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
    if (path.includes("-p-") && body.spec?.holderIdentity === "") {
      state.beforeWrite = undefined;
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
    await waitForControl(renewed.promise, AbortSignal.timeout(5000));
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
      fence: async () => {
        throw Error("unpublished Pod must not be adopted");
      },
      remove: async (worker) => {
        assert.equal(worker.epoch, 1);
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

test("owned executions awaiting binding recovery return retryable 503 without cluster reads", async (t) => {
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
  const record = await a.journal.create(id, {
    phase: "registered",
    grants: [{ token: "d".repeat(64) }],
  });
  await a.indexLegacy(record);
  const before = state.requests;
  const response = await fetch(
    `${controller.origin}/execution/${id}/${"d".repeat(64)}/session`,
  );
  assert.equal(response.status, 503);
  assert.equal((await response.json()).error, "browser_controller_owner_moved");
  const socket = new WebSocket(
    `${controller.origin.replace(/^http/, "ws")}/view/socket`,
  );
  socket.once("open", () =>
    socket.send(JSON.stringify({ type: "authenticate", token: "d".repeat(64) })),
  );
  socket.on("error", () => {});
  const closed = await new Promise((resolve) =>
    socket.once("close", (code, reason) => resolve({ code, reason: String(reason) })),
  );
  assert.deepEqual(closed, { code: 1013, reason: "browser_controller_owner_moved" });
  assert.equal(state.requests, before);
});

test("a missing migration mount does not mark the journal imported forever", async (t) => {
  const { a } = await fixture(t);
  const root = await mkdtemp(join(tmpdir(), "missing-journal-mount-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const directory = join(root, "executions");
  const options = {
    directory,
    installationId: "fixture",
    ownership: a,
    validate: validateControllerRecord,
  };
  assert.equal(await importExecutionJournal(options), 0);
  assert.equal(await a.migrationComplete(), false);
  await cp(
    new URL("../.github/scripts/managed/fixtures/journal-v0.2.0/", import.meta.url),
    directory,
    { recursive: true },
  );
  assert.equal(await importExecutionJournal(options), 1);
});

test("authoritative lease refresh removes missed deletions and their capability routes", async (t) => {
  const { a, objects } = await fixture(t);
  await a.resolve(id, true);
  const record = await a.journal.create(id, {
    phase: "registered",
    grants: [{ token: "e".repeat(64) }],
  });
  await a.indexLegacy(record);
  const key = [...objects.keys()].find(
    (path) =>
      path.includes("/leases/") &&
      objects.get(path).metadata.labels["browser-sandbox.io/execution"] === id,
  );
  objects.delete(key);
  await a.refresh();
  assert.equal(a.lookup(id), undefined);
  assert.equal(a.findCapability("e".repeat(64)), undefined);
  assert.equal(a.owns(id), false);
});

test("an older LIST snapshot does not erase or replace a newer local observation", async (t) => {
  const { a, state } = await fixture(t);
  await a.resolve(id, true);
  const record = await a.journal.create(id, {
    phase: "registered",
    grants: [{ token: "e".repeat(64) }],
  });
  const entered = Promise.withResolvers(),
    resume = Promise.withResolvers();
  // Synthetic stale LIST: a newer Lease update arrives while the empty snapshot is in flight.
  state.beforeList = async () => {
    entered.resolve();
    await resume.promise;
    return [];
  };
  const refreshing = a.refresh();
  await entered.promise;
  await a.indexLegacy(record);
  resume.resolve();
  await refreshing;
  assert.equal(a.lookup(id)?.replica, "a");
  assert.equal(a.findCapability("e".repeat(64)), id);
  assert.equal(a.owns(id), true);
});

test("compaction skips a candidate changed during its record read and continues", async (t) => {
  const { a, b, state, advance } = await fixture(t, { maxFinished: 1 });
  const third = "33333333-3333-4333-8333-333333333333";
  for (const executionId of [id, other, third]) {
    await a.resolve(executionId, true);
    await a.journal.create(executionId, { phase: "finished" });
    await a.complete(executionId);
  }
  const entered = Promise.withResolvers(),
    resume = Promise.withResolvers();
  state.readAfterGet = true;
  state.beforeGet = async (path) => {
    if (!path.includes("/secrets/") || !path.endsWith(id)) return;
    state.beforeGet = undefined;
    entered.resolve();
    await resume.promise;
  };
  const refreshing = a.refresh();
  t.after(() => resume.resolve());
  const outcome = refreshing.then(
    () => null,
    (error) => error,
  );
  await waitForControl(entered.promise, AbortSignal.timeout(5000));
  await b.resolve(id);
  advance(15001);
  await b.resolve(id);
  resume.resolve();
  assert.equal(await outcome, null);
  assert.equal(b.owns(id), true);
  assert.equal(await a.journal.read(other), undefined);
  assert.equal((await a.journal.read(third)).data.phase, "finished");
});

test("compaction removes execution Leases and recreated ids start at epoch one with a different UID", async (t) => {
  const { a, b, objects, advance } = await fixture(t, { maxFinished: 1 });
  await a.resolve(id, true);
  await a.journal.create(id, { phase: "registered", profileId: "epoch-floor" });
  await a.acquireProfile(id, "epoch-floor");
  const first = [...objects.values()].find(
    (value) =>
      value.kind === "Lease" &&
      value.metadata.labels["browser-sandbox.io/execution"] === id,
  );
  await b.resolve(id);
  advance(15001);
  const old = await b.resolve(id);
  assert.equal(old.epoch, 2);
  await b.journal.update(id, 1, { phase: "finished", profileId: "epoch-floor" });
  await b.complete(id);
  await b.resolve(other, true);
  await b.journal.create(other, { phase: "finished" });
  await b.complete(other);
  await b.refresh();
  assert.equal(await b.journal.read(id), undefined);
  assert.equal(await b.resolve(id), undefined);
  assert.equal(
    [...objects.values()].some(
      (value) =>
        value.kind === "Lease" &&
        value.metadata.labels["browser-sandbox.io/execution"] === id,
    ),
    false,
  );
  const next = await b.resolve(id, true);
  assert.equal(next.epoch, 1);
  assert.notEqual(next.uid, first.metadata.uid);
  await b.journal.create(id, { phase: "registered", profileId: "epoch-floor" });
  await b.acquireProfile(id, "epoch-floor");
});

test("a Pod POST landing after compaction retains the old Lease UID and is skipped for GC", async (t) => {
  const { a, b, objects, gc, state, advance, apiServer } = await fixture(t, {
    maxFinished: 1,
  });
  state.pauseGc = true;
  const backend = (ownership) =>
    createKubernetesBackend({
      namespace: "test",
      installation: "test",
      apiServer,
      ownerName: "workers",
      image: `browser-sandbox@sha256:${"a".repeat(64)}`,
      ownership,
      template: {
        securityContext: {
          seccompProfile: {
            type: "Localhost",
            localhostProfile: "browser-sandbox.json",
          },
        },
      },
    });
  await a.resolve(id, true);
  await a.journal.create(id, { phase: "registered" });
  await b.resolve(id);
  advance(15001);
  const old = await b.resolve(id);
  assert.equal(old.epoch, 2);
  const oldLease = [...objects.values()].find(
    (value) =>
      value.kind === "Lease" &&
      value.metadata.labels["browser-sandbox.io/execution"] === id,
  );
  const entered = Promise.withResolvers(),
    resume = Promise.withResolvers();
  state.beforePost = async (path) => {
    if (!path.endsWith("/pods")) return;
    state.beforePost = undefined;
    entered.resolve();
    await resume.promise;
  };
  const pending = (await backend(b)).create(
    { executionId: id, incarnation: other },
    AbortSignal.timeout(10000),
  );
  const rejected = assert.rejects(pending, /browser_controller_fenced/);
  t.after(() => resume.resolve());
  await waitForControl(entered.promise, AbortSignal.timeout(5000));
  await a.resolve(id);
  advance(15001);
  await a.resolve(id);
  await a.journal.update(id, 1, { phase: "finished" });
  await a.complete(id);
  await a.resolve(other, true);
  await a.journal.create(other, { phase: "finished" });
  await a.complete(other);
  await a.refresh();
  assert.equal(
    await a.journal.read(id),
    undefined,
    "compaction completes before the old POST lands",
  );
  assert.equal(
    [...objects.keys()].some((path) => path.includes("/pods/")),
    false,
  );
  const current = await a.resolve(id, true);
  assert.equal(current.epoch, 1);
  assert.notEqual(current.uid, oldLease.metadata.uid);
  resume.resolve();
  await rejected;
  const pod = [...objects.values()].find((value) => value.kind === "Pod");
  assert.deepEqual(pod.metadata.ownerReferences, [
    {
      apiVersion: "coordination.k8s.io/v1",
      kind: "Lease",
      name: oldLease.metadata.name,
      uid: oldLease.metadata.uid,
      controller: true,
      blockOwnerDeletion: false,
    },
  ]);
  assert.equal(
    (await (await backend(a)).list(AbortSignal.timeout(5000))).length,
    0,
    "old-UID Pods are skipped without a fencing error",
  );
  gc();
  assert.equal(
    [...objects.values()].some((value) => value.kind === "Pod"),
    false,
  );
  await a.journal.create(id, { phase: "registered" });
});

function watchSentinel(objects, executionId) {
  const lease = structuredClone(
    [...objects.values()].find(
      (value) =>
        value.kind === "Lease" &&
        value.metadata.labels["browser-sandbox.io/execution"] === executionId,
    ),
  );
  lease.metadata.annotations["browser-sandbox.io/epoch"] = String(
    Number(lease.metadata.annotations["browser-sandbox.io/epoch"]) + 1,
  );
  lease.metadata.resourceVersion += "-sentinel";
  lease.spec.holderIdentity = "b";
  return { type: "MODIFIED", object: lease };
}

test("a stale LIST cannot restore a route added and deleted by watch while it was in flight", async (t) => {
  const { a, b, objects, state } = await fixture(t);
  await a.resolve(other, true);
  await b.resolve(id, true);
  const token = "f".repeat(64);
  const record = await b.journal.create(id, {
    phase: "registered",
    grants: [{ token }],
  });
  await b.indexLegacy(record);
  const lease = structuredClone(
    [...objects.values()].find(
      (value) =>
        value.kind === "Lease" &&
        value.metadata.labels["browser-sandbox.io/execution"] === id,
    ),
  );
  const entered = Promise.withResolvers(),
    resume = Promise.withResolvers();
  state.beforeList = async (items) => {
    state.beforeList = undefined;
    entered.resolve();
    await resume.promise;
    return items;
  };
  const refreshing = a.refresh();
  t.after(() => resume.resolve());
  await waitForControl(entered.promise, AbortSignal.timeout(5000));
  objects.delete(
    `/apis/coordination.k8s.io/v1/namespaces/test/leases/${lease.metadata.name}`,
  );
  const processed = Promise.withResolvers();
  a.onLost((executionId) => {
    if (executionId === other) processed.resolve();
  });
  // Controlled watch ordering: observe both changes before releasing the old LIST snapshot.
  state.watchEvents = [
    { type: "ADDED", object: lease },
    { type: "DELETED", object: lease },
    watchSentinel(objects, other),
  ];
  void a.watch();
  await waitForControl(processed.promise, AbortSignal.timeout(5000));
  assert.equal(a.lookup(id), undefined);
  assert.equal(a.findCapability(token), undefined);
  resume.resolve();
  await refreshing;
  assert.equal(a.lookup(id), undefined);
  assert.equal(a.findCapability(token), undefined);
});

test("a delayed old-UID watch deletion cannot evict a recreated epoch-one execution", async (t) => {
  const { a, objects, state } = await fixture(t, { maxFinished: 1 });
  for (const executionId of [id, other]) {
    await a.resolve(executionId, true);
    await a.journal.create(executionId, { phase: "finished" });
    await a.complete(executionId);
  }
  const old = structuredClone(
    [...objects.values()].find(
      (value) =>
        value.kind === "Lease" &&
        value.metadata.labels["browser-sandbox.io/execution"] === id,
    ),
  );
  await a.compact();
  const next = await a.resolve(id, true);
  assert.equal(next.epoch, 1);
  assert.notEqual(old.metadata.uid, next.uid);
  await a.resolve(other, true);
  const processed = Promise.withResolvers();
  a.onLost((executionId) => {
    if (executionId === other) processed.resolve();
  });
  state.watchEvents = [{ type: "DELETED", object: old }, watchSentinel(objects, other)];
  void a.watch();
  await waitForControl(processed.promise, AbortSignal.timeout(5000));
  assert.equal(a.owns(id), true);
});

test("malformed watch deletions are ignored without discarding valid subsequent events", async (t) => {
  const { a, objects, state } = await fixture(t);
  await a.resolve(id, true);
  await a.resolve(other, true);
  const processed = Promise.withResolvers();
  a.onLost((executionId) => {
    if (executionId === other) processed.resolve();
  });
  state.watchEvents = [
    {
      type: "DELETED",
      object: {
        metadata: {
          labels: { "browser-sandbox.io/execution": id },
          annotations: { "browser-sandbox.io/epoch": "invalid" },
        },
      },
    },
    watchSentinel(objects, other),
  ];
  void a.watch();
  await waitForControl(processed.promise, AbortSignal.timeout(5000));
  assert.equal(a.owns(id), true);
});

test("shared execution resources belong to the release and Secrets follow the execution Lease UID", async (t) => {
  const { a, objects } = await fixture(t);
  await a.resolve(id, true);
  await a.journal.create(id, { phase: "registered", profileId: "owned-profile" });
  await a.acquireProfile(id, "owned-profile");
  await a.finishMigration();
  const resources = [...objects.values()];
  const lease = resources.find(
    (value) =>
      value.kind === "Lease" &&
      value.metadata.labels["browser-sandbox.io/state"] === "execution",
  );
  const secret = resources.find(
    (value) =>
      value.kind === "Secret" &&
      value.metadata.labels["browser-sandbox.io/state"] === "execution",
  );
  for (const resource of resources.filter(
    (value) => ["Lease", "Secret"].includes(value.kind) && value !== secret,
  )) {
    assert.deepEqual(resource.metadata.ownerReferences, [
      {
        apiVersion: "v1",
        kind: "ConfigMap",
        name: "workers",
        uid: "release-uid",
        controller: true,
        blockOwnerDeletion: false,
      },
    ]);
  }
  assert.deepEqual(secret.metadata.ownerReferences, [
    {
      apiVersion: "coordination.k8s.io/v1",
      kind: "Lease",
      name: lease.metadata.name,
      uid: lease.metadata.uid,
      controller: true,
      blockOwnerDeletion: false,
    },
  ]);
});

test("maintenance cleanup does not claim unready executions and API failures do not stop recovery", async (t) => {
  const { a, state, objects } = await fixture(t, { maxFinished: 1 });
  const third = "33333333-3333-4333-8333-333333333333";
  for (const executionId of [id, other, third]) {
    await a.resolve(executionId, true);
    await a.journal.create(executionId, { phase: "finished" });
    await a.complete(executionId);
  }
  // A controlled API failure on the first candidate must not block the next one.
  state.failGet = id;
  assert.deepEqual(await a.refresh(), []);
  assert.equal(a.owns(id), false);
  assert.equal(await a.journal.read(other), undefined);
  state.failGet = undefined;
  const secret = [...objects.values()].find(
    (value) => value.kind === "Secret" && value.metadata.name.endsWith(id),
  );
  const body = JSON.parse(Buffer.from(secret.data.record, "base64").toString());
  body.record.data.phase = "registered";
  secret.data.record = Buffer.from(JSON.stringify(body)).toString("base64");
  await a.refresh();
  assert.equal(
    a.owns(id),
    false,
    "maintenance must never keep an unloaded binding owned",
  );
  assert.equal((await a.journal.read(id)).data.phase, "registered");
});

test("Secret snapshot precedes fresh Lease check even when the response arrives after handoff", async (t) => {
  const { a, b, state, objects } = await fixture(t);
  await a.resolve(id, true);
  await a.journal.create(id, { phase: "registered", marker: "original" });
  await b.resolve(id);
  const entered = Promise.withResolvers(),
    resume = Promise.withResolvers();
  state.readAfterGet = true;
  state.beforeGet = async (path) => {
    if (!path.includes("/secrets/") || !path.endsWith(id)) return;
    state.beforeGet = undefined;
    entered.resolve();
    await resume.promise;
  };
  const old = a.journal.update(id, 1, { phase: "registered", marker: "stale" });
  const rejection = assert.rejects(old, /browser_controller_fenced/);
  await entered.promise;
  // Synthetic release visible in the API before the old controller drops local state.
  const lease = [...objects.values()].find(
    (v) =>
      v.kind === "Lease" && v.metadata.labels["browser-sandbox.io/execution"] === id,
  );
  lease.spec.holderIdentity = "";
  lease.metadata.resourceVersion += "-released";
  await b.refresh();
  await b.resolve(id);
  resume.resolve();
  await rejection;
  assert.equal((await b.journal.read(id)).data.marker, "original");
});

test("handoff CAS retries preserve a concurrent local record revision", async (t) => {
  const { a, b, state, objects } = await fixture(t);
  await a.resolve(id, true);
  await a.journal.create(id, { phase: "registered", marker: "original" });
  await b.resolve(id);
  const lease = [...objects.values()].find(
    (v) =>
      v.kind === "Lease" && v.metadata.labels["browser-sandbox.io/execution"] === id,
  );
  lease.spec.holderIdentity = "";
  lease.metadata.resourceVersion += "-released";
  await b.refresh();
  const entered = Promise.withResolvers(),
    resume = Promise.withResolvers();
  state.beforeWrite = async (path) => {
    if (!path.includes("/secrets/")) return;
    state.beforeWrite = undefined;
    entered.resolve();
    await resume.promise;
  };
  const claiming = b.resolve(id);
  await entered.promise;
  await b.journal.update(id, 1, { phase: "registered", marker: "concurrent" });
  resume.resolve();
  await claiming;
  const saved = await b.journal.read(id);
  assert.equal(saved.revision, 2);
  assert.equal(saved.data.marker, "concurrent");
});

test("a late absent Secret POST retains old UID and handoff removes it", async (t) => {
  const { a, b, objects, state } = await fixture(t);
  const entered = Promise.withResolvers(),
    resume = Promise.withResolvers();
  state.beforePost = async (path) => {
    if (!path.endsWith("/secrets")) return;
    state.beforePost = undefined;
    entered.resolve();
    await resume.promise;
  };
  const old = a.resolve(id, true);
  const rejected = assert.rejects(old, /browser_controller_fenced/);
  await entered.promise;
  const leaseEntry = [...objects].find(
    ([, value]) =>
      value.kind === "Lease" &&
      value.metadata.labels["browser-sandbox.io/execution"] === id,
  );
  const oldUid = leaseEntry[1].metadata.uid;
  objects.delete(leaseEntry[0]);
  resume.resolve();
  await rejected;
  const stale = [...objects.values()].find((value) => value.kind === "Secret");
  assert.equal(stale.metadata.ownerReferences[0].uid, oldUid);
  const next = await b.resolve(id, true);
  assert.equal(next.epoch, 1);
  assert.notEqual(next.uid, oldUid);
  const fresh = [...objects.values()].find((value) => value.kind === "Secret");
  assert.equal(fresh.metadata.ownerReferences[0].uid, next.uid);
  assert.notEqual(fresh.metadata.uid, stale.metadata.uid);
  await b.journal.create(id, { phase: "registered" });
});

test("recreated UID fences an old Profile PUT after a free tombstone", async (t) => {
  const { a, b, objects, state, gc } = await fixture(t);
  await a.resolve(id, true);
  await a.journal.create(id, { phase: "registered", profileId: "same-id" });
  await a.acquireProfile(id, "same-id");
  const entered = Promise.withResolvers(),
    resume = Promise.withResolvers();
  state.beforeWrite = async (path) => {
    if (!path.includes("-p-")) return;
    state.beforeWrite = undefined;
    entered.resolve();
    await resume.promise;
  };
  await a.journal.update(id, 1, { phase: "finished", profileId: "same-id" });
  const old = a.releaseProfile(id, "same-id");
  const rejected = assert.rejects(
    old,
    /browser_profile_busy|browser_controller_fenced/,
  );
  await entered.promise;
  const leaseEntry = [...objects].find(
    ([, value]) =>
      value.kind === "Lease" &&
      value.metadata.labels["browser-sandbox.io/execution"] === id,
  );
  const oldUid = leaseEntry[1].metadata.uid;
  objects.delete(leaseEntry[0]);
  gc();
  const profile = [...objects.values()].find(
    (value) =>
      value.kind === "Lease" &&
      value.metadata.labels["browser-sandbox.io/state"] === "profile",
  );
  profile.spec.holderIdentity = "";
  profile.metadata.resourceVersion += "-free";
  const next = await b.resolve(id, true);
  assert.equal(next.epoch, 1);
  assert.notEqual(next.uid, oldUid);
  await b.journal.create(id, { phase: "registered", profileId: "same-id" });
  await b.acquireProfile(id, "same-id");
  resume.resolve();
  await rejected;
  const held = [...objects.values()].find(
    (value) =>
      value.kind === "Lease" &&
      value.metadata.labels["browser-sandbox.io/state"] === "profile",
  );
  assert.equal(held.spec.holderIdentity, id);
  assert.equal(held.metadata.annotations["browser-sandbox.io/execution-uid"], next.uid);
});

test("recovery cannot reinsert a binding after ownership loss", async (t) => {
  const { a, b, ports, advance, cleanups } = await fixture(t);
  await a.resolve(id, true);
  await a.journal.create(id, {
    phase: "active",
    incarnation: other,
    allowedDomains: [],
    registeredAt: Date.now(),
    lastActivityAt: Date.now(),
    grants: [],
    workerProtocol: "browser-sandbox.worker/v2",
    workerId: "worker/uid",
    generation: "original",
    endpoint: {
      cdpUrl: `ws://127.0.0.1:1/execution/${"a".repeat(64)}`,
      viewerToken: "b".repeat(64),
    },
  });
  const entered = Promise.withResolvers(),
    resume = Promise.withResolvers();
  const worker = {
    id: "worker/uid",
    executionId: id,
    incarnation: other,
    origin: "http://127.0.0.1:1",
    epoch: 1,
    runtime: {
      close() {},
      setEpoch() {},
      setAutomation() {},
      async request() {
        entered.resolve();
        await resume.promise;
        return {
          protocol: "browser-sandbox.worker/v2",
          session: { generation: "original", state: "live" },
        };
      },
    },
  };
  const starting = createController({
    ownership: a,
    peerPort: ports[0],
    port: 0,
    backend: {
      list: async () => [worker],
      fence: async () => {},
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
  await waitForControl(entered.promise, AbortSignal.timeout(5000));
  await b.resolve(id);
  advance(15001);
  await b.resolve(id);
  await a.refresh();
  assert.equal(a.owns(id), false);
  resume.resolve();
  const c = await starting;
  assert.equal(c.bindings.has(id), false);
});

test("ambiguous Secret owners fail closed and never delete the persisted record", async (t) => {
  const { a, objects, advance, b } = await fixture(t);
  await a.resolve(id, true);
  const record = await a.journal.create(id, {
    phase: "registered",
    marker: "preserved",
  });
  const secret = [...objects.values()].find(
    (value) => value.kind === "Secret" && value.metadata.name.endsWith(id),
  );
  secret.metadata.ownerReferences.push({
    apiVersion: "v1",
    kind: "ConfigMap",
    name: "workers",
    uid: "release-uid",
    controller: false,
  });
  await b.resolve(id);
  advance(15001);
  await assert.rejects(b.resolve(id), /browser_controller_state_corrupt/);
  assert.equal([...objects.values()].includes(secret), true);
  assert.deepEqual(
    JSON.parse(Buffer.from(secret.data.record, "base64").toString()).record,
    record,
  );
});

test("orphan recovery releases its binding and admission slot when ownership is lost during status", async (t) => {
  const { a, b, ports, advance, cleanups } = await fixture(t);
  const entered = Promise.withResolvers(),
    resume = Promise.withResolvers();
  let workers = [],
    closed = 0;
  const worker = {
    id: "orphan/uid",
    executionId: id,
    incarnation: other,
    origin: "http://127.0.0.1:1",
    epoch: 1,
    runtime: {
      close() {
        closed++;
      },
      setEpoch() {},
      setAutomation() {},
      async request() {
        entered.resolve();
        await resume.promise;
        return { protocol: "browser-sandbox.worker/v2" };
      },
    },
  };
  const c = await createController({
    ownership: a,
    peerPort: ports[0],
    port: 0,
    reconciliationMs: 60000,
    backend: {
      list: async () => workers,
      fence: async () => {},
      create: async () => {
        throw Error("unexpected");
      },
      remove: async () => {},
    },
  });
  cleanups.unshift(async () => {
    resume.resolve();
    await c.stop({ finalize: false }).catch(() => {});
  });
  workers = [worker];
  const recovering = c.reconcile();
  recovering.catch(() => {});
  await waitForControl(entered.promise, AbortSignal.timeout(5000));
  await b.resolve(id);
  advance(15001);
  await b.resolve(id);
  await a.refresh();
  resume.resolve();
  await recovering.catch(() => {});
  assert.equal(c.bindings.has(id), false);
  assert.equal(c.capacity().workers, 0);
  assert(closed > 0);
});

for (const restoring of [false, true])
  test(`a delayed successful Profile acquisition cannot restore local state (${restoring ? "recovery" : "registration"})`, async (t) => {
    const { a, b, ports, advance, cleanups } = await fixture(t);
    const entered = Promise.withResolvers(),
      resume = Promise.withResolvers();
    const ownership = {
      ...a,
      async acquireProfile(...args) {
        await a.acquireProfile(...args);
        entered.resolve();
        await resume.promise;
      },
    };
    if (restoring) {
      await a.resolve(id, true);
      await a.journal.create(id, {
        phase: "registered",
        profileId: "delayed",
        profileAcquired: true,
        storageMode: "volume",
        incarnation: other,
        allowedDomains: [],
        registeredAt: Date.now(),
        lastActivityAt: Date.now(),
        grants: [],
      });
    }
    let busy;
    const creating = createController({
      ownership,
      peerPort: ports[0],
      port: 0,
      reconciliationMs: 60000,
      profileStore: {
        open: () => ({}),
        expire: async (_signal, isBusy) => {
          busy = isBusy("delayed");
          return 0;
        },
      },
      backend: {
        list: async () => [],
        create: async () => {
          throw Error("unexpected");
        },
        remove: async () => {},
      },
    });
    let c;
    cleanups.unshift(async () => {
      resume.resolve();
      c ??= await creating;
      await c.stop({ finalize: false });
    });
    const registering = restoring
      ? creating
      : creating.then((value) => {
          c = value;
          return c.command({ op: "register", executionId: id, profileId: "delayed" });
        });
    registering.catch(() => {});
    await waitForControl(entered.promise, AbortSignal.timeout(5000));
    await b.resolve(id);
    advance(15001);
    await b.resolve(id);
    await a.refresh();
    resume.resolve();
    await registering.catch(() => {});
    c ??= await creating;
    assert.equal(c.bindings.has(id), false);
    await c.expireProfiles();
    assert.equal(busy, false);
  });

test("a controller can register a compacted finished id under its new Lease UID", async (t) => {
  const { a, ports, cleanups } = await fixture(t, { maxFinished: 1 });
  const c = await createController({
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
  cleanups.unshift(() => c.stop({ finalize: false }));
  const peer = { signal: new AbortController().signal, callback: async () => {} };
  for (const executionId of [id, other]) {
    await c.command({ op: "register", executionId }, peer);
    await c.command({ op: "finalize", executionId }, peer);
  }
  const oldUid = a.lookup(id).uid;
  await a.compact();
  assert.equal(a.lookup(id), undefined);
  await c.command({ op: "register", executionId: id }, peer);
  assert.equal(a.lookup(id).epoch, 1);
  assert.notEqual(a.lookup(id).uid, oldUid);
  assert.equal(c.bindings.get(id).phase, "registered");
});

test("a recreated record from another replica clears the former controller’s finished cache", async (t) => {
  const { a, b, ports, advance, cleanups } = await fixture(t, { maxFinished: 1 });
  const backend = {
    list: async () => [],
    create: async () => {
      throw Error("unexpected");
    },
    remove: async () => {},
  };
  const c = await createController({
    ownership: a,
    peerPort: ports[0],
    port: 0,
    reconciliationMs: 60000,
    backend,
  });
  cleanups.unshift(() => c.stop({ finalize: false }));
  const peer = { signal: new AbortController().signal, callback: async () => {} };
  for (const executionId of [id, other]) {
    await c.command({ op: "register", executionId }, peer);
    await c.command({ op: "finalize", executionId }, peer);
  }
  await a.compact();
  await b.resolve(id, true);
  await b.journal.create(id, {
    phase: "registered",
    incarnation: other,
    allowedDomains: [],
    registeredAt: Date.now(),
    lastActivityAt: Date.now(),
    grants: [],
  });
  await a.refresh();
  advance(15001);
  await a.resolve(id);
  await c.command({ op: "register", executionId: id }, peer);
  assert.equal(c.bindings.get(id).phase, "registered");
});

test("failed shared recovery closes the worker channel restored before a later corrupt record", async (t) => {
  const { a, ports } = await fixture(t);
  for (const executionId of [id, other]) await a.resolve(executionId, true);
  await a.journal.create(id, {
    phase: "active",
    incarnation: other,
    allowedDomains: [],
    registeredAt: Date.now(),
    lastActivityAt: Date.now(),
    grants: [],
    workerProtocol: "browser-sandbox.worker/v2",
    workerId: "worker/uid",
    generation: "original",
  });
  await a.journal.create(other, { phase: "active" });
  let closed = 0;
  const worker = {
    id: "worker/uid",
    executionId: id,
    incarnation: other,
    origin: "http://127.0.0.1:1",
    epoch: 1,
    runtime: {
      close() {
        closed++;
      },
      setEpoch() {},
      setAutomation() {},
      request: async () => ({
        protocol: "browser-sandbox.worker/v2",
        session: { state: "live", generation: "original" },
      }),
    },
  };
  await assert.rejects(
    createController({
      ownership: a,
      peerPort: ports[0],
      port: 0,
      backend: {
        list: async () => [worker],
        fence: async () => {},
        create: async () => {
          throw Error("unexpected");
        },
        remove: async () => {},
      },
    }),
    /browser_execution_journal_corrupt/,
  );
  assert(closed > 0);
});

test("a recreated Lease cannot inherit an old UID Secret’s record or grants while GC is pending", async (t) => {
  const { a, b, objects } = await fixture(t);
  const prior = await a.resolve(id, true);
  // Synthetic retained old record: GC has not removed this UID's Secret yet.
  await a.journal.create(id, {
    phase: "registered",
    grants: [{ token: "a".repeat(64) }],
    marker: "old-identity",
  });
  const oldSecret = [...objects.values()].find((value) => value.kind === "Secret");
  const leasePath = [...objects].find(
    ([, value]) => value.kind === "Lease" && value.metadata.uid === prior.uid,
  )[0];
  objects.delete(leasePath);
  const current = await b.resolve(id, true);
  assert.equal(current.epoch, 1);
  assert.notEqual(current.uid, prior.uid);
  assert.equal(await b.journal.read(id), undefined);
  const newSecret = [...objects.values()].find((value) => value.kind === "Secret");
  assert.notEqual(newSecret.metadata.uid, oldSecret.metadata.uid);
  await b.journal.create(id, {
    phase: "registered",
    grants: [],
    marker: "new-identity",
  });
  await assert.rejects(
    a.journal.update(id, 1, { phase: "registered", marker: "stale" }),
    /browser_controller_fenced/,
  );
  assert.deepEqual((await b.journal.read(id)).data.grants, []);
});
