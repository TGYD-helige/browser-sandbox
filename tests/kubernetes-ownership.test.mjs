import assert from "node:assert/strict";
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
    if (req.method === "GET") return reply(previous ? 200 : 404, previous);
    if (req.method === "POST") {
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
  await owner.journal.create(id, { phase: "registered" });
  await owner.acquireProfile(id, "profile-1");
  await peer.resolve(other, true);
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
  const record = await peer.journal.update(id, 1, { phase: "finished" });
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
  await b.journal.create(other, { phase: "finished", profileId: "stranded" });
  await b.acquireProfile(other, "stranded");
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
