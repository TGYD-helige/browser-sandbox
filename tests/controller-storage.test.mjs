import assert from "node:assert/strict";
import { randomBytes, randomUUID } from "node:crypto";
import { once } from "node:events";
import fs from "node:fs/promises";
import { createServer } from "node:http";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { archiveFixture } from "../.github/scripts/benchmark-storage.mjs";
import { createController } from "../dist/controller.js";
import { createControllerRuntime } from "../dist/network-control.js";
import { createProfileStore } from "../dist/profile-store.js";
import { createProfileArchive } from "../dist/profiles.js";

for (const mode of ["host", "volume"])
  test(`controller ${mode} storage preserves native Profile state across worker instances`, async (t) => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "controller-storage-"));
    t.after(() => fs.rm(root, { recursive: true, force: true }));
    const host = await archiveFixture(root);
    t.after(() => host.close());
    let store;
    const privateServer = createServer((req, res) => store.handle(req, res));
    privateServer.listen(0, "127.0.0.1");
    await once(privateServer, "listening");
    store = await createProfileStore({
      root: path.join(root, "store"),
      baseUrl: `http://127.0.0.1:${privateServer.address().port}`,
    });
    t.after(async () => {
      await store.close();
      privateServer.closeAllConnections();
      await new Promise((resolve) => privateServer.close(resolve));
    });
    const archive = createProfileArchive({
      browserVersion: async () => "Google Chrome 146.0.7680.177",
    });
    const workers = new Map();
    let removed = 0,
      finalizations = 0,
      failRemoval = false;
    const backend = {
      async list() {
        return [...workers.values()];
      },
      async create(placement) {
        const journalDir = path.join(root, placement.incarnation);
        await fs.mkdir(journalDir);
        const entry = {
          ...placement,
          journalDir,
          profileDir: path.join(journalDir, "profile"),
          uid: process.getuid(),
          gid: process.getgid(),
        };
        let adapter;
        let closed = false;
        let restored = false;
        const server = createServer(async (req, res) => {
          if (!restored) {
            await archive.restore(
              entry,
              await adapter.load(AbortSignal.timeout(5000)),
              AbortSignal.timeout(5000),
            );
            restored = true;
          }
          if (req.method === "PUT")
            await fs.writeFile(
              path.join(entry.profileDir, "state"),
              "confirmed native data",
            );
          res.end(
            await fs
              .readFile(path.join(entry.profileDir, "state"), "utf8")
              .catch((error) => {
                if (error.code === "ENOENT") return "absent";
                throw error;
              }),
          );
        });
        server.listen(0, "127.0.0.1");
        await once(server, "listening");
        const origin = `http://127.0.0.1:${server.address().port}`;
        const worker = {
          ...placement,
          id: placement.incarnation,
          origin,
          server,
          entry,
          runtime: {
            async attachStorage(registration, storage) {
              assert.equal(registration.profileId, placement.profileId);
              adapter = storage;
              return { close() {} };
            },
            close() {
              closed = true;
            },
            async request(request) {
              if (closed) throw Error("browser_control_disconnected");
              if (request.op === "register") {
                return {
                  cdpUrl:
                    origin.replace("http:", "ws:") + "/execution/" + "a".repeat(64),
                  viewerToken: "b".repeat(64),
                };
              }
              assert.equal(request.op, "finalize");
              finalizations++;
              const result = await archive.save(
                entry,
                await adapter.save(AbortSignal.timeout(5000)),
                AbortSignal.timeout(5000),
              );
              assert.equal(
                (await adapter.commit(result, AbortSignal.timeout(5000))).confirmedSha,
                result.archive.sha256,
              );
              return { ...result, stopped: true };
            },
          },
        };
        workers.set(worker.id, worker);
        return worker;
      },
      async remove(worker) {
        if (failRemoval) {
          failRemoval = false;
          throw Error("browser_synthetic_worker_removal_failed");
        }
        removed++;
        worker.server.closeAllConnections();
        await new Promise((resolve) => worker.server.close(resolve));
        await fs.rm(worker.entry.journalDir, { recursive: true });
        workers.delete(worker.id);
      },
    };
    const controller = await createController({
      port: 0,
      backend,
      profileStore: store,
    });
    t.after(async () => {
      try {
        await controller.stop();
      } finally {
        for (const worker of workers.values()) {
          worker.server.closeAllConnections();
          await new Promise((resolve) => worker.server.close(resolve));
        }
      }
    });
    const runtime = createControllerRuntime(controller.controlOrigin);
    t.after(() => runtime.close());
    let latest, key;
    let failCommit = false;
    const hostAdapter = {
      async load() {
        return { seed: latest ? { ...latest, key, url: host.url } : null };
      },
      async save() {
        key = randomBytes(32).toString("hex");
        return { target: { key, objectKey: "archive", url: host.url } };
      },
      async commit(result) {
        if (failCommit) throw Error("browser_control_disconnected");
        assert.equal(result.previousSha256, latest?.sha256 ?? null);
        latest = result.archive;
        return { confirmedSha: latest.sha256 };
      },
    };
    async function open() {
      const request = { executionId: randomUUID(), profileId: "account" };
      if (mode === "host") await runtime.attachStorage(request, hostAdapter);
      const endpoint = await runtime.request({
        op: "register",
        ...request,
        storage: mode === "host" ? true : undefined,
      });
      return {
        ...request,
        url: endpoint.wsEndpoint.replace("ws:", "http:") + "/files/state",
      };
    }
    const first = await open();
    assert.equal(workers.size, 0, "registration must not request archive data");
    assert.equal(
      await (await fetch(first.url, { method: "PUT" })).text(),
      "confirmed native data",
    );
    if (mode === "host") {
      failCommit = true;
      await assert.rejects(
        runtime.request({ op: "finalize", executionId: first.executionId }),
        /browser_control_disconnected/,
      );
      assert.equal(
        removed,
        0,
        "failed confirmation must preserve worker data and ownership",
      );
      await assert.rejects(open(), /browser_profile_busy/);
      failCommit = false;
    }
    failRemoval = true;
    await assert.rejects(
      runtime.request({ op: "finalize", executionId: first.executionId }),
      /browser_synthetic_worker_removal_failed/,
    );
    const saves = finalizations;
    await assert.rejects(open(), /browser_profile_busy/);
    const saved = await runtime.request({
      op: "finalize",
      executionId: first.executionId,
    });
    assert.equal(saved.stopped, true);
    assert.equal(saved.used, true);
    assert.match(saved.archive.sha256, /^[a-f0-9]{64}$/);
    assert.equal(
      finalizations,
      saves,
      "Cleanup retry must not repeat a confirmed save",
    );
    const next = await open();
    assert.equal(await (await fetch(next.url)).text(), "confirmed native data");
    await runtime.request({ op: "finalize", executionId: next.executionId });
    assert.equal(removed, 2);
  });
