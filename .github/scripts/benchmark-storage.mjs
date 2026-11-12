import assert from "node:assert/strict";
import { randomBytes, randomUUID } from "node:crypto";
import { once } from "node:events";
import { createReadStream, createWriteStream } from "node:fs";
import { createServer } from "node:http";
import { join } from "node:path";
import { pipeline } from "node:stream/promises";
import { attachStorage } from "../../dist/host-client.js";
import { barrier, execution, workload } from "./benchmark-fixture.mjs";
import { trackBenchmark } from "./benchmark-report.mjs";

export async function archiveFixture(root) {
  const gates = new Map();
  const site = createServer(async (req, res) => {
    const life = new AbortController();
    const cancelled = () => {
      if (!res.writableFinished) life.abort(Error("archive_fixture_cancelled"));
    };
    res.once("close", cancelled);
    try {
      assert.equal(req.url, "/archive");
      assert(["GET", "PUT"].includes(req.method));
      const gate = gates.get(req.method);
      gates.delete(req.method);
      const pause = async function* (source) {
        let first = true;
        for await (const chunk of source) {
          if (first && gate) {
            first = false;
            await gate.hold(life.signal);
          }
          yield chunk;
        }
      };
      if (req.method === "PUT") {
        await pipeline(req, pause, createWriteStream(join(root, "isolated-archive")), {
          signal: life.signal,
        });
        res.writeHead(200).end();
      } else
        await pipeline(createReadStream(join(root, "isolated-archive")), pause, res, {
          signal: life.signal,
        });
    } catch {
      res.destroy();
    } finally {
      res.off("close", cancelled);
    }
  });
  site.listen(0, "127.0.0.1");
  await once(site, "listening");
  return {
    url: `http://127.0.0.1:${site.address().port}/archive`,
    hold(method) {
      const gate = barrier();
      gates.set(method, gate);
      return gate;
    },
    async close() {
      site.closeAllConnections();
      await new Promise((resolve) => site.close(resolve));
    },
  };
}

export async function benchmarkStorage({ socket, root, origin, samples, report }) {
  const site = await archiveFixture(root);
  const profileId = `isolation/${randomUUID()}`,
    key = randomBytes(32).toString("hex");
  let latest;
  const remote = async () => {
    const client = await execution(socket, { profileId, storage: true });
    let storage;
    try {
      storage = await attachStorage(socket, client.request, {
        async load() {
          return { seed: latest ? { ...latest, key, url: site.url } : null };
        },
        async save() {
          return { target: { key, url: site.url, objectKey: "isolation" } };
        },
        async commit(result) {
          assert.equal(result.previousSha256, latest?.sha256 ?? null);
          latest = result.archive;
          return { confirmedSha: latest.sha256 };
        },
      });
      return { client, storage };
    } catch (error) {
      storage?.close();
      await client.finish();
      throw error;
    }
  };
  try {
    const seeded = await remote();
    try {
      const { cdp, sessionId } = await seeded.client.ready(origin + "/page");
      await cdp.call(
        "Runtime.evaluate",
        { expression: "localStorage.setItem('isolation','saved')" },
        sessionId,
      );
      await seeded.client.finish();
    } finally {
      await seeded.client.finish();
      seeded.storage.close();
    }
    const cases = report?.pendingCases ?? [];
    for (const method of ["GET", "PUT"]) {
      const measurements = { register: [], firstBrowserReady: [], finalize: [] };
      trackBenchmark(report, `slow-storage-${method}`, measurements);
      for (let sample = -3; sample < samples; sample++) {
        if (report) report.sample = sample;
        const current = await remote();
        let gate, pending, other;
        try {
          if (method === "PUT") await current.client.ready(origin + "/page");
          gate = site.hold(method);
          pending =
            method === "GET"
              ? current.client.ready(origin + "/page")
              : current.client.finish();
          pending.catch(() => {});
          await Promise.race([
            gate.entered,
            pending.then(() => {
              throw Error("Transfer bypassed its barrier");
            }),
          ]);
          let start = performance.now();
          other = await execution(socket);
          const registered = performance.now() - start;
          start = performance.now();
          await other.ready(origin + "/page");
          const ready = performance.now() - start;
          start = performance.now();
          await other.finish();
          const finalized = performance.now() - start;
          // Release only after unrelated work finishes: a global control lock deadlocks this test.
          gate.release();
          const restored = await pending;
          if (method === "GET") {
            const { result } = await restored.cdp.call(
              "Runtime.evaluate",
              { expression: "localStorage.getItem('isolation')", returnByValue: true },
              restored.sessionId,
            );
            assert.equal(result.value, "saved");
          }
          if (sample >= 0) {
            measurements.register.push(registered);
            measurements.firstBrowserReady.push(ready);
            measurements.finalize.push(finalized);
          }
        } finally {
          gate?.release();
          await pending?.catch(() => {});
          await other?.finish();
          await current.client.finish();
          current.storage.close();
        }
      }
      cases.push(
        workload(
          `slow-storage-${method === "GET" ? "restore" : "upload"}`,
          measurements,
          {},
          {
            note: "The encrypted archive transfer is held at its first chunk until an unrelated execution registers, renders, and finalizes. Local controlled storage, not cloud network latency.",
          },
        ),
      );
    }
    return cases;
  } finally {
    await site.close();
  }
}
