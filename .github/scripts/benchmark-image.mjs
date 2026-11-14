import assert from "node:assert/strict";
import { fork } from "node:child_process";
import { createHash, randomBytes, randomUUID } from "node:crypto";
import { once } from "node:events";
import { createReadStream, createWriteStream } from "node:fs";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { arch, cpus, release, tmpdir } from "node:os";
import { join } from "node:path";
import { pipeline } from "node:stream/promises";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath, pathToFileURL } from "node:url";
import { connectCdp } from "../../dist/cdp.js";
import { attachStorage, hostRequest } from "../../dist/host-client.js";
import { readCpuAccounting } from "./benchmark-cpu.mjs";
import { benchmarkExtended } from "./benchmark-extended.mjs";
import { execution } from "./benchmark-fixture.mjs";
import { benchmarkLoad } from "./benchmark-load.mjs";
import {
  collectBenchmark,
  trackBenchmark,
  writeBenchmarkReport,
} from "./benchmark-report.mjs";
import { benchmarkSoak } from "./benchmark-soak.mjs";

export function createBenchmarkSite(root) {
  const transfers = { download: [], upload: [] };
  const site = createServer(async (req, res) => {
    if (req.url === "/page" && req.method === "GET") {
      res
        .writeHead(200, { "content-type": "text/html" })
        .end("<h1>Browser benchmark</h1>");
      return;
    }
    if (req.url !== "/archive") {
      res.writeHead(404).end();
      return;
    }
    if (req.method !== "GET" && req.method !== "PUT") {
      res.writeHead(405).end();
      return;
    }
    const start = performance.now();
    try {
      if (req.method === "PUT") {
        await pipeline(req, createWriteStream(join(root, "archive")));
        res.writeHead(200).end();
        transfers.upload.push(performance.now() - start);
      } else {
        await pipeline(createReadStream(join(root, "archive")), res);
        transfers.download.push(performance.now() - start);
      }
    } catch {
      res.destroy();
    }
  });
  return { site, transfers };
}

if (process.argv[2] === "worker") {
  let cdp, sessionId;
  process.on("message", async ({ action, url, bytes, cookieCount = 0 }) => {
    let phase = "command";
    try {
      let result;
      if (action === "unused") {
        result = true;
      } else if (action === "ready") {
        phase = "connect";
        cdp = await connectCdp(process.env.BROWSER_WS_ENDPOINT);
        phase = "target";
        const { targetId } = await cdp.call("Target.createTarget", { url });
        phase = "attach";
        ({ sessionId } = await cdp.call("Target.attachToTarget", {
          targetId,
          flatten: true,
        }));
        phase = "page-ready";
        let pageReady = false;
        for (let i = 0; i < 200; i++) {
          const { result: ready } = await cdp.call(
            "Runtime.evaluate",
            {
              expression: `location.href === ${JSON.stringify(url)} && document.readyState === 'complete'`,
              returnByValue: true,
            },
            sessionId,
          );
          if (ready.value) {
            pageReady = true;
            break;
          }
          await delay(25);
        }
        assert(pageReady, "Benchmark page did not become ready");
        phase = "screenshot";
        const screenshot = await cdp.call("Page.captureScreenshot", {}, sessionId);
        assert(screenshot.data.length > 100);
        phase = "version";
        result = (await cdp.call("Browser.getVersion")).product;
      } else if (action === "seed" || action === "verify") {
        if (cookieCount) {
          if (action === "seed")
            await cdp.call("Storage.setCookies", {
              cookies: Array.from({ length: cookieCount }, (_, index) => ({
                name: `benchmark-${index}`,
                value: "fixture",
                domain: "127.0.0.1",
                path: "/",
                expires: Date.now() / 1000 + 3600,
              })),
            });
          else
            assert.equal(
              (await cdp.call("Storage.getCookies")).cookies.filter((cookie) =>
                cookie.name.startsWith("benchmark-"),
              ).length,
              cookieCount,
            );
        }
        const { result: value, exceptionDetails } = await cdp.call(
          "Runtime.evaluate",
          {
            expression: `(async () => {
            const db = await new Promise((resolve, reject) => {
              const request = indexedDB.open('benchmark', 1);
              request.onupgradeneeded = () => request.result.createObjectStore('data');
              request.onsuccess = () => resolve(request.result);
              request.onerror = () => reject(request.error);
            });
            try {
              const tx = db.transaction('data', ${JSON.stringify(action === "seed" ? "readwrite" : "readonly")});
              const store = tx.objectStore('data');
              ${action === "seed" ? `const data = new Uint8Array(${bytes}); for (let offset = 0; offset < data.length; offset += 65536) crypto.getRandomValues(data.subarray(offset, Math.min(offset + 65536, data.length))); store.put(data, 'payload'); await new Promise((resolve, reject) => { tx.oncomplete = resolve; tx.onerror = () => reject(tx.error); }); return data.byteLength;` : "return await new Promise((resolve, reject) => { const request = store.get('payload'); request.onsuccess = () => resolve(request.result?.byteLength); request.onerror = () => reject(request.error); });"}
            } finally { db.close(); }
          })()`,
            awaitPromise: true,
            returnByValue: true,
          },
          sessionId,
        );
        assert(!exceptionDetails);
        result = value.value;
      } else if (action === "close") {
        cdp.close();
        result = true;
      }
      process.send({ result });
    } catch (error) {
      const reason =
        [
          "browser_cdp_connect_failed",
          "browser_cdp_closed",
          "browser_cdp_timeout",
          "browser_cdp_operation_failed",
          "ERR_ASSERTION",
        ].find((value) => value === error?.message || value === error?.code) ??
        "unknown";
      process.send({ error: "browser_benchmark_worker_failed", phase, reason });
    }
  });
  process.on("disconnect", () => {
    cdp?.close();
    process.exit(0);
  });
} else if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const samples = Number(process.env.SAMPLES ?? 20);
  assert(Number.isInteger(samples) && samples >= 10 && samples <= 100);
  // standard is lifecycle followed by extended; CI runs those halves as parallel jobs.
  const suite = process.env.BENCHMARK_SUITE ?? "standard";
  assert(["standard", "lifecycle", "extended", "soak"].includes(suite));
  const root = await mkdtemp(join(tmpdir(), "browser-benchmark-"));
  const socket = process.env.BROWSER_CONTROL_SOCKET;
  const { site, transfers } = createBenchmarkSite(root);
  site.listen(18081, "127.0.0.1");
  await once(site, "listening");
  const origin = "http://127.0.0.1:18081";
  const summary = (values) => {
    const sorted = [...values].sort((a, b) => a - b);
    return {
      samples: sorted.length,
      medianMs: +sorted[Math.floor(sorted.length / 2)].toFixed(3),
      p95Ms: +sorted[Math.ceil(sorted.length * 0.95) - 1].toFixed(3),
    };
  };
  const report = { samples, suite, warmups: 3, cases: [] };
  const cases = report.cases;
  await writeBenchmarkReport(report, async () => {
    try {
      const cpuAccountingBefore = await readCpuAccounting();
      const cpuSet = (
        await readFile("/sys/fs/cgroup/cpuset.cpus.effective", "utf8")
      ).trim();
      Object.assign(report, {
        node: process.version,
        platform: process.platform,
        architecture: arch(),
        kernel: release(),
        cpu: cpus()[0]?.model,
        cpuSet,
        cpuAccounting: {
          before: cpuAccountingBefore,
        },
        environment:
          "Sandboxed image; 2 CPU affinity and quota, 2 GiB RAM; periodic Docker healthchecks disabled during measurement; real CDP renderer and local streaming HTTP archive store",
        note: "First readiness includes client startup, Profile restore, browser startup, navigation, and screenshot. Finalization includes checkpoint, browser exit, encryption/archive, upload, and commit. HTTP transfer timings are measured at the local storage server. Profile acquisition and source lookup/restore are measured inside the runtime. Browser launch includes Browserless queue wait. Unused executions use a generic Node client without connecting CDP; real harness startup is measured separately. These measurements do not establish production/cloud latency or a comparative performance claim.",
        cases,
      });
      if (suite === "standard" || suite === "lifecycle") {
        for (const [name, bytes] of [
          ["unused", -1],
          ["temporary", null],
          ["local-small", 1024],
          ["local-many-files", 1024 * 1024],
          ["remote-small", 1024],
          ["remote-large", 8 * 1024 * 1024],
        ]) {
          report.activeWorkload = name;
          console.error(`Benchmark workload: ${name}`);
          const remote = name === "unused" || name.startsWith("remote-");
          const profileId = bytes === null ? undefined : `benchmark/${randomUUID()}`;
          const key = randomBytes(32).toString("hex");
          let latest;
          let storageCalls = 0;
          const results = {
            register: [],
            profileAcquire: [],
            profileRestore: [],
            browserLaunch: [],
            firstBrowserReady: [],
            finalize: [],
            archiveDownload: [],
            archiveUpload: [],
          };
          trackBenchmark(report, name, results);
          for (let sample = -3; sample < samples; sample++) {
            report.sample = sample;
            const executionId = randomUUID();
            const registration = {
              executionId,
              executorUid: name.startsWith("local-")
                ? 100091 + ((sample + 3) % 2)
                : 100091,
              ...(profileId ? { profileId, ...(remote ? { storage: true } : {}) } : {}),
            };
            let storage, child;
            let finalized = false;
            try {
              let start = performance.now();
              const { wsEndpoint } = await hostRequest(socket, {
                op: "register",
                ...registration,
              });
              const registered = performance.now() - start;
              if (profileId && remote)
                storage = await attachStorage(socket, registration, {
                  async load() {
                    storageCalls++;
                    return {
                      seed: latest
                        ? { ...latest, key, url: origin + "/archive" }
                        : null,
                    };
                  },
                  async save() {
                    storageCalls++;
                    return {
                      target: { key, url: origin + "/archive", objectKey: "benchmark" },
                    };
                  },
                  async commit(result) {
                    storageCalls++;
                    assert.equal(result.previousSha256, latest?.sha256 ?? null);
                    latest = result.archive;
                    return { confirmedSha: latest.sha256 };
                  },
                });
              transfers.download.length = transfers.upload.length = 0;
              start = performance.now();
              child = fork(fileURLToPath(import.meta.url), ["worker"], {
                uid: registration.executorUid,
                gid: registration.executorUid,
                env: { PATH: process.env.PATH, BROWSER_WS_ENDPOINT: wsEndpoint },
                stdio: ["ignore", "ignore", "ignore", "ipc"],
              });
              const call = async (action, params = {}) => {
                const result = once(child, "message", {
                  signal: AbortSignal.timeout(60000),
                });
                child.send({ action, ...params });
                const [response] = await result;
                assert(!response.error, "Browser benchmark worker failed");
                return response.result;
              };
              const unused = name === "unused";
              const version = await call(unused ? "unused" : "ready", {
                url: origin + "/page",
              });
              if (!unused) report.browserVersion = version;
              const ready = performance.now() - start;
              if (unused)
                assert.equal(
                  (
                    await hostRequest(socket, {
                      op: "session",
                      executionId,
                      action: "status",
                    })
                  ).state,
                  "unused",
                );
              let preparation;
              if (!unused && process.env.REQUIRE_PREPARATION_TIMINGS !== "0") {
                const timingPath = `/run/browser-sandbox/benchmark/${executionId}.json`;
                for (let attempt = 0; attempt < 100; attempt++) {
                  try {
                    preparation = JSON.parse(await readFile(timingPath, "utf8"));
                    break;
                  } catch (error) {
                    if (error.code !== "ENOENT" && !(error instanceof SyntaxError))
                      throw error;
                    await delay(10);
                  }
                }
                assert(preparation, "Missing preparation timing record");
                await rm(timingPath);
              }
              if (profileId && !unused)
                assert.equal(
                  await call(sample === -3 ? "seed" : "verify", {
                    bytes,
                    cookieCount: name === "local-many-files" ? 100 : 0,
                  }),
                  bytes,
                );
              if (!unused) await call("close");
              const exited = once(child, "exit");
              child.disconnect();
              await exited;
              start = performance.now();
              await hostRequest(socket, { op: "finalize", executionId });
              finalized = true;
              if (name === "local-many-files" && sample === -3) {
                // Synthetic small files exercise ownership traversal independently of database packing.
                const profile = join(
                  "/browser-data/.profiles",
                  createHash("sha256").update(profileId).digest("hex"),
                  "profile",
                  "benchmark-files",
                );
                await mkdir(profile, { mode: 0o700 });
                for (let offset = 0; offset < 1000; offset += 4)
                  await Promise.all(
                    Array.from({ length: 4 }, (_, index) =>
                      writeFile(
                        join(profile, String(offset + index)),
                        Buffer.alloc(256),
                        { mode: 0o600 },
                      ),
                    ),
                  );
              }
              if (sample >= 0) {
                const expectedTransfers = profileId && remote && !unused ? 1 : 0;
                assert.equal(transfers.download.length, expectedTransfers);
                assert.equal(transfers.upload.length, expectedTransfers);
                results.register.push(registered);
                if (!unused) {
                  results.firstBrowserReady.push(ready);
                  if (preparation) {
                    results.profileAcquire.push(preparation.profileAcquireMs);
                    results.profileRestore.push(preparation.profileRestoreMs);
                    results.browserLaunch.push(preparation.browserLaunchMs);
                  }
                }
                results.finalize.push(performance.now() - start);
                results.archiveDownload.push(...transfers.download);
                results.archiveUpload.push(...transfers.upload);
              }
            } finally {
              child?.kill("SIGKILL");
              if (!finalized)
                await hostRequest(socket, { op: "finalize", executionId });
              storage?.close();
            }
          }
          if (name === "unused")
            assert.equal(storageCalls, 0, "Unused execution contacted storage");
          cases.push({
            storageCalls,
            measurements: results,
            name,
            payloadBytes: Math.max(0, bytes ?? 0),
            phases: Object.fromEntries(
              Object.entries(results)
                .filter(([, values]) => values.length)
                .map(([phase, values]) => [phase, summary(values)]),
            ),
          });
        }
        delete report.sample;
        await collectBenchmark(report, "concurrent-temporary", () =>
          benchmarkLoad({ socket, url: origin + "/page", samples, report }),
        );
      } else {
        report.activeWorkload = "browser-version";
        const probe = await execution(socket);
        try {
          report.browserVersion = (
            await (await probe.connect()).call("Browser.getVersion")
          ).product;
        } finally {
          await probe.finish();
        }
      }
      if (suite === "standard" || suite === "extended")
        await benchmarkExtended({ socket, origin, root, samples }, report);
      if (suite === "soak") {
        const cycles = Number(process.env.SOAK_CYCLES ?? 500);
        await collectBenchmark(report, "soak-temporary", () =>
          benchmarkSoak({ socket, origin, samples, cycles, report }),
        );
      }
      report.cpuAccounting.after = await readCpuAccounting();
    } finally {
      site.closeAllConnections();
      await new Promise((resolve) => site.close(resolve));
      await rm(root, { recursive: true, force: true });
    }
  });
}
