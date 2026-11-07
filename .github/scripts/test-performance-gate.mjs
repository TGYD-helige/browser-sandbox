import assert from "node:assert/strict";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { runBatch } from "./benchmark-load.mjs";
import { comparePerformance } from "./compare-performance.mjs";

const report = (memory = 100 * 1024 * 1024) => ({
  architecture: "arm64",
  browserVersion: "Chrome/146",
  environment: "same runner",
  samples: 20,
  cases: [
    {
      name: "concurrent-temporary",
      concurrency: 4,
      phases: { batch: {} },
      measurements: { batch: Array(20).fill(100) },
      retainedMemoryBytes: Array(20).fill(memory),
    },
  ],
});

test("performance comparison rejects silently omitted preparation phases", () => {
  const baseline = report();
  const current = report();
  current.cases[0].phases.profileRestore = {};
  current.cases[0].measurements.profileRestore = Array(20).fill(100);
  assert.throws(
    () => comparePerformance(baseline, current),
    /Incompatible benchmark phases/,
  );
});

test("load memory has a relative budget and requires complete compatible samples", () => {
  assert.equal(comparePerformance(report(), report()).passed, true);
  assert.equal(comparePerformance(report(), report(200 * 1024 * 1024)).passed, false);
  const current = report();
  current.cases[0].retainedMemoryBytes.pop();
  assert.throws(() => comparePerformance(report(), current), /Missing memory samples/);
  current.cases[0].concurrency = 2;
  assert.throws(() => comparePerformance(report(), current), /concurrency/);
});

test("concurrent batches start together and drain all clients on failure", async () => {
  let release;
  const barrier = new Promise((resolve) => {
    release = resolve;
  });
  let started = 0;
  let completed = 0;
  const failure = new Error("renderer failed");
  const batch = runBatch(async (index) => {
    started++;
    if (index === 0) throw failure;
    await barrier;
    completed++;
  }, 4);
  assert.equal(started, 4);
  let settled = false;
  const rejected = assert
    .rejects(batch, (error) => error === failure)
    .then(() => {
      settled = true;
    });
  await delay(0);
  assert.equal(settled, false);
  release();
  await rejected;
  assert.equal(completed, 3);
  assert.deepEqual(await runBatch(async (index) => index, 4), [0, 1, 2, 3]);
});

test("extended resource budgets reject regressions, missing metrics and changed workloads", () => {
  const baseline = report(),
    current = report();
  baseline.cases[0].resources = {
    cpuMicros: Array(20).fill(100000),
    runtimeFdCount: Array(20).fill(50),
  };
  current.cases[0].resources = {
    cpuMicros: Array(20).fill(200000),
    runtimeFdCount: Array(20).fill(50),
  };
  assert.equal(comparePerformance(baseline, current).passed, false);
  current.cases[0].resources.cpuMicros.fill(100000);
  assert.equal(comparePerformance(baseline, current).passed, true);
  current.cases[0].resources.runtimeFdCount.fill(55);
  assert.equal(comparePerformance(baseline, current).passed, false);
  current.cases[0].resources.runtimeFdCount.pop();
  assert.throws(() => comparePerformance(baseline, current), /resource samples/);
  delete current.cases[0].resources.runtimeFdCount;
  assert.throws(() => comparePerformance(baseline, current), /Incompatible resources/);
  current.cases[0].operationsPerSample = 50;
  assert.throws(() => comparePerformance(baseline, current), /Incompatible workload/);
});

test("soak growth rejects accumulating descriptors, processes, temporary data and memory", async () => {
  const { resourceGrowth } = await import("./benchmark-soak.mjs");
  const rows = () =>
    Array.from({ length: 20 }, () => ({
      memoryBytes: 100000000,
      runtimeFdCount: 50,
      processCount: 5,
      diskBytes: 0,
    }));
  assert.deepEqual(resourceGrowth(rows()), {
    memoryBytes: 0,
    runtimeFdCount: 0,
    processCount: 0,
    diskBytes: 0,
  });
  for (const [key, growth] of [
    ["memoryBytes", 200000000],
    ["runtimeFdCount", 5],
    ["processCount", 3],
    ["diskBytes", 100000],
  ]) {
    const sample = rows();
    for (const row of sample.slice(-5)) row[key] += growth;
    assert.throws(() => resourceGrowth(sample), /grew|accumulated/);
  }
});

test("archive barriers expose first-chunk arrival, unblock on release and remove cancellation listeners", async () => {
  const { barrier } = await import("./benchmark-fixture.mjs");
  const { getEventListeners } = await import("node:events");
  const gate = barrier(),
    life = new AbortController();
  let completed = false;
  const held = gate.hold(life.signal).then(() => {
    completed = true;
  });
  await gate.entered;
  assert.equal(completed, false);
  gate.release();
  await held;
  assert.equal(getEventListeners(life.signal, "abort").length, 0);
  const cancelled = barrier(),
    abort = new AbortController();
  const rejected = assert.rejects(cancelled.hold(abort.signal), /cancelled/);
  await cancelled.entered;
  abort.abort(Error("cancelled"));
  await rejected;
  assert.equal(getEventListeners(abort.signal, "abort").length, 0);
});

test("file benchmark streams complete bytes and verifies downloaded content", async () => {
  const { createServer } = await import("node:http");
  const { once } = await import("node:events");
  const { upload, download } = await import("./benchmark-files.mjs");
  let data;
  const site = createServer(async (req, res) => {
    if (req.method === "PUT") {
      const chunks = [];
      for await (const chunk of req) chunks.push(chunk);
      data = Buffer.concat(chunks);
      res
        .writeHead(201, { "content-type": "application/json" })
        .end(JSON.stringify({ byteSize: data.length }));
    } else res.end(data);
  });
  site.listen(0, "127.0.0.1");
  await once(site, "listening");
  try {
    const url = `http://127.0.0.1:${site.address().port}/fixture`;
    const digest = await upload(url, 128 * 1024 + 3);
    await download(url, 128 * 1024 + 3, digest);
    data[0] ^= 1;
    await assert.rejects(download(url, data.length, digest));
  } finally {
    site.closeAllConnections();
    await new Promise((resolve) => site.close(resolve));
  }
});

test("the real archive fixture holds GET and PUT body transfer at an observable chunk", async () => {
  const { archiveFixture } = await import("./benchmark-storage.mjs");
  const { mkdtemp, rm } = await import("node:fs/promises");
  const { tmpdir } = await import("node:os");
  const { join } = await import("node:path");
  const root = await mkdtemp(join(tmpdir(), "archive-barrier-"));
  const site = await archiveFixture(root);
  try {
    const gate = site.hold("PUT");
    let completed = false;
    const put = fetch(site.url, {
      method: "PUT",
      body: "fixture-ciphertext",
      signal: AbortSignal.timeout(5000),
    }).then(async (response) => {
      assert.equal(response.status, 200);
      await response.arrayBuffer();
      completed = true;
    });
    try {
      await gate.entered;
      assert.equal(completed, false);
    } finally {
      gate.release();
    }
    await put;
    const read = site.hold("GET");
    const download = fetch(site.url, { signal: AbortSignal.timeout(5000) }).then(
      (response) => response.text(),
    );
    try {
      await read.entered;
    } finally {
      read.release();
    }
    assert.equal(await download, "fixture-ciphertext");
  } finally {
    await site.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("held transfers report an unexpected disconnect after admission", async () => {
  const { createServer } = await import("node:http");
  const { once } = await import("node:events");
  const { holdTransfer } = await import("./benchmark-files.mjs");
  let response;
  const site = createServer((_req, res) => {
    response = res;
    res.writeHead(200, { "content-length": 1024 * 1024 });
    res.write(Buffer.alloc(1024));
  });
  site.listen(0, "127.0.0.1");
  await once(site, "listening");
  const transfer = holdTransfer(`http://127.0.0.1:${site.address().port}/file`, "GET");
  try {
    await transfer.ready;
    response.destroy();
    await transfer.closed;
    await assert.rejects(transfer.close(), /aborted|closed|reset/i);
  } finally {
    await transfer.close().catch(() => {});
    site.closeAllConnections();
    await new Promise((resolve) => site.close(resolve));
  }
});

test("frame correlation waits for matching rendered content, not just a new frame", async () => {
  const { waitForMarker } = await import("./benchmark-viewer.mjs");
  let image = { data: "stale", receivedAt: 20, sequence: 1 };
  let entered;
  const firstDecoded = new Promise((resolve) => {
    entered = resolve;
  });
  const pending = waitForMarker({
    latest: () => image,
    expected: [230, 20, 30],
    updatedAt: 10,
    async decode(data) {
      if (data === "stale") {
        entered();
        return [20, 40, 230];
      }
      return [230, 20, 29];
    },
  });
  await firstDecoded;
  image = { data: "matching", receivedAt: 25, sequence: 2 };
  assert.equal((await pending).latency, 15);
});

test("held-slot acquisition waits for real admission and retains all sixteen slots", async () => {
  const { createServer } = await import("node:http");
  const { once } = await import("node:events");
  const { fillTransfers } = await import("./benchmark-files.mjs");
  let capacity = 0,
    active = 0,
    denied;
  const rejected = new Promise((resolve) => {
    denied = resolve;
  });
  const site = createServer((_req, res) => {
    if (active >= capacity) {
      res.writeHead(429).end();
      denied();
      return;
    }
    active++;
    res.once("close", () => active--);
    res.writeHead(200, { "content-length": 16 * 1024 * 1024 });
    res.write(Buffer.alloc(1024));
  });
  site.listen(0, "127.0.0.1");
  await once(site, "listening");
  let held = [];
  try {
    const base = `http://127.0.0.1:${site.address().port}/`;
    const pending = fillTransfers({ base, method: "GET" });
    await rejected;
    capacity = 16;
    held = await pending;
    assert.equal(active, 16);
    const extra = await fetch(base + "probe");
    await extra.arrayBuffer();
    assert.equal(extra.status, 429);
  } finally {
    await Promise.all(held.map((entry) => entry.close()));
    site.closeAllConnections();
    await new Promise((resolve) => site.close(resolve));
  }
});

test("real file routes recover between full download and incomplete upload cohorts", async () => {
  const { createGateway } = await import("../../dist/gateway.js");
  const { default: WebSocket, WebSocketServer } = await import("ws");
  const { mkdtemp, rm } = await import("node:fs/promises");
  const { tmpdir } = await import("node:os");
  const { join } = await import("node:path");
  const { randomUUID } = await import("node:crypto");
  const { fillTransfers, upload } = await import("./benchmark-files.mjs");
  const root = await mkdtemp(join(tmpdir(), "file-admission-"));
  const gateway = await createGateway({
    WebSocket,
    WebSocketServer,
    port: 0,
    socketPath: join(root, "control.sock"),
    dataRoot: join(root, "data"),
    access: { port: 0 },
    healthy: async () => true,
    launch: async () => {
      throw Error("Files must not launch a browser");
    },
  });
  let held = [];
  try {
    const executionId = randomUUID();
    await gateway.command({ op: "register", executionId, executorUid: 100096 }, true);
    const access = await gateway.command({ op: "access_create", executionId }, true);
    const base = access.cdpUrl.replace(/^ws/, "http") + "/files/";
    await upload(base + "held.bin", 16 * 1024 * 1024);
    for (const method of ["GET", "PUT", "GET", "PUT"]) {
      held = await fillTransfers({
        base,
        method,
        directory: join(root, "data", executionId, "files"),
      });
      const extra = await fetch(base + "held.bin");
      await extra.arrayBuffer();
      assert.equal(extra.status, 429);
      await Promise.all(held.map((entry) => entry.close()));
      held = [];
    }
  } finally {
    await Promise.all(held.map((entry) => entry.close()));
    await gateway.stop();
    await rm(root, { recursive: true, force: true });
  }
});

test("image verification cleans an acquired extraction container and preserves the original failure", async () => {
  const { mkdtemp, writeFile, readFile, rm } = await import("node:fs/promises");
  const { tmpdir } = await import("node:os");
  const { join } = await import("node:path");
  const { execFile } = await import("node:child_process");
  const { promisify } = await import("node:util");
  const root = await mkdtemp(join(tmpdir(), "image-cleanup-"));
  try {
    const log = join(root, "docker.log");
    await writeFile(
      join(root, "docker"),
      '#!/usr/bin/env bash\nprintf "%s\\n" "$*" >> "$DOCKER_CALL_LOG"\nif [[ "$1" == image ]];then exit 0;fi\nif [[ "$1" == create ]];then echo fixture-extract;exit 0;fi\nif [[ "$1" == cp ]];then exit 7;fi\nif [[ "$1" == rm ]];then exit 0;fi\nexit 99\n',
      { mode: 0o755 },
    );
    await assert.rejects(
      promisify(execFile)(
        "bash",
        [".github/scripts/verify-image.sh", "fixture:image"],
        {
          env: {
            ...process.env,
            PATH: root + ":" + process.env.PATH,
            DOCKER_CALL_LOG: log,
          },
        },
      ),
      (error) => error.code === 7,
    );
    assert((await readFile(log, "utf8")).includes("rm -f fixture-extract"));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("benchmark affinity selects two permitted CPUs without assuming CPU zero is available", async () => {
  const { selectBenchmarkCpus } = await import("./benchmark-cpu.mjs");
  assert.equal(selectBenchmarkCpus("0-3\n"), "0,1");
  assert.equal(selectBenchmarkCpus("2,5-7"), "2,5");
  assert.equal(selectBenchmarkCpus("4-5,8-11"), "4,5");
  assert.throws(() => selectBenchmarkCpus("3"), /two available CPUs/);
  assert.throws(() => selectBenchmarkCpus("invalid"), /Invalid effective CPU set/);
});

test("failed benchmarks preserve completed raw samples without serializing secrets", async () => {
  const { writeBenchmarkReport, collectBenchmark } = await import(
    "./benchmark-report.mjs"
  );
  const state = { samples: 20, cases: [] };
  const output = [];
  const failure = new Error("https://private.invalid/?token=secret");
  failure.code = "ERR_ASSERTION";
  await assert.rejects(
    writeBenchmarkReport(
      state,
      async () => {
        await collectBenchmark(state, "completed", async () => ({
          name: "completed",
          measurements: { batch: [12, 13] },
        }));
        await collectBenchmark(state, "failing-workload", async () => {
          throw failure;
        });
      },
      (json) => output.push(json),
    ),
    (error) => error === failure,
  );
  assert.equal(output.length, 1);
  const partial = JSON.parse(output[0]);
  assert.equal(partial.status, "failed");
  assert.equal(partial.activeWorkload, "failing-workload");
  assert.deepEqual(partial.cases[0].measurements.batch, [12, 13]);
  assert.deepEqual(partial.failure, { code: "ERR_ASSERTION" });
  assert(!output[0].includes("secret"));
  const complete = { cases: [] };
  await writeBenchmarkReport(
    complete,
    async () => {},
    (json) => output.push(json),
  );
  assert.equal(JSON.parse(output[1]).status, "complete");
});

test("soak assertion diagnostics retain in-progress samples and completed subcases", async () => {
  const { writeBenchmarkReport, collectBenchmark, trackBenchmark } = await import(
    "./benchmark-report.mjs"
  );
  const { resourceGrowth } = await import("./benchmark-soak.mjs");
  const state = { cases: [] };
  let output;
  await assert.rejects(
    writeBenchmarkReport(
      state,
      async () => {
        await collectBenchmark(state, "extended", async () => {
          state.pendingCases.push({
            name: "previous-subcase",
            measurements: { batch: [7] },
          });
          const sampled = {
            memoryBytes: [],
            runtimeFdCount: [],
            processCount: [],
            diskBytes: [],
          };
          const measurements = { cycleBatch: [] };
          trackBenchmark(state, "soak-temporary", measurements, sampled);
          const rows = [];
          for (let sample = 0; sample < 20; sample++) {
            state.sample = sample;
            const row = {
              memoryBytes: 1000,
              runtimeFdCount: 20 + sample,
              processCount: 4,
              diskBytes: 0,
            };
            rows.push(row);
            measurements.cycleBatch.push(10);
            for (const key of Object.keys(sampled)) sampled[key].push(row[key]);
          }
          resourceGrowth(rows);
          return [];
        });
      },
      (json) => {
        output = JSON.parse(json);
      },
    ),
    /file descriptors grew/,
  );
  assert.equal(output.status, "failed");
  assert.equal(output.sample, 19);
  assert.equal(output.pendingCases[0].name, "previous-subcase");
  assert.equal(output.current.name, "soak-temporary");
  assert.deepEqual(output.current.measurements.cycleBatch, Array(20).fill(10));
  assert.deepEqual(
    output.current.resources.runtimeFdCount,
    Array.from({ length: 20 }, (_, index) => 20 + index),
  );
});

test("a failed report cannot pass comparison even with all duration samples", () => {
  const partial = { ...report(), status: "failed" };
  assert.throws(() => comparePerformance(report(), partial), /Incomplete benchmark/);
  assert.throws(() => comparePerformance(partial, report()), /Incomplete benchmark/);
});
