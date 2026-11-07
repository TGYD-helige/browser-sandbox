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
