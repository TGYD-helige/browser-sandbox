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
