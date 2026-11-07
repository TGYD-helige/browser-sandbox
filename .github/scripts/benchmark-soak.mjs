import assert from "node:assert/strict";
import { lstat, readdir } from "node:fs/promises";
import { diskUsage, execution, resources, workload } from "./benchmark-fixture.mjs";
import { trackBenchmark } from "./benchmark-report.mjs";

export function resourceGrowth(rows) {
  assert(rows.length >= 10);
  const median = (values) =>
    [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)];
  const growth = Object.fromEntries(
    ["memoryBytes", "runtimeFdCount", "processCount", "diskBytes"].map((key) => [
      key,
      Math.max(
        0,
        median(rows.slice(-5).map((row) => row[key])) -
          median(rows.slice(0, 5).map((row) => row[key])),
      ),
    ]),
  );
  // Count growth is checked absolutely as well as against the baseline: both versions may leak.
  assert(
    growth.runtimeFdCount <= 4,
    "Runtime file descriptors grew across completed executions",
  );
  assert(growth.processCount <= 2, "Processes accumulated across completed executions");
  assert(growth.diskBytes <= 64 * 1024, "Temporary execution data accumulated on disk");
  assert(
    growth.memoryBytes <= 128 * 1024 * 1024,
    "Retained memory grew across completed executions",
  );
  return growth;
}

export async function benchmarkSoak({
  socket,
  origin,
  samples,
  cycles = samples,
  report,
}) {
  assert(Number.isInteger(cycles) && cycles >= samples && cycles % samples === 0);
  const measurements = { cycleBatch: [] },
    rows = [],
    sampled = { memoryBytes: [], runtimeFdCount: [], processCount: [], diskBytes: [] };
  trackBenchmark(report, "soak-temporary", measurements, sampled);
  const perSample = cycles / samples;
  for (let sample = -3; sample < samples; sample++) {
    if (report) report.sample = sample;
    const start = performance.now();
    for (let cycle = 0; cycle < perSample; cycle++) {
      const client = await execution(socket);
      try {
        await client.ready(origin + "/page");
      } finally {
        await client.finish();
      }
      await assert.rejects(lstat(`/browser-data/${client.executionId}`), {
        code: "ENOENT",
      });
    }
    const temporary = (await readdir("/browser-data")).filter(
      (name) => name !== ".profiles",
    );
    const resource = {
      ...(await resources()),
      diskBytes: (
        await Promise.all(temporary.map((name) => diskUsage(`/browser-data/${name}`)))
      ).reduce((a, b) => a + b, 0),
    };
    if (sample < 0) continue;
    measurements.cycleBatch.push(performance.now() - start);
    rows.push(resource);
    for (const key of Object.keys(sampled)) sampled[key].push(resource[key]);
  }
  const growth = resourceGrowth(rows);
  return workload("soak-temporary", measurements, sampled, {
    cycles,
    cyclesPerSample: perSample,
    warmupCycles: 3 * perSample,
    growth,
    note: "Completed browser lifecycles with real navigation and screenshots; execution directories must be absent after every finalize. Growth compares median first/last five batches. Runtime tombstones are bounded retained state, so memory is not expected to return byte-for-byte to its initial value.",
  });
}
