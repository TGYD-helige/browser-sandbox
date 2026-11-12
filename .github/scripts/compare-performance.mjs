import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

// Concatenate samples from repeated runs of one runtime, e.g. the two halves of an ABBA schedule.
export function mergeReports(runs) {
  assert(runs.length > 0, "No benchmark reports");
  const merged = structuredClone(runs[0]);
  for (const run of runs.slice(1)) {
    for (const key of [
      "status",
      "architecture",
      "browserVersion",
      "environment",
      "cpuSet",
    ])
      assert.equal(run[key], merged[key], `Inconsistent repeated benchmark ${key}`);
    merged.samples += run.samples;
    for (const entry of merged.cases) {
      const other = run.cases.find((candidate) => candidate.name === entry.name);
      assert(other, `Missing workload ${entry.name}`);
      for (const field of ["measurements", "resources"])
        for (const [key, values] of Object.entries(entry[field] ?? {}))
          values.push(...(other[field]?.[key] ?? []));
      entry.retainedMemoryBytes?.push(...(other.retainedMemoryBytes ?? []));
    }
  }
  return merged;
}

export function comparePerformance(baseline, current) {
  for (const report of [baseline, current])
    assert(
      report.status === undefined || report.status === "complete",
      "Incomplete benchmark report",
    );
  for (const key of ["architecture", "browserVersion", "environment", "cpuSet"])
    assert.equal(current[key], baseline[key], `Incompatible benchmark ${key}`);
  assert(
    baseline.samples >= 20 && current.samples === baseline.samples,
    "Use matching runs with at least 20 samples",
  );
  const comparisons = [];
  for (const before of baseline.cases) {
    const after = current.cases.find((entry) => entry.name === before.name);
    assert(after, `Missing workload ${before.name}`);
    for (const key of [
      "operationsPerSample",
      "bytesPerSample",
      "viewers",
      "observationWindowMs",
      "cycles",
      "cyclesPerSample",
    ])
      assert.equal(
        after[key],
        before[key],
        `Incompatible workload ${before.name}/${key}`,
      );
    assert.deepEqual(
      Object.keys(after.phases).sort(),
      Object.keys(before.phases).sort(),
      `Incompatible benchmark phases for ${before.name}`,
    );
    for (const phase of Object.keys(before.phases)) {
      const measured = after.phases[phase];
      assert(measured, `Missing phase ${before.name}/${phase}`);
      const oldSamples = before.measurements[phase],
        newSamples = after.measurements[phase];
      assert(
        oldSamples.length === baseline.samples && newSamples.length === current.samples,
        "Missing or extra phase samples",
      );
      for (const value of [...oldSamples, ...newSamples])
        assert(Number.isFinite(value) && value >= 0);
      const quantile = (values, q) =>
        [...values].sort((a, b) => a - b)[Math.ceil(values.length * q) - 1];
      const baselineMedianMs = quantile(oldSamples, 0.5),
        currentMedianMs = quantile(newSamples, 0.5),
        baselineP95Ms = quantile(oldSamples, 0.95),
        currentP95Ms = quantile(newSamples, 0.95);
      // Initial CI regression guard, relative to the same runner's baseline; not a production SLA.
      const medianLimitMs = baselineMedianMs * 1.25 + 25,
        p95LimitMs = baselineP95Ms * 1.25 + 25;
      comparisons.push({
        workload: before.name,
        phase,
        baselineMedianMs,
        currentMedianMs,
        medianLimitMs,
        baselineP95Ms,
        currentP95Ms,
        p95LimitMs,
        // With 20 samples P95 is the second-largest value; a tail alone is runner noise, a shifted median too is a regression.
        passed: currentMedianMs <= medianLimitMs || currentP95Ms <= p95LimitMs,
      });
    }
  }
  assert(comparisons.length > 0, "No comparable measurements");
  const resources = [];
  for (const before of baseline.cases.filter((entry) => entry.retainedMemoryBytes)) {
    const after = current.cases.find((entry) => entry.name === before.name);
    assert.equal(
      after.concurrency,
      before.concurrency,
      "Incompatible load concurrency",
    );
    for (const values of [before.retainedMemoryBytes, after.retainedMemoryBytes]) {
      assert.equal(values?.length, baseline.samples, "Missing memory samples");
      assert(values.every((value) => Number.isFinite(value) && value > 0));
    }
    const baselineBytes = Math.max(...before.retainedMemoryBytes);
    const currentBytes = Math.max(...after.retainedMemoryBytes);
    const limitBytes = baselineBytes * 1.25 + 64 * 1024 * 1024;
    resources.push({
      workload: before.name,
      baselineBytes,
      currentBytes,
      limitBytes,
      passed: currentBytes <= limitBytes,
    });
  }
  const resourceBudgets = {
    memoryBytes: [1.25, 64 * 1024 * 1024],
    heldMemoryBytes: [1.25, 64 * 1024 * 1024],
    retainedMemoryGrowthBytes: [1.25, 64 * 1024 * 1024],
    cpuMicros: [1.25, 50000],
    runtimeFdCount: [1, 4],
    processCount: [1, 2],
    diskBytes: [1.25, 64 * 1024],
    viewerBytes: [1.25, 64 * 1024],
  };
  for (const before of baseline.cases) {
    const after = current.cases.find((entry) => entry.name === before.name);
    assert.deepEqual(
      Object.keys(after.resources ?? {}).sort(),
      Object.keys(before.resources ?? {}).sort(),
      `Incompatible resources for ${before.name}`,
    );
    for (const [key, oldValues] of Object.entries(before.resources ?? {})) {
      assert(resourceBudgets[key], `Missing budget for ${key}`);
      const newValues = after.resources[key];
      for (const values of [oldValues, newValues]) {
        assert.equal(values.length, baseline.samples, "Missing resource samples");
        assert(
          values.every((value) => Number.isFinite(value) && value >= 0),
          "Invalid resource measurement",
        );
      }
      const baselineValue = Math.max(...oldValues),
        currentValue = Math.max(...newValues);
      const [ratio, allowance] = resourceBudgets[key];
      const limit = baselineValue * ratio + allowance;
      resources.push({
        workload: before.name,
        resource: key,
        baselineValue,
        currentValue,
        limit,
        passed: currentValue <= limit,
      });
    }
  }
  return {
    resources,
    budget: "fail when both median and P95 exceed baseline * 1.25 + 25 ms",
    passed: [...comparisons, ...resources].every((entry) => entry.passed),
    comparisons,
  };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const files = process.argv.slice(2);
  const split = files.indexOf("--");
  const sides =
    split === -1
      ? files.map((file) => [file])
      : [files.slice(0, split), files.slice(split + 1)];
  assert(
    sides.length === 2 && sides.every((side) => side.length > 0),
    "Usage: compare-performance.mjs baseline.json... -- current.json...",
  );
  const [baseline, current] = await Promise.all(
    sides.map(async (side) =>
      mergeReports(
        await Promise.all(
          side.map(async (file) => JSON.parse(await readFile(file, "utf8"))),
        ),
      ),
    ),
  );
  const result = comparePerformance(baseline, current);
  console.log(JSON.stringify(result, null, 2));
  console.error(
    `Performance comparison: ${result.comparisons.length} duration and ${result.resources.length} resource checks; passed=${result.passed}`,
  );
  if (!result.passed) {
    console.error(
      JSON.stringify(
        {
          failedDurations: result.comparisons.filter((entry) => !entry.passed),
          failedResources: result.resources.filter((entry) => !entry.passed),
        },
        null,
        2,
      ),
    );
    process.exitCode = 1;
  }
}
