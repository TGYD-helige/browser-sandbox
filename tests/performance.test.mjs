import assert from "node:assert/strict";
import test from "node:test";
import { comparePerformance } from "../.github/scripts/compare-performance.mjs";

test("performance reports enforce the baseline budget and reject incompatible or missing measurements", () => {
  const report = (p95Ms) => ({
    architecture: "arm64",
    browserVersion: "Chrome/146",
    environment: "same runner",
    samples: 20,
    cases: [
      {
        name: "temporary",
        phases: { finalize: { medianMs: p95Ms, p95Ms } },
        measurements: { finalize: Array(20).fill(p95Ms) },
      },
    ],
  });
  const baseline = report(100);
  assert.equal(comparePerformance(baseline, report(150)).passed, true);
  assert.equal(comparePerformance(baseline, report(151)).passed, false);
  assert.equal(
    comparePerformance(baseline, report(120)).comparisons[0].pairedMedianDeltaMs,
    20,
  );
  assert.throws(
    () => comparePerformance(baseline, { ...report(100), architecture: "x64" }),
    /architecture/,
  );
  assert.throws(
    () => comparePerformance(baseline, { ...report(100), cases: [] }),
    /Missing workload/,
  );
  assert.throws(
    () => comparePerformance(baseline, { ...report(100), samples: 10 }),
    /matching runs/,
  );
});
