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

test("performance budgets use raw durations rather than a stale summary", () => {
  const report = (value) => ({
    architecture: "x64",
    browserVersion: "Chrome/146",
    environment: "same runner",
    samples: 20,
    cases: [
      {
        name: "temporary",
        phases: { finalize: { p95Ms: 100 } },
        measurements: { finalize: Array(20).fill(value) },
      },
    ],
  });
  assert.equal(comparePerformance(report(100), report(1000)).passed, false);
  const partial = report(100);
  partial.cases[0].measurements.finalize.pop();
  assert.throws(() => comparePerformance(partial, report(100)), /samples/);
});

test("benchmark archive metrics exclude favicon and unrelated requests", async () => {
  const { createBenchmarkSite } = await import(
    "../.github/scripts/benchmark-image.mjs"
  );
  const { mkdtemp, rm } = await import("node:fs/promises");
  const { tmpdir } = await import("node:os");
  const { join } = await import("node:path");
  const { once } = await import("node:events");
  const root = await mkdtemp(join(tmpdir(), "benchmark-routing-"));
  const { site, transfers } = createBenchmarkSite(root);
  site.listen(0, "127.0.0.1");
  await once(site, "listening");
  const url = `http://127.0.0.1:${site.address().port}`;
  try {
    const uploaded = await fetch(url + "/archive", {
      method: "PUT",
      body: "profile-ciphertext",
    });
    await uploaded.arrayBuffer();
    assert.equal(uploaded.status, 200);
    const favicon = await fetch(url + "/favicon.ico");
    await favicon.arrayBuffer();
    assert.equal(favicon.status, 404);
    const invalid = await fetch(url + "/archive", { method: "POST" });
    await invalid.arrayBuffer();
    assert.equal(invalid.status, 405);
    const downloaded = await fetch(url + "/archive");
    assert.equal(await downloaded.text(), "profile-ciphertext");
    const deadline = Date.now() + 2000;
    while (transfers.download.length === 0 && Date.now() < deadline)
      await new Promise((resolve) => setTimeout(resolve, 5));
    assert.equal(transfers.download.length, 1);
    assert.equal(transfers.upload.length, 1);
  } finally {
    site.closeAllConnections();
    await new Promise((resolve) => site.close(resolve));
    await rm(root, { recursive: true, force: true });
  }
});
