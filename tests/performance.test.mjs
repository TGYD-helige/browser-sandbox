import assert from "node:assert/strict";
import test from "node:test";
import {
  comparePerformance,
  mergeReports,
} from "../.github/scripts/compare-performance.mjs";

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
    comparePerformance(baseline, report(120)).comparisons[0].currentMedianMs,
    120,
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

test("performance budgets ignore tail-only noise but reject a shifted distribution", () => {
  const report = (values) => ({
    architecture: "x64",
    browserVersion: "Chrome/146",
    environment: "same runner",
    samples: values.length,
    cases: [
      {
        name: "temporary",
        phases: { finalize: {} },
        measurements: { finalize: values },
      },
    ],
  });
  const baseline = report(Array(20).fill(50));
  const tail = [...Array(18).fill(50), 190, 190];
  assert.equal(comparePerformance(baseline, report(tail)).passed, true);
  assert.equal(comparePerformance(baseline, report(Array(20).fill(200))).passed, false);
  const halves = [report(Array(10).fill(50)), report(Array(10).fill(200))];
  const merged = mergeReports(halves);
  assert.equal(merged.samples, 20);
  assert.equal(merged.cases[0].measurements.finalize.length, 20);
  assert.equal(halves[0].cases[0].measurements.finalize.length, 10);
  assert.throws(
    () => mergeReports([halves[0], { ...halves[1], architecture: "arm64" }]),
    /architecture/,
  );
  assert.throws(
    () => mergeReports([halves[0], { ...halves[1], cases: [] }]),
    /Missing workload/,
  );
});

test("a bimodal phase is judged against the slower baseline half, not a pooled median", () => {
  const run = (values) => ({
    architecture: "x64",
    browserVersion: "Chrome/146",
    environment: "same runner",
    samples: values.length,
    cases: [
      {
        name: "remote-small",
        phases: { finalize: {} },
        measurements: { finalize: values },
      },
    ],
  });
  const compare = (baseline, current) =>
    comparePerformance(mergeReports(baseline.map(run)), mergeReports(current.map(run)));
  // Remote finalization samples from one amd64 runner; both modes appear in both runtimes.
  const baseline = [
    [306, 300, 297, 156, 156, 159, 165, 178, 163, 166],
    [178, 170, 286, 288, 177, 288, 311, 289, 287, 303],
  ];
  const current = [
    [321, 326, 300, 292, 439, 166, 306, 160, 436, 169],
    [171, 311, 171, 154, 167, 294, 320, 281, 281, 293],
  ];
  assert.equal(compare(baseline, current).passed, true);
  const shifted = current.map((values) => values.map((value) => value * 1.5 + 40));
  assert.equal(compare(baseline, shifted).passed, false);
  assert.throws(
    () => mergeReports([run(baseline[0]), run(baseline[1].slice(1))]),
    /Inconsistent repeated benchmark samples/,
  );
});

test("resource budgets ignore one outlier sample but reject a shifted distribution", () => {
  const report = (cpuMicros) => ({
    architecture: "x64",
    browserVersion: "Chrome/146",
    environment: "same runner",
    samples: 20,
    cases: [
      {
        name: "viewer-1",
        phases: { frame: {} },
        measurements: { frame: Array(20).fill(30) },
        resources: { cpuMicros },
      },
    ],
  });
  const baseline = report(Array(20).fill(600_000));
  const outlier = [...Array(19).fill(550_000), 1_150_000];
  assert.equal(comparePerformance(baseline, report(outlier)).passed, true);
  assert.equal(
    comparePerformance(
      baseline,
      report([...Array(18).fill(550_000), 1_150_000, 1_150_000]),
    ).passed,
    false,
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
