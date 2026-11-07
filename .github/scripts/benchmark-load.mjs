import assert from "node:assert/strict";
import { fork } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { hostRequest } from "../../dist/host-client.js";
import { trackBenchmark } from "./benchmark-report.mjs";

// Settle every client before cleanup, including when one concurrent task fails.
export async function runBatch(work, concurrency) {
  const results = await Promise.allSettled(
    Array.from({ length: concurrency }, (_, index) => work(index)),
  );
  const failure = results.find((result) => result.status === "rejected");
  if (failure) throw failure.reason;
  return results.map((result) => result.value);
}

export async function benchmarkLoad({ socket, url, samples, report }) {
  const concurrency = 4;
  const measurements = { firstBrowserReady: [], finalize: [], batch: [] };
  const memoryBytes = [];
  trackBenchmark(report, "concurrent-temporary", measurements, { memoryBytes });
  const worker = fileURLToPath(new URL("benchmark-image.mjs", import.meta.url));
  const execute = async (index) => {
    const executionId = randomUUID();
    let child;
    let finalized = false;
    try {
      const { wsEndpoint } = await hostRequest(socket, {
        op: "register",
        executionId,
        executorUid: 100092 + index,
      });
      const start = performance.now();
      child = fork(worker, ["worker"], {
        uid: 100092 + index,
        gid: 100092 + index,
        env: { PATH: process.env.PATH, BROWSER_WS_ENDPOINT: wsEndpoint },
        stdio: ["ignore", "ignore", "ignore", "ipc"],
      });
      const response = once(child, "message", { signal: AbortSignal.timeout(60000) });
      child.send({ action: "ready", url });
      const [message] = await response;
      assert(!message.error, "Concurrent renderer failed");
      const ready = performance.now() - start;
      const exited = once(child, "exit");
      child.disconnect();
      await exited;
      const finish = performance.now();
      await hostRequest(socket, { op: "finalize", executionId });
      finalized = true;
      return { firstBrowserReady: ready, finalize: performance.now() - finish };
    } finally {
      if (child && child.exitCode === null && child.signalCode === null) {
        const exited = once(child, "exit");
        child.kill("SIGKILL");
        await exited;
      }
      if (!finalized) await hostRequest(socket, { op: "finalize", executionId });
    }
  };
  for (let sample = -3; sample < samples; sample++) {
    if (report) report.sample = sample;
    const start = performance.now();
    const results = await runBatch(execute, concurrency);
    const duration = performance.now() - start;
    // cgroup v2 includes Chrome, Browserless, clients and page cache in the 2 GiB image.
    const memory = Number(await readFile("/sys/fs/cgroup/memory.current", "utf8"));
    assert(Number.isFinite(memory) && memory > 0, "Missing cgroup memory accounting");
    if (sample < 0) continue;
    measurements.batch.push(duration);
    for (const phase of ["firstBrowserReady", "finalize"])
      measurements[phase].push(Math.max(...results.map((result) => result[phase])));
    memoryBytes.push(memory);
  }
  const summary = (values) => {
    const sorted = [...values].sort((a, b) => a - b);
    return {
      samples: sorted.length,
      medianMs: sorted[Math.floor(sorted.length / 2)],
      p95Ms: sorted[Math.ceil(sorted.length * 0.95) - 1],
    };
  };
  return {
    name: "concurrent-temporary",
    concurrency,
    measurements,
    phases: Object.fromEntries(
      Object.entries(measurements).map(([phase, values]) => [phase, summary(values)]),
    ),
    retainedMemoryBytes: memoryBytes,
    tasksPerSecond:
      (samples * concurrency * 1000) / measurements.batch.reduce((a, b) => a + b, 0),
    note: "Four simultaneous temporary executions against two browser slots; each client navigates and screenshots through CDP. Phase samples use the slowest client in each batch. Memory is sampled after each finalized batch, not peak RSS or proof of no leaks.",
  };
}
