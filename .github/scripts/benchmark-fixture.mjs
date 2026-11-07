import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { lstat, readdir, readFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { connectCdp } from "../../dist/cdp.js";
import { hostRequest } from "../../dist/host-client.js";

export function summarize(values) {
  assert(
    values.length && values.every((value) => Number.isFinite(value) && value >= 0),
  );
  const sorted = [...values].sort((a, b) => a - b);
  return {
    samples: values.length,
    medianMs: sorted[Math.floor(sorted.length / 2)],
    p95Ms: sorted[Math.ceil(sorted.length * 0.95) - 1],
  };
}

export function workload(name, measurements, resources = {}, extra = {}) {
  console.error(`Benchmark completed: ${name}`);
  return {
    name,
    measurements,
    phases: Object.fromEntries(
      Object.entries(measurements).map(([key, values]) => [key, summarize(values)]),
    ),
    resources,
    ...extra,
  };
}

export async function until(check, message, timeoutMs = 10000) {
  const deadline = performance.now() + timeoutMs;
  while (performance.now() < deadline) {
    const result = await check();
    if (result) return result;
    await delay(10);
  }
  throw Error(message);
}

export function barrier() {
  let entered, release;
  const waiting = new Promise((resolve) => {
    entered = resolve;
  });
  const released = new Promise((resolve) => {
    release = resolve;
  });
  return {
    entered: waiting,
    release,
    async hold(signal) {
      entered();
      signal.throwIfAborted();
      let cancelled;
      try {
        await Promise.race([
          released,
          new Promise((_, reject) => {
            cancelled = () => reject(signal.reason);
            signal.addEventListener("abort", cancelled, { once: true });
            if (signal.aborted) cancelled();
          }),
        ]);
      } finally {
        signal.removeEventListener("abort", cancelled);
      }
    },
  };
}

// These clients use host-issued access grants, not Browserless's private port/token.
export async function execution(socket, registration = {}) {
  const executionId = randomUUID();
  const request = { executionId, executorUid: 100096, ...registration };
  const connections = new Set();
  let finished = false;
  const closeConnections = async () => {
    for (const cdp of connections) cdp.close();
    await Promise.all([...connections].map((cdp) => cdp.closed));
    connections.clear();
  };
  const finish = async () => {
    await closeConnections();
    if (finished) return;
    await hostRequest(socket, { op: "finalize", executionId });
    finished = true;
    await rm(`/run/browser-sandbox/benchmark/${executionId}.json`, { force: true });
  };
  try {
    await hostRequest(socket, { op: "register", ...request });
    const access = await hostRequest(socket, {
      op: "access_create",
      executionId,
      durationMs: 1800000,
    });
    return {
      executionId,
      request,
      access,
      closeConnections,
      finish,
      async connect(signal) {
        const life = new AbortController();
        const timeout = setTimeout(() => life.abort(), 60000);
        try {
          const cdp = await connectCdp(
            access.cdpUrl,
            signal ? AbortSignal.any([signal, life.signal]) : life.signal,
          );
          connections.add(cdp);
          await cdp.call("Browser.getVersion");
          return cdp;
        } finally {
          clearTimeout(timeout);
        }
      },
      async ready(url) {
        const cdp = await this.connect();
        const { targetId } = await cdp.call("Target.createTarget", { url });
        const { sessionId } = await cdp.call("Target.attachToTarget", {
          targetId,
          flatten: true,
        });
        await until(async () => {
          const { result, exceptionDetails } = await cdp.call(
            "Runtime.evaluate",
            {
              expression: `location.href === ${JSON.stringify(url)} && document.readyState === 'complete'`,
              returnByValue: true,
            },
            sessionId,
          );
          return !exceptionDetails && result.value === true;
        }, "Benchmark navigation did not complete");
        const frame = await cdp.call("Page.captureScreenshot", {}, sessionId);
        assert(frame.data.length > 100);
        return { cdp, targetId, sessionId };
      },
    };
  } catch (error) {
    await finish();
    throw error;
  }
}

export async function resources() {
  const cpu = await readFile("/sys/fs/cgroup/cpu.stat", "utf8");
  const memoryBytes = Number(await readFile("/sys/fs/cgroup/memory.current", "utf8"));
  const pids = (await readdir("/proc")).filter((name) => /^\d+$/.test(name));
  let runtimeFdCount;
  for (const pid of pids) {
    try {
      const args = (await readFile(`/proc/${pid}/cmdline`, "utf8")).split("\0");
      if (args.includes("/opt/browser-sandbox/dist/main.js")) {
        runtimeFdCount = (await readdir(`/proc/${pid}/fd`)).length;
        break;
      }
    } catch (error) {
      if (error.code !== "ENOENT" && error.code !== "ESRCH") throw error;
    }
  }
  const cpuMicros = Number(/^usage_usec (\d+)$/m.exec(cpu)?.[1]);
  assert(
    Number.isFinite(memoryBytes) &&
      Number.isFinite(runtimeFdCount) &&
      Number.isFinite(cpuMicros),
    "Missing runtime resource accounting",
  );
  return {
    memoryBytes,
    cpuMicros,
    processCount: pids.length,
    runtimeFdCount,
  };
}

export async function diskUsage(directory) {
  let bytes = 0;
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const name = join(directory, entry.name);
    if (entry.isDirectory()) bytes += await diskUsage(name);
    else {
      const stat = await lstat(name);
      bytes += stat.size;
    }
  }
  return bytes;
}
