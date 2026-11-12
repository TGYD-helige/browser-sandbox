import assert from "node:assert/strict";
import { execution, resources, summarize, workload } from "./benchmark-fixture.mjs";
import { runBatch } from "./benchmark-load.mjs";
import { trackBenchmark } from "./benchmark-report.mjs";

export async function benchmarkCdp({ socket, origin, samples, report }) {
  const client = await execution(socket);
  try {
    const { cdp, sessionId } = await client.ready(origin + "/page");
    const cases = report?.pendingCases ?? [];
    const connections = [cdp];
    for (let i = 1; i < 4; i++) connections.push(await client.connect());
    for (const [name, count, parallel] of [
      ["cdp-small", 100, false],
      ["cdp-four-connections", 100, true],
      ["cdp-large-response", 4, false],
      ["cdp-screenshot", 1, false],
    ]) {
      const measurements = { batch: [], commandP95: [] };
      const sampled = { cpuMicros: [], memoryBytes: [] };
      trackBenchmark(report, name, measurements, sampled);
      let transferredBytes = 0;
      if (name === "cdp-screenshot") {
        // Deterministic noisy pixels prevent a tiny blank PNG from hiding large-response costs.
        await cdp.call(
          "Runtime.evaluate",
          {
            expression: `(() => { const c=document.createElement('canvas');c.width=1024;c.height=768;document.body.replaceChildren(c);const x=c.getContext('2d');const p=x.createImageData(c.width,c.height);let seed=42;for(let i=0;i<p.data.length;i++){seed=(Math.imul(seed,1664525)+1013904223)>>>0;p.data[i]=i%4===3?255:seed>>>24;}x.putImageData(p,0,0);})()`,
          },
          sessionId,
        );
      }
      for (let sample = -3; sample < samples; sample++) {
        if (report) report.sample = sample;
        const before = await resources();
        const roundTrips = [];
        const start = performance.now();
        let bytes = 0;
        const run = async () => {
          for (let i = 0; i < count; i++) {
            const sent = performance.now();
            if (name === "cdp-screenshot") {
              const result = await cdp.call("Page.captureScreenshot", {}, sessionId);
              const size = Buffer.byteLength(result.data, "base64");
              assert(
                size > 1024 * 1024,
                "Screenshot fixture must exercise a large response",
              );
              bytes += size;
            } else {
              const large = name === "cdp-large-response";
              const { result } = await cdp.call(
                "Runtime.evaluate",
                {
                  expression: large ? "'x'.repeat(1024*1024)" : "1+1",
                  returnByValue: true,
                },
                sessionId,
              );
              assert.equal(
                large ? result.value.length : result.value,
                large ? 1024 * 1024 : 2,
              );
              if (large) bytes += result.value.length;
            }
            roundTrips.push(performance.now() - sent);
          }
        };
        if (parallel) {
          await runBatch(async (index) => {
            // Browser.getVersion is valid on every browser-scoped CDP connection.
            for (let i = 0; i < count; i++) {
              const sent = performance.now();
              assert((await connections[index].call("Browser.getVersion")).product);
              roundTrips.push(performance.now() - sent);
            }
          }, 4);
        } else await run();
        const duration = performance.now() - start;
        const after = await resources();
        if (sample >= 0) {
          assert.equal(roundTrips.length, count * (parallel ? 4 : 1));
          measurements.batch.push(duration);
          measurements.commandP95.push(summarize(roundTrips).p95Ms);
          sampled.cpuMicros.push(
            ((after.cpuMicros - before.cpuMicros) * 1000) /
              (after.sampledAt - before.sampledAt),
          );
          sampled.memoryBytes.push(after.memoryBytes);
          transferredBytes += bytes;
        }
      }
      const operations = count * (parallel ? 4 : 1);
      cases.push(
        workload(name, measurements, sampled, {
          operationsPerSample: operations,
          operationsPerSecond:
            (samples * operations * 1000) /
            measurements.batch.reduce((a, b) => a + b, 0),
          transferredBytes,
        }),
      );
    }
    return cases;
  } finally {
    await client.finish();
  }
}
