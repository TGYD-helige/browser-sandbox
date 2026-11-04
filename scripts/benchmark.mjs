import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import WebSocket, { WebSocketServer } from "ws";
import { createGateway } from "../dist/gateway.js";

const samples = Number(process.env.SAMPLES ?? 50);
if (!Number.isInteger(samples) || samples < 10 || samples > 1000)
  throw Error("SAMPLES must be between 10 and 1000");
const implementations = [["current", createGateway]];
if (process.env.BASELINE_GATEWAY)
  implementations.unshift([
    "baseline",
    (await import(pathToFileURL(process.env.BASELINE_GATEWAY))).createGateway,
  ]);
const upstream = new WebSocketServer({ host: "127.0.0.1", port: 0 });
await once(upstream, "listening");
upstream.on("connection", (socket) =>
  socket.on("message", (data) => socket.send(data)),
);
const endpoint = `ws://127.0.0.1:${upstream.address().port}`;
const summary = (values) => {
  values.sort((a, b) => a - b);
  return {
    medianMs: +values[Math.floor(values.length / 2)].toFixed(3),
    p95Ms: +values[Math.ceil(values.length * 0.95) - 1].toFixed(3),
  };
};
const results = [];
try {
  for (const [name, factory] of implementations) {
    const root = await mkdtemp(join(tmpdir(), "browser-benchmark-"));
    const runtime = await factory({
      WebSocket,
      WebSocketServer,
      port: 0,
      socketPath: join(root, "control.sock"),
      dataRoot: join(root, "data"),
      healthy: async () => true,
      launch: async () => ({ endpoint, close: async () => {} }),
    });
    const phases = {
      register: [],
      unusedFinish: [],
      firstTransport: [],
      usedFinish: [],
    };
    try {
      for (let i = 0; i < samples + 5; i++) {
        const executionId = randomUUID();
        let start = performance.now();
        const { wsEndpoint } = await runtime.command({
          op: "register",
          executionId,
        });
        const registered = performance.now() - start;
        start = performance.now();
        if (i % 2) {
          const socket = new WebSocket(wsEndpoint);
          await once(socket, "open");
          const reply = once(socket, "message");
          socket.send("probe");
          await reply;
          if (i >= 5) phases.firstTransport.push(performance.now() - start);
          start = performance.now();
          await runtime.command({ op: "finish", executionId });
          if (i >= 5) phases.usedFinish.push(performance.now() - start);
        } else {
          await runtime.command({ op: "finish", executionId });
          if (i >= 5) phases.unusedFinish.push(performance.now() - start);
        }
        if (i >= 5) phases.register.push(registered);
      }
      results.push({
        name,
        samples,
        phases: Object.fromEntries(
          Object.entries(phases).map(([phase, values]) => [phase, summary(values)]),
        ),
      });
    } finally {
      await runtime.stop();
      await rm(root, { recursive: true, force: true });
    }
  }
} finally {
  await new Promise((resolve) => upstream.close(resolve));
}
console.log(
  JSON.stringify(
    {
      node: process.version,
      platform: process.platform,
      architecture: process.arch,
      note: "Synthetic gateway and filesystem overhead only. Browser startup, Profile restore, storage transfer, and harness startup are excluded.",
      results,
    },
    null,
    2,
  ),
);
