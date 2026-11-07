import assert from "node:assert/strict";
import { once } from "node:events";
import { setTimeout as delay } from "node:timers/promises";
import WebSocket from "ws";
import { execution, resources, workload } from "./benchmark-fixture.mjs";

async function observe(viewerUrl) {
  const url = new URL(viewerUrl);
  const ws = new WebSocket(url.origin.replace(/^http/, "ws") + "/view/socket", {
    origin: url.origin,
    maxPayload: 12 * 1024 * 1024,
    handshakeTimeout: 10000,
  });
  ws.on("error", () => {});
  const closed = new Promise((resolve) => ws.once("close", resolve));
  const stats = { bytes: 0, frames: 0, times: [] };
  let ready, failed;
  const first = new Promise((resolve, reject) => {
    ready = resolve;
    failed = reject;
  });
  first.catch(() => {});
  ws.on("message", (raw) => {
    const message = JSON.parse(String(raw));
    if (message.type === "error") {
      failed(Error(message.error));
      return;
    }
    if (message.type !== "frame") return;
    stats.bytes += raw.length;
    stats.frames++;
    stats.times.push(performance.now());
    ready();
    if (ws.readyState === WebSocket.OPEN)
      ws.send(JSON.stringify({ type: "ack", sequence: message.sequence }));
  });
  ws.once("close", () => failed(Error("Viewer closed before first frame")));
  const timeout = setTimeout(
    () => failed(Error("Viewer first frame timed out")),
    10000,
  );
  const start = performance.now();
  try {
    await once(ws, "open");
    ws.send(JSON.stringify({ type: "authenticate", token: url.hash.slice(1) }));
    await first;
    return {
      stats,
      firstFrameMs: performance.now() - start,
      async close() {
        ws.terminate();
        await closed;
      },
    };
  } catch (error) {
    ws.terminate();
    await closed;
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

export async function benchmarkViewer({ socket, origin, samples }) {
  const client = await execution(socket);
  try {
    const { cdp, sessionId, targetId } = await client.ready(origin + "/page");
    for (const target of (await cdp.call("Target.getTargets")).targetInfos)
      if (target.type === "page" && target.targetId !== targetId)
        await cdp.call("Target.closeTarget", { targetId: target.targetId });
    await cdp.call(
      "Runtime.evaluate",
      {
        expression: `(() => { const c=document.createElement('canvas');c.width=640;c.height=360;document.body.replaceChildren(c);const x=c.getContext('2d');let frame=0;const draw=()=>{x.fillStyle='hsl('+((frame++*17)%360)+' 70% 45%)';x.fillRect(0,0,640,360);x.fillStyle='white';x.font='40px sans-serif';x.fillText(String(frame),40,100);requestAnimationFrame(draw);};draw();})()`,
      },
      sessionId,
    );
    const cases = [];
    for (const count of [0, 1, 4]) {
      const measurements = { cdpBatch: [] },
        sampled = {
          cpuMicros: [],
          memoryBytes: [],
          runtimeFdCount: [],
          processCount: [],
          viewerBytes: [],
        };
      if (count) {
        measurements.firstFrame = [];
        measurements.frameInterval = [];
      }
      const framesPerSecond = [];
      for (let sample = -3; sample < samples; sample++) {
        const viewers = [];
        try {
          for (let i = 0; i < count; i++)
            viewers.push(await observe(client.access.viewerUrl));
          const before = await resources();
          let start = performance.now();
          for (let i = 0; i < 50; i++)
            assert((await cdp.call("Browser.getVersion")).product);
          const cdpTime = performance.now() - start;
          const offsets = viewers.map((viewer) => ({
            frames: viewer.stats.frames,
            bytes: viewer.stats.bytes,
            times: viewer.stats.times.length,
          }));
          start = performance.now();
          // This is a fixed observation window, not synchronization for a state transition.
          await delay(500);
          const elapsed = performance.now() - start;
          const after = await resources();
          const frameCounts = viewers.map(
            (viewer, i) => viewer.stats.frames - offsets[i].frames,
          );
          if (count)
            assert(
              frameCounts.every((frames) => frames >= 2),
              "Animated viewers must continue delivering frames",
            );
          if (sample < 0) continue;
          measurements.cdpBatch.push(cdpTime);
          sampled.cpuMicros.push(
            ((after.cpuMicros - before.cpuMicros) * 1000) / elapsed,
          );
          sampled.memoryBytes.push(after.memoryBytes);
          sampled.runtimeFdCount.push(after.runtimeFdCount);
          sampled.processCount.push(after.processCount);
          sampled.viewerBytes.push(
            (viewers.reduce(
              (sum, viewer, i) => sum + viewer.stats.bytes - offsets[i].bytes,
              0,
            ) *
              1000) /
              elapsed,
          );
          if (count) {
            measurements.firstFrame.push(
              Math.max(...viewers.map((viewer) => viewer.firstFrameMs)),
            );
            const intervals = viewers.flatMap((viewer, i) => {
              const times = viewer.stats.times.slice(offsets[i].times);
              return times.slice(1).map((time, index) => time - times[index]);
            });
            measurements.frameInterval.push(Math.max(...intervals));
            framesPerSecond.push((Math.min(...frameCounts) * 1000) / elapsed);
          }
        } finally {
          await Promise.all(viewers.map((viewer) => viewer.close()));
        }
      }
      cases.push(
        workload(`viewer-${count}`, measurements, sampled, {
          viewers: count,
          observationWindowMs: 500,
          framesPerSecond,
          note: "Real authenticated viewer WebSockets with frame acknowledgements and an animated renderer. CPU and wire bytes are normalized per second. Frame intervals/first arrival measure protocol delivery, not image decoding or screen paint in a viewer browser.",
        }),
      );
    }
    return cases;
  } finally {
    await client.finish();
  }
}
