import assert from "node:assert/strict";
import { once } from "node:events";
import { setTimeout as delay } from "node:timers/promises";
import WebSocket from "ws";
import { execution, resources, until, workload } from "./benchmark-fixture.mjs";
import { trackBenchmark } from "./benchmark-report.mjs";

export async function decodeFrameRgb(cdp, sessionId, data) {
  const { result, exceptionDetails } = await cdp.call(
    "Runtime.evaluate",
    {
      expression: `(async () => { const response=await fetch(${JSON.stringify("data:image/jpeg;base64,")}+${JSON.stringify(data)});const image=await createImageBitmap(await response.blob());try{const canvas=new OffscreenCanvas(1,1);const context=canvas.getContext('2d');context.drawImage(image,16,16,1,1,0,0,1,1);return Array.from(context.getImageData(0,0,1,1).data).slice(0,3);}finally{image.close();} })()`,
      awaitPromise: true,
      returnByValue: true,
    },
    sessionId,
  );
  assert(!exceptionDetails, "Renderer could not decode its viewer JPEG");
  assert(
    Array.isArray(result.value) &&
      result.value.length === 3 &&
      result.value.every((value) => Number.isFinite(value)),
    "Frame marker pixel was not decoded",
  );
  return result.value;
}

export async function waitForMarker({ latest, decode, expected, updatedAt }) {
  let seen = 0;
  return until(async () => {
    const image = latest();
    if (!image || image.receivedAt < updatedAt || image.sequence === seen) return null;
    seen = image.sequence;
    const rgb = await decode(image.data);
    return rgb.every((value, index) => Math.abs(value - expected[index]) <= 25)
      ? { latency: image.receivedAt - updatedAt }
      : null;
  }, "Viewer delivered stale renderer frames");
}

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
  let latest;
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
    const receivedAt = performance.now();
    stats.times.push(receivedAt);
    latest = { data: message.data, receivedAt, sequence: stats.frames };
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
      latest: () => latest,
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

export async function benchmarkViewer({ socket, origin, samples, report }) {
  const client = await execution(socket);
  try {
    const { cdp, sessionId, targetId } = await client.ready(origin + "/page");
    for (const target of (await cdp.call("Target.getTargets")).targetInfos)
      if (target.type === "page" && target.targetId !== targetId)
        await cdp.call("Target.closeTarget", { targetId: target.targetId });
    await cdp.call(
      "Runtime.evaluate",
      {
        expression: `(() => { document.body.style.margin='0';const c=document.createElement('canvas');c.width=640;c.height=360;document.body.replaceChildren(c);const x=c.getContext('2d');let frame=0;window.benchmarkMarker=[230,20,30];window.benchmarkDraw=()=>{x.fillStyle='hsl('+((frame++*17)%360)+' 70% 45%)';x.fillRect(0,0,640,360);x.fillStyle='white';x.font='40px sans-serif';x.fillText(String(frame),40,100);x.fillStyle='rgb('+window.benchmarkMarker.join(',')+')';x.fillRect(0,0,48,48);};const draw=()=>{window.benchmarkDraw();requestAnimationFrame(draw);};draw();})()`,
      },
      sessionId,
    );
    const cases = report?.pendingCases ?? [];
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
        measurements.updateToFrame = [];
      }
      trackBenchmark(report, `viewer-${count}`, measurements, sampled);
      const framesPerSecond = [];
      for (let sample = -3; sample < samples; sample++) {
        if (report) report.sample = sample;
        const viewers = [];
        try {
          for (let i = 0; i < count; i++)
            viewers.push(await observe(client.access.viewerUrl));
          let start = performance.now();
          for (let i = 0; i < 50; i++)
            assert((await cdp.call("Browser.getVersion")).product);
          const cdpTime = performance.now() - start;
          const before = await resources();
          const offsets = viewers.map((viewer) => ({
            frames: viewer.stats.frames,
            bytes: viewer.stats.bytes,
            times: viewer.stats.times.length,
          }));
          start = performance.now();
          // This is a fixed observation window, not synchronization for a state transition.
          await delay(500);
          const elapsed = performance.now() - start;
          const endings = viewers.map((viewer, i) => ({
            frames: viewer.stats.frames - offsets[i].frames,
            bytes: viewer.stats.bytes - offsets[i].bytes,
            times: viewer.stats.times.slice(offsets[i].times),
          }));
          const after = await resources();
          const frameCounts = endings.map((ending) => ending.frames);
          if (count)
            assert(
              frameCounts.every((frames) => frames >= 2),
              "Animated viewers must continue delivering frames",
            );
          let updateToFrame;
          if (count) {
            const expected = [
              [230, 20, 30],
              [20, 220, 40],
              [20, 40, 230],
            ][(sample + 3) % 3];
            const updatedAt = performance.now();
            await cdp.call(
              "Runtime.evaluate",
              {
                expression: `window.benchmarkMarker=${JSON.stringify(expected)};window.benchmarkDraw()`,
              },
              sessionId,
            );
            // Decode outside the CPU/bandwidth window; latency ends at frame receipt, before decoding.
            const delivered = await Promise.all(
              viewers.map((viewer) =>
                waitForMarker({
                  latest: viewer.latest,
                  expected,
                  updatedAt,
                  decode: (data) => decodeFrameRgb(cdp, sessionId, data),
                }),
              ),
            );
            updateToFrame = Math.max(...delivered.map((frame) => frame.latency));
          }
          if (sample < 0) continue;
          measurements.cdpBatch.push(cdpTime);
          sampled.cpuMicros.push(
            ((after.cpuMicros - before.cpuMicros) * 1000) /
              (after.sampledAt - before.sampledAt),
          );
          sampled.memoryBytes.push(after.memoryBytes);
          sampled.runtimeFdCount.push(after.runtimeFdCount);
          sampled.processCount.push(after.processCount);
          sampled.viewerBytes.push(
            (endings.reduce((sum, ending) => sum + ending.bytes, 0) * 1000) / elapsed,
          );
          if (count) {
            measurements.updateToFrame.push(updateToFrame);
            measurements.firstFrame.push(
              Math.max(...viewers.map((viewer) => viewer.firstFrameMs)),
            );
            const intervals = endings.flatMap(({ times }) =>
              times.slice(1).map((time, index) => time - times[index]),
            );
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
          note: "Real authenticated viewer WebSockets with frame acknowledgements and an animated renderer. CPU and wire bytes are normalized per second. Correlated color markers measure renderer-update to matching-frame receipt; JPEG decoding is excluded from CPU/wire windows and the latency endpoint. Client UI screen paint is not measured.",
        }),
      );
    }
    return cases;
  } finally {
    await client.finish();
  }
}
