import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { request } from "node:http";
import { arch, cpus, release } from "node:os";
import { setTimeout as delay } from "node:timers/promises";
import WebSocket from "ws";
import { connectCdp } from "./dist/cdp.js";
import { createDockerClient } from "./dist/docker-backend.js";
import { createControllerRuntime, sessionRequest } from "./dist/index.js";

const api = createDockerClient();
const runtime = createControllerRuntime("http://controller:8080");
const stateFile = "/tmp/managed-state.json";
const installation = process.env.BROWSER_INSTALLATION;
const ids = [];
const summary = (values) => {
  const sorted = [...values].sort((a, b) => a - b);
  return {
    median: sorted[Math.floor(sorted.length / 2)],
    p95: sorted[Math.ceil(sorted.length * 0.95) - 1],
    samples: sorted.length,
  };
};
const workers = () =>
  api(
    `/containers/json?filters=${encodeURIComponent(JSON.stringify({ label: [`browser-sandbox.installation=${installation}`] }))}`,
  );
async function workerFor(executionId) {
  return (await workers()).find(
    (worker) => worker.Labels["browser-sandbox.execution"] === executionId,
  );
}
async function execWorker(id, script) {
  const execution = await api(`/containers/${id}/exec`, "POST", {
    Cmd: ["node", "--input-type=module", "-e", script],
    AttachStdout: true,
    AttachStderr: true,
  });
  const bytes = await new Promise((resolve, reject) => {
    const req = request(
      {
        socketPath: "/var/run/docker.sock",
        path: `/v1.47/exec/${execution.Id}/start`,
        method: "POST",
        headers: { "content-type": "application/json" },
        signal: AbortSignal.timeout(30000),
      },
      (res) => {
        const chunks = [];
        let size = 0;
        res.on("data", (chunk) => {
          size += chunk.length;
          if (size > 1048576) req.destroy();
          else chunks.push(chunk);
        });
        res.on("end", () => resolve(Buffer.concat(chunks)));
        res.on("error", reject);
      },
    );
    req.on("error", reject);
    req.end(JSON.stringify({ Detach: false, Tty: false }));
  });
  let output = "";
  for (let offset = 0; offset < bytes.length; ) {
    const size = bytes.readUInt32BE(offset + 4);
    output += bytes.subarray(offset + 8, offset + 8 + size).toString();
    offset += 8 + size;
  }
  const status = await api(`/exec/${execution.Id}/json`);
  assert.equal(status.ExitCode, 0, output);
  return output;
}
async function page(cdp) {
  const { targetId } = await cdp.call("Target.createTarget", {
    url: "data:text/html,<h1>managed retained page</h1>",
  });
  const { sessionId } = await cdp.call("Target.attachToTarget", {
    targetId,
    flatten: true,
  });
  return { targetId, sessionId };
}
try {
  if (process.argv[2] === "cleanup") {
    const state = JSON.parse(await readFile(stateFile, "utf8"));
    for (const executionId of state.ids)
      await runtime.request({ op: "finalize", executionId });
    assert.equal((await workers()).length, 0);
    console.log(JSON.stringify({ finalized: true }));
  } else {
    const readiness = [];
    let retained, endpoint, primaryWorker, pageId;
    for (let sample = 0; sample < 3; sample++) {
      const executionId = randomUUID();
      ids.push(executionId);
      const { wsEndpoint } = await runtime.request({ op: "register", executionId });
      assert.equal((await sessionRequest(wsEndpoint)).state, "unused");
      assert.equal(
        await workerFor(executionId),
        undefined,
        "Unused registration allocated a worker",
      );
      const started = performance.now();
      const cdp = await connectCdp(wsEndpoint);
      try {
        await cdp.call("Browser.getVersion");
        readiness.push(performance.now() - started);
        const current = await page(cdp);
        const screenshot = await cdp.call(
          "Page.captureScreenshot",
          {},
          current.sessionId,
        );
        assert.equal(
          Buffer.from(screenshot.data, "base64").subarray(0, 8).toString("hex"),
          "89504e470d0a1a0a",
        );
        if (sample === 0) {
          endpoint = wsEndpoint;
          pageId = current.targetId;
          const status = await sessionRequest(endpoint, "retain", 30000);
          retained = status;
          const renewed = await sessionRequest(endpoint, "renew", 120000);
          assert(renewed.expiresAt > status.expiresAt);
          assert(renewed.expiresAt <= renewed.hardExpiresAt);
          primaryWorker = await workerFor(executionId);
        }
      } finally {
        cdp.close();
        await cdp.closed;
      }
      if (sample > 0) await runtime.request({ op: "finalize", executionId });
    }
    const executionId = ids[0];
    const cdp = await connectCdp(endpoint);
    try {
      assert(
        (await cdp.call("Target.getTargets")).targetInfos.some(
          (target) => target.targetId === pageId,
        ),
      );
      assert.equal((await sessionRequest(endpoint)).generation, retained.generation);
      const files = endpoint.replace("ws:", "http:") + "/files/check.txt";
      assert.equal(
        (await fetch(files, { method: "PUT", body: "managed transfer" })).status,
        201,
      );
      assert.equal(await (await fetch(files)).text(), "managed transfer");
      // Chrome's own sandbox diagnostics exercise a real renderer in the managed worker.
      const sandbox = await cdp.call("Target.createTarget", {
        url: "chrome://sandbox",
      });
      const attached = await cdp.call("Target.attachToTarget", {
        targetId: sandbox.targetId,
        flatten: true,
      });
      let sandboxText = "";
      const deadline = Date.now() + 10000;
      do {
        sandboxText = (
          await cdp.call(
            "Runtime.evaluate",
            { expression: "document.body?.innerText ?? ''", returnByValue: true },
            attached.sessionId,
          )
        ).result.value;
        if (sandboxText.includes("Seccomp")) break;
        await delay(25);
      } while (Date.now() < deadline);
      assert.match(sandboxText, /Layer 1 Sandbox\s+Namespace/i);
      assert.match(sandboxText, /PID namespaces\s+Yes/i);
      assert.match(sandboxText, /Network namespaces\s+Yes/i);
      assert.match(sandboxText, /Seccomp-BPF sandbox\s+Yes/i);
      await cdp.call("Target.closeTarget", { targetId: sandbox.targetId });
      const access = await runtime.request({
        op: "access_create",
        executionId,
        durationMs: 120000,
      });
      const fresh = await connectCdp(access.cdpUrl);
      assert(
        (await fresh.call("Target.getTargets")).targetInfos.some(
          (target) => target.targetId === pageId,
        ),
      );
      fresh.close();
      await fresh.closed;
      const viewer = new WebSocket("ws://controller:8080/view/socket", {
        origin: "http://controller:8080",
      });
      viewer.on("error", () => {});
      await new Promise((resolve, reject) => {
        viewer.once("open", resolve);
        viewer.once("error", reject);
      });
      const state = new Promise((resolve, reject) => {
        viewer.once("message", (raw, binary) => {
          try {
            assert.equal(binary, false);
            assert.equal(JSON.parse(String(raw)).type, "state");
            resolve();
          } catch (e) {
            reject(e);
          }
        });
        viewer.once("close", () => reject(Error("viewer_closed")));
      });
      viewer.send(
        JSON.stringify({
          type: "authenticate",
          token: new URL(access.viewerUrl).hash.slice(1),
        }),
      );
      await state;
      viewer.terminate();
      await runtime.request({ op: "access_revoke", executionId });
      await assert.rejects(connectCdp(access.cdpUrl));
      assert.equal(
        (await fetch(access.cdpUrl.replace("ws:", "http:") + "/session")).status,
        403,
      );
      endpoint = (await runtime.request({ op: "endpoint", executionId })).wsEndpoint;
      const expired = await runtime.request({
        op: "access_create",
        executionId,
        durationMs: 1,
      });
      await delay(Math.max(1, expired.expiresAt - Date.now() + 1));
      await assert.rejects(connectCdp(expired.cdpUrl));
      // Measure the same CDP payload on the direct private worker and controller routes.
      const details = await api(`/containers/${primaryWorker.Id}/json`);
      const workerOrigin = details.Config.Env.find((value) =>
        value.startsWith("BROWSER_WORKER_ORIGIN="),
      ).slice("BROWSER_WORKER_ORIGIN=".length);
      const workerRuntime = createControllerRuntime(workerOrigin);
      let direct;
      try {
        const workerEndpoint = await workerRuntime.request({
          op: "register",
          executionId,
        });
        direct = await connectCdp(workerEndpoint.cdpUrl);
        const results = {};
        for (const [label, connection] of [
          ["direct", direct],
          ["proxy", await connectCdp(endpoint)],
        ]) {
          try {
            const batches = [];
            for (let sample = 0; sample < 3; sample++) {
              const statsBefore = await api(
                `/containers/${process.env.HOSTNAME}/stats?stream=false&one-shot=true`,
              );
              const times = [],
                cpu = process.cpuUsage(),
                start = performance.now();
              let bytes = 0;
              for (let i = 0; i < 30; i++) {
                const begin = performance.now();
                const result = await connection.call("Browser.getVersion");
                times.push(performance.now() - begin);
                bytes += Buffer.byteLength(JSON.stringify(result));
              }
              const screenshots = [];
              let payloadBytes = 0;
              const target = await connection.call("Target.attachToTarget", {
                targetId: pageId,
                flatten: true,
              });
              const transferStart = performance.now();
              for (let i = 0; i < 5; i++) {
                const begin = performance.now();
                const result = await connection.call(
                  "Page.captureScreenshot",
                  {},
                  target.sessionId,
                );
                screenshots.push(performance.now() - begin);
                payloadBytes += Buffer.byteLength(result.data);
              }
              const statsAfter = await api(
                `/containers/${process.env.HOSTNAME}/stats?stream=false&one-shot=true`,
              );
              const networkBytes = (stats) =>
                Object.values(stats.networks ?? {}).reduce(
                  (total, network) => total + network.rx_bytes + network.tx_bytes,
                  0,
                );
              batches.push({
                roundTripSamples: times,
                screenshotSamples: screenshots,
                controllerCpuNanos:
                  statsAfter.cpu_stats.cpu_usage.total_usage -
                  statsBefore.cpu_stats.cpu_usage.total_usage,
                controllerNetworkBytes:
                  networkBytes(statsAfter) - networkBytes(statsBefore),
                roundTripMs: summary(times),
                screenshotMs: summary(screenshots),
                payloadBytes,
                bytesPerSecond:
                  payloadBytes / ((performance.now() - transferStart) / 1000),
                clientCpuMicros: process.cpuUsage(cpu),
                elapsedMs: performance.now() - start,
                replyBytes: bytes,
              });
            }
            results[label] = {
              roundTripMs: summary(batches.flatMap((batch) => batch.roundTripSamples)),
              screenshotMs: summary(
                batches.flatMap((batch) => batch.screenshotSamples),
              ),
              controllerCpuNanos: summary(
                batches.map((batch) => batch.controllerCpuNanos),
              ),
              controllerNetworkBytes: summary(
                batches.map((batch) => batch.controllerNetworkBytes),
              ),
              bytesPerSecond: summary(batches.map((batch) => batch.bytesPerSecond)),
              batches,
            };
          } finally {
            connection.close();
            await connection.closed;
          }
        }
        const controllerDetails = await api(`/containers/${process.env.HOSTNAME}/json`);
        const controllerIp = Object.entries(details.NetworkSettings.Networks)
          .map(([name]) => controllerDetails.NetworkSettings.Networks[name]?.IPAddress)
          .find(Boolean);
        const secondId = randomUUID();
        ids.push(secondId);
        const secondEndpoint = (
          await runtime.request({ op: "register", executionId: secondId })
        ).wsEndpoint;
        const second = await connectCdp(secondEndpoint);
        try {
          await second.call("Browser.getVersion");
          await sessionRequest(secondEndpoint, "retain", 100);
        } finally {
          second.close();
          await second.closed;
        }
        const expiryDeadline = Date.now() + 15000;
        let expiredSession;
        do {
          expiredSession = await sessionRequest(secondEndpoint);
          if (expiredSession.state === "expired") break;
          await delay(25);
        } while (Date.now() < expiryDeadline);
        assert.equal(expiredSession.state, "expired");
        await assert.rejects(
          sessionRequest(secondEndpoint, "renew", 10000),
          /browser_session_not_live/,
        );
        const secondWorker = await workerFor(secondId);
        const secondDetails = await api(`/containers/${secondWorker.Id}/json`);
        const secondIp = Object.values(secondDetails.NetworkSettings.Networks)[0]
          .IPAddress;
        assert((await fetch(`${workerOrigin}/health`)).ok);
        const checks = await execWorker(
          primaryWorker.Id,
          String.raw`
          import assert from 'node:assert/strict';
          import {execFileSync} from 'node:child_process';
          for(const target of ['http://169.254.169.254/','http://${controllerIp}:8080/health','http://${secondIp}:8081/health']) await assert.rejects(fetch(target,{signal:AbortSignal.timeout(2000)}));
          const rules=execFileSync('iptables',['-L','OUTPUT','-nvx']).toString();
          assert(rules.split('\n').some(line=>line.includes('REJECT')&&line.includes('169.254.0.0/16')&&Number(line.trim().split(/\s+/)[0])>0));
          execFileSync(process.execPath, ['-e', "fetch('http://127.0.0.1:8081/health',{signal:AbortSignal.timeout(2000)}).then(()=>process.exit(1),()=>process.exit(0))"], {uid:20001,gid:20001});
          console.log('metadata, other worker, controller, and browser-loopback egress denied');
        `,
        );
        assert.match(checks, /egress denied/);
        await writeFile(
          stateFile,
          JSON.stringify({
            ids,
            endpoint,
            executionId,
            workerId: primaryWorker.Id,
            workerOrigin,
            report: {
              firstBrowserMs: summary(readiness),
              traffic: results,
              environment: {
                arch: arch(),
                kernel: release(),
                cpus: cpus().length,
                sandbox: "namespace + seccomp",
                backend: "Docker",
                node: process.version,
              },
            },
          }),
          { mode: 0o600 },
        );
        console.log(
          JSON.stringify({
            prepared: true,
            firstBrowserMs: summary(readiness),
            traffic: results,
          }),
        );
      } finally {
        direct?.close();
        workerRuntime.close();
      }
    } finally {
      cdp.close();
      await cdp.closed;
    }
  }
} catch (error) {
  for (const executionId of ids)
    await runtime.request({ op: "finalize", executionId }).catch(() => {});
  throw error;
} finally {
  runtime.close();
}
