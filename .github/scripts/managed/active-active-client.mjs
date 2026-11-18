import assert from "node:assert/strict";
import { randomBytes, randomUUID } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { setTimeout as delay } from "node:timers/promises";

const root = process.env.BROWSER_PACKAGE_ROOT ?? "/opt/browser-sandbox";
const { createControllerRuntime, sessionRequest } = await import(
  `${root}/dist/index.js`
);
const { connectCdp } = await import(`${root}/dist/cdp.js`);
const { observeCdpHandshake, readVerifiedStatus } = await import(
  `${root}/.github/scripts/managed/active-active-checks.mjs`
);
const require = createRequire(`${root}/package.json`);
const WebSocket = require("ws");
const action = process.env.TEST_ACTION;
const executionId = process.env.TEST_EXECUTION_ID ?? randomUUID();
const stateFile = `/tmp/ha-${executionId}.json`;
const runtime = createControllerRuntime(process.env.TEST_CONTROL_ORIGIN);
const profileId = process.env.TEST_PROFILE;
const profileFile = profileId ? `/tmp/ha-profile-${profileId}.json` : undefined;
const adapter = {
  async load() {
    if (!profileFile) return { seed: null };
    let latest;
    try {
      latest = JSON.parse(await readFile(profileFile, "utf8"));
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
    return {
      seed: latest
        ? {
            ...latest.archive,
            key: latest.key,
            url: process.env.BROWSER_HOST_ARCHIVE_URL,
          }
        : null,
    };
  },
  save: async () => {
    const key = randomBytes(32).toString("hex");
    return {
      target: {
        key,
        keyMaterial: { key },
        objectKey: "archive",
        url: process.env.BROWSER_HOST_ARCHIVE_URL,
      },
    };
  },
  async commit(result) {
    if (profileFile)
      await writeFile(
        profileFile,
        JSON.stringify({
          archive: result.archive,
          key: result.archive.keyMaterial.key,
        }),
        { mode: 0o600 },
      );
    return { confirmedSha: result.archive.sha256 };
  },
};
let cdp;
let storage;
let handshake;
class DiagnosticWebSocket extends WebSocket {
  constructor(...args) {
    super(...args);
    handshake = observeCdpHandshake(this);
  }
}
try {
  if (action === "register" || action === "profile-contend") {
    const request = {
      op: "register",
      executionId,
      ...(action === "profile-contend"
        ? { profileId: "ha-contended", storage: true }
        : {}),
    };
    if (action === "profile-contend")
      storage = await runtime.attachStorage(request, adapter);
    const endpoint = await runtime.request(request);
    console.log(
      JSON.stringify({
        registered: true,
        executionId,
        path: new URL(endpoint.wsEndpoint).pathname,
      }),
    );
  } else if (action === "status" || action === "finish") {
    if (process.env.TEST_PROFILE)
      storage = await runtime.attachStorage(
        { executionId, profileId: process.env.TEST_PROFILE },
        adapter,
      );
    console.log(
      JSON.stringify(
        await runtime.request({
          op: action === "finish" ? "finalize" : "controller_status",
          executionId,
        }),
      ),
    );
  } else {
    let state;
    if (action === "prepare" || action === "restore") {
      if (profileId)
        storage = await runtime.attachStorage({ executionId, profileId }, adapter);
      const { wsEndpoint } = await runtime.request({
        op: "register",
        executionId,
        profileId,
        storage: profileId ? true : undefined,
      });
      state = { executionId, wsEndpoint, profileId };
    } else state = JSON.parse(await readFile(stateFile, "utf8"));
    const url = new URL(state.wsEndpoint);
    const destination = new URL(process.env.TEST_AGENT_ORIGIN);
    url.host = destination.host;
    url.protocol = destination.protocol === "https:" ? "wss:" : "ws:";
    cdp = await connectCdp(url.href, undefined, DiagnosticWebSocket);
    await cdp.call("Browser.getVersion");
    if (action === "prepare" || action === "restore") {
      if (action === "restore") {
        const cookies = await cdp.call("Storage.getCookies");
        assert(
          cookies.cookies.some(
            (cookie) => cookie.name === "ha-profile" && cookie.value === "preserved",
          ),
          "Cold Profile restore lost the saved Cookie",
        );
      }
      const { targetId } = await cdp.call("Target.createTarget", {
        url: "about:blank",
      });
      const { sessionId } = await cdp.call("Target.attachToTarget", {
        targetId,
        flatten: true,
      });
      await cdp.call(
        "Runtime.evaluate",
        {
          expression:
            "globalThis.haMarker='original-page';document.title='HA original'",
        },
        sessionId,
      );
      await cdp.call("Storage.setCookies", {
        cookies: [
          {
            name: "ha-profile",
            value: "preserved",
            domain: "ha.example",
            path: "/",
            expires: Math.floor(Date.now() / 1000) + 86400,
          },
        ],
      });
      const status = await sessionRequest(url.href, "retain", 600000);
      Object.assign(state, { targetId, generation: status.generation });
      await writeFile(stateFile, JSON.stringify(state), { mode: 0o600 });
    } else {
      const targets = await cdp.call("Target.getTargets");
      assert(targets.targetInfos.some((target) => target.targetId === state.targetId));
      const { sessionId } = await cdp.call("Target.attachToTarget", {
        targetId: state.targetId,
        flatten: true,
      });
      const value = await cdp.call(
        "Runtime.evaluate",
        { expression: "globalThis.haMarker" },
        sessionId,
      );
      assert.equal(value.result.value, "original-page");
      const cookies = await cdp.call("Storage.getCookies");
      assert(
        cookies.cookies.some(
          (cookie) => cookie.name === "ha-profile" && cookie.value === "preserved",
        ),
      );
      const status = await readVerifiedStatus(
        action,
        (signal) => sessionRequest(url.href, "status", undefined, signal),
        AbortSignal.timeout(30000),
      );
      assert.equal(status.generation, state.generation);
      if (action === "live") {
        await writeFile(`/tmp/ha-live-${executionId}.ready`, "ready");
        let closed = false;
        void cdp.closed.then(() => {
          closed = true;
        });
        while (!closed) {
          try {
            await cdp.call("Browser.getVersion");
          } catch (error) {
            if (!closed) throw error;
          }
          if (!closed) await delay(100);
        }
      } else if (action === "functional") {
        const base = url.href.replace(/^ws/, "http");
        const filename = `ha-${randomUUID()}.txt`;
        const upload = await fetch(`${base}/files/${filename}`, {
          method: "PUT",
          body: "forwarded file",
        });
        assert.equal(upload.status, 201);
        assert.equal(
          await (await fetch(`${base}/files/${filename}`)).text(),
          "forwarded file",
        );
        const access = await runtime.request({
          op: "access_create",
          executionId,
          durationMs: 60000,
        });
        const [id, token] = new URL(access.viewerUrl).hash.slice(1).split(".");
        assert.equal(id, executionId);
        const viewer = new WebSocket(
          `${destination.origin.replace(/^http/, "ws")}/execution/${id}/view/socket`,
        );
        try {
          await new Promise((resolve, reject) => {
            const timer = setTimeout(() => reject(Error("viewer_timeout")), 20000);
            viewer.once("open", () =>
              viewer.send(JSON.stringify({ type: "authenticate", token })),
            );
            viewer.on("message", (raw) => {
              const event = JSON.parse(String(raw));
              if (event.type === "frame") {
                clearTimeout(timer);
                resolve();
              }
            });
            viewer.once("error", reject);
            viewer.once("close", () => {
              clearTimeout(timer);
              reject(Error("viewer_closed"));
            });
          });
        } finally {
          viewer.terminate();
        }
      } else if (action === "benchmark") {
        const samples = [];
        const started = performance.now();
        for (let i = 0; i < 50; i++) {
          const before = performance.now();
          const response = await cdp.call(
            "Runtime.evaluate",
            { expression: "'x'.repeat(65536)", returnByValue: true },
            sessionId,
          );
          assert.equal(response.result.value.length, 65536);
          samples.push(performance.now() - before);
        }
        samples.sort((a, b) => a - b);
        console.log(
          JSON.stringify({
            medianMs: (samples[24] + samples[25]) / 2,
            p95Ms: samples[47],
            samples: 50,
            payloadBytes: 65536,
            elapsedMs: performance.now() - started,
          }),
        );
      }
    }
    if (action !== "benchmark")
      console.log(
        JSON.stringify({
          executionId,
          path: new URL(state.wsEndpoint).pathname,
          verified: true,
          architecture: process.arch,
        }),
      );
  }
} catch (error) {
  // Keep capabilities and storage grants out of Actions logs.
  console.error(
    JSON.stringify({
      action,
      handshake,
      codes: String(error).match(/browser_[a-z_]+/g) ?? [],
      message: String(error.message)
        .replace(/(?:https?|wss?):\/\/[^\s]+/g, "<url>")
        .replace(/[a-f0-9]{64}/g, "<capability>")
        .slice(0, 300),
    }),
  );
  process.exitCode = 1;
} finally {
  cdp?.close();
  await cdp?.closed;
  storage?.close();
  runtime.close();
}
