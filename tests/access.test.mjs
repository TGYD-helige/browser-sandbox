import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { request } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import WebSocket, { WebSocketServer } from "ws";
import { browserAuthRequest } from "../dist/browser-auth.js";
import { createGateway } from "../dist/gateway.js";
import { hostRequest } from "../dist/host-client.js";
import { sessionRequest } from "../dist/session.js";

async function fixture(t, access = {}, gatewayOptions = {}) {
  const root = await mkdtemp(join(tmpdir(), "browser-view-"));
  const upstream = new WebSocketServer({ host: "127.0.0.1", port: 0 });
  await once(upstream, "listening");
  const calls = [];
  const extraTargets = [];
  let targetsGate, notifyTargets;
  let launches = 0,
    url = "https://example.com/form",
    generation = "document-1";
  const target = () => ({
    targetId: "page",
    type: "page",
    title: "Form <untrusted>",
    url,
  });
  upstream.on("connection", (socket) =>
    socket.on("message", async (raw) => {
      const request = JSON.parse(String(raw));
      calls.push(request);
      if (request.method === "Target.getTargets" && targetsGate) {
        notifyTargets();
        await targetsGate;
      }
      let result = {};
      if (request.method === "Target.getTargets")
        result = {
          targetInfos: [
            target(),
            ...extraTargets,
            { type: "page", targetId: "internal", url: "chrome://settings" },
          ],
        };
      if (request.method === "Target.attachToTarget")
        result = { sessionId: "page-session" };
      if (request.method === "Page.getFrameTree")
        result = { frameTree: { frame: { id: "frame", loaderId: generation, url } } };
      if (request.method === "Page.createIsolatedWorld")
        result = { executionContextId: 99 };
      if (request.method === "Runtime.callFunctionOn")
        result = {
          result: {
            value: request.params.functionDeclaration.includes("const position")
              ? { x: 20, y: 20 }
              : true,
          },
        };
      if (request.method === "Page.captureScreenshot") result = { data: "jpeg" };
      if (request.method === "Page.getLayoutMetrics")
        result = { cssVisualViewport: { clientWidth: 800, clientHeight: 600 } };
      socket.send(JSON.stringify({ id: request.id, result }));
      if (request.method === "Page.startScreencast")
        setTimeout(() => {
          if (socket.readyState === WebSocket.OPEN)
            socket.send(
              JSON.stringify({
                sessionId: "page-session",
                method: "Page.screencastFrame",
                params: {
                  sessionId: 1,
                  data: "jpeg",
                  metadata: { deviceWidth: 800, deviceHeight: 600 },
                },
              }),
            );
        }, 10);
    }),
  );
  const socketPath = join(root, "control.sock");
  const gateway = await createGateway({
    WebSocket,
    WebSocketServer,
    port: 0,
    dataRoot: join(root, "data"),
    socketPath,
    healthy: async () => true,
    resolvePeerUid: async () => {
      throw Error("remote requests must use their own authorization");
    },
    access: { port: 0, ...access },
    ...gatewayOptions,
    async launch() {
      launches++;
      return {
        endpoint: `ws://127.0.0.1:${upstream.address().port}`,
        close: async () => {},
      };
    },
  });
  const origin = `http://127.0.0.1:${gateway.accessPort}`;
  const clients = [];
  t.after(async () => {
    for (const client of clients) client.terminate();
    await gateway.stop();
    for (const client of upstream.clients) client.terminate();
    await new Promise((resolve) => upstream.close(resolve));
    await rm(root, { recursive: true, force: true });
  });
  const executionId = randomUUID();
  await gateway.command({ op: "register", executionId, executorUid: 100001 });
  async function issue(options = {}) {
    return hostRequest(socketPath, { op: "access_create", executionId, ...options });
  }
  async function agent(url) {
    const client = new WebSocket(url);
    clients.push(client);
    client.on("error", () => {});
    await once(client, "open");
    const reply = once(client, "message");
    client.send(JSON.stringify({ id: 1, method: "Browser.getVersion" }));
    await reply;
    return client;
  }
  async function viewer(url) {
    const client = new WebSocket(origin.replace(/^http/, "ws") + "/view/socket", {
      origin,
    });
    clients.push(client);
    client.on("error", () => {});
    const messages = [];
    client.on("message", (raw) => messages.push(JSON.parse(String(raw))));
    await once(client, "open");
    client.send(
      JSON.stringify({ type: "authenticate", token: new URL(url).hash.slice(1) }),
    );
    let id = 0;
    async function wait(predicate, timeout = 5000) {
      const deadline = Date.now() + timeout;
      while (Date.now() < deadline) {
        const found = messages.find(predicate);
        if (found) return found;
        await delay(10);
      }
      throw Error(`Viewer event not received: ${JSON.stringify(messages)}`);
    }
    return {
      client,
      messages,
      wait,
      async command(type, fields = {}) {
        const requestId = ++id;
        client.send(JSON.stringify({ type, id: requestId, ...fields }));
        return wait((message) => message.id === requestId);
      },
    };
  }
  return {
    gateway,
    socketPath,
    origin,
    issue,
    agent,
    viewer,
    calls,
    addTarget: (target) => extraTargets.push(target),
    executionId,
    blockTargetReads() {
      let release, entered;
      targetsGate = new Promise((resolve) => {
        release = () => {
          targetsGate = undefined;
          resolve();
        };
      });
      entered = new Promise((resolve) => {
        notifyTargets = resolve;
      });
      return { entered, release };
    },
    get launches() {
      return launches;
    },
    navigate(next) {
      url = next;
      generation += "-next";
    },
  };
}

test("only the trusted host issues access; watching and HTTP assets preserve lazy startup", async (t) => {
  const f = await fixture(t);
  await assert.rejects(
    f.gateway.command({ op: "access_create", executionId: f.executionId }),
    /host_authority/,
  );
  const access = await f.issue();
  const view = await f.viewer(access.viewerUrl);
  const state = await view.wait((message) => message.type === "state");
  assert.equal(state.browser.state, "unused");
  assert.equal(state.canAcquire, false);
  assert.equal(f.launches, 0);
  const page = await fetch(f.origin + "/view");
  assert.equal(page.status, 200);
  assert.match(await page.text(), /browser-viewer/);
  assert.equal(page.headers.get("referrer-policy"), "no-referrer");
  assert.equal((await fetch(f.origin + "/viewer.js")).status, 200);
  assert.equal((await sessionRequest(access.cdpUrl)).state, "unused");
  assert.equal(
    (
      await fetch(
        f.origin + "/execution/" + new URL(access.viewerUrl).hash.slice(1) + "/session",
      )
    ).status,
    403,
  );
  assert.equal((await view.command("acquire")).error, "browser_control_denied");
});

test("remote CDP, files and observation work without bypassing the local UID check", async (t) => {
  const f = await fixture(t);
  const access = await f.issue();
  await f.agent(access.cdpUrl);
  const view = await f.viewer(access.viewerUrl);
  const frame = await view.wait((message) => message.type === "frame");
  assert.equal(frame.interactive, false);
  view.client.send(JSON.stringify({ type: "ack", sequence: frame.sequence }));
  const targets = await view.wait((message) => message.type === "targets");
  assert.deepEqual(
    targets.targets.map((item) => item.targetId),
    ["page"],
  );
  const file = access.cdpUrl.replace(/^ws/, "http") + "/files/note.txt";
  assert.equal((await fetch(file, { method: "PUT", body: "hello" })).status, 201);
  assert.equal(await (await fetch(file)).text(), "hello");
  assert.equal((await sessionRequest(access.cdpUrl)).connectedClients, 1);
  assert.equal(f.launches, 1);
});

test("view grants cannot assert automation stop and a failed coordinator grants no lease", async (t) => {
  const f = await fixture(t, {
    automation: {
      async pause() {
        throw Error("private diagnostic");
      },
      async resume() {},
    },
  });
  const access = await f.issue({ control: true });
  await f.agent(access.cdpUrl);
  const view = await f.viewer(access.viewerUrl);
  await view.wait((message) => message.type === "frame");
  const result = await view.command("acquire", {
    automationStopped: true,
    actorId: "forged",
  });
  assert.equal(result.error, "browser_view_request_failed");
  assert(!f.calls.some((call) => call.method === "Runtime.terminateExecution"));
});

test("confirmed takeover fences all CDP, consumes fresh frames and explicitly hands back a new origin", async (t) => {
  const f = await fixture(t);
  const access = await f.issue({ control: true });
  const agent = await f.agent(access.cdpUrl);
  const view = await f.viewer(access.viewerUrl);
  await view.wait((message) => message.type === "frame");
  assert.equal(
    (await view.command("acquire", { automationStopped: true })).error,
    "browser_automation_stop_required",
  );
  await hostRequest(f.socketPath, {
    op: "access_confirm_stopped",
    executionId: f.executionId,
    automationStopped: true,
  });
  const closed = once(agent, "close");
  assert.equal((await view.command("acquire")).result.state, "human");
  await closed;
  const frame = await view.wait(
    (message) => message.type === "frame" && message.interactive,
  );
  assert.equal(
    (
      await view.command("input", {
        generation: frame.generation,
        targetId: frame.targetId,
        frameRevision: "stale",
        action: { op: "text", text: "must not type" },
      })
    ).error,
    "browser_frame_stale",
  );
  const before = view.messages.length;
  assert.equal((await view.command("refresh")).type, "result");
  const fresh = view.messages.slice(before).find((message) => message.type === "frame");
  assert.equal(
    (
      await view.command("input", {
        generation: fresh.generation,
        targetId: fresh.targetId,
        frameRevision: fresh.frameRevision,
        action: { op: "text", text: "你好" },
      })
    ).type,
    "result",
  );
  assert(
    f.calls.some(
      (call) => call.method === "Input.insertText" && call.params.text === "你好",
    ),
  );
  f.navigate("https://login.example/complete");
  const index = view.messages.length;
  await view.command("refresh");
  const next = view.messages.slice(index).find((message) => message.type === "frame");
  const release = await view.command("release", {
    targetId: next.targetId,
    frameRevision: next.frameRevision,
  });
  assert.equal(release.result.state, "automation");
  assert.equal(release.result.resumed, false);
  await f.agent(access.cdpUrl);
});

test("grant expiry and explicit revoke close established CDP and viewer connections", async (t) => {
  const f = await fixture(t);
  const access = await f.issue();
  const agent = await f.agent(access.cdpUrl);
  const view = await f.viewer(access.viewerUrl);
  await view.wait((message) => message.type === "state");
  const closed = [once(agent, "close"), once(view.client, "close")];
  await hostRequest(f.socketPath, { op: "access_revoke", executionId: f.executionId });
  await Promise.all(closed);
  await assert.rejects(sessionRequest(access.cdpUrl), /session_failed/);
  const short = await f.issue({ durationMs: 100 });
  const client = await f.agent(short.cdpUrl);
  await once(client, "close");
  assert.equal(
    (await fetch(short.cdpUrl.replace(/^ws/, "http") + "/session")).status,
    403,
  );
});

test("execution finish revokes links and observing does not keep an idle browser alive", async (t) => {
  const f = await fixture(t);
  const access = await f.issue();
  const agent = await f.agent(access.cdpUrl);
  const view = await f.viewer(access.viewerUrl);
  await view.wait((message) => message.type === "frame");
  agent.terminate();
  await delay(1200);
  assert.equal((await sessionRequest(access.cdpUrl)).state, "lost");
  const closed = once(view.client, "close");
  await f.gateway.command({ op: "finish", executionId: f.executionId }, true);
  await closed;
});

test("cross-origin viewer handshakes and unknown upgrade routes are rejected", async (t) => {
  const f = await fixture(t);
  for (const [path, origin] of [
    ["/view/socket", "https://evil.example"],
    ["/unknown", f.origin],
    ["/execution/invalid", f.origin],
  ]) {
    const client = new WebSocket(f.origin.replace(/^http/, "ws") + path, { origin });
    client.on("error", () => {});
    const status = await new Promise((resolve) =>
      client.once("unexpected-response", (_request, response) => {
        resolve(response.statusCode);
        response.destroy();
        client.terminate();
      }),
    );
    assert.equal(status, 403);
  }
});

test("observers share capture and a late viewer receives a static page without another browser frame", async (t) => {
  const f = await fixture(t);
  const first = await f.issue();
  await f.agent(first.cdpUrl);
  const a = await f.viewer(first.viewerUrl);
  await a.wait((message) => message.type === "frame");
  const second = await f.issue();
  const b = await f.viewer(second.viewerUrl);
  await b.wait((message) => message.type === "frame");
  assert.equal(
    f.calls.filter((call) => call.method === "Page.startScreencast").length,
    1,
  );
});

test("configured embedding origins are allowed while credentials remain scoped", async (t) => {
  const f = await fixture(t, { viewerOrigins: ["https://app.example"] });
  const response = await fetch(f.origin + "/view", {
    headers: { Origin: "https://app.example" },
  });
  assert.equal(response.status, 200);
  assert.match(
    response.headers.get("content-security-policy"),
    /frame-ancestors 'self' https:\/\/app.example/,
  );
  assert.equal(
    (await fetch(f.origin + "/view", { headers: { Origin: "https://other.example" } }))
      .status,
    403,
  );
});

test("a user request waits for an observation poll instead of failing with browser_view_busy", async (t) => {
  const f = await fixture(t);
  const access = await f.issue();
  await f.agent(access.cdpUrl);
  const view = await f.viewer(access.viewerUrl);
  await view.wait((message) => message.type === "frame");
  const gate = f.blockTargetReads();
  await gate.entered;
  let result;
  const request = view.command("selectTarget", { targetId: "page" }).then((value) => {
    result = value;
  });
  await delay(30);
  gate.release();
  assert.equal(result, undefined, "a poll must not reject a legitimate action");
  await request;
  assert.equal(result.type, "result");
});

test("private and remote file routes share one transfer admission limit", async (t) => {
  const f = await fixture(t, {}, { resolvePeerUid: async () => 100001 });
  const local = await f.gateway.command(
    { op: "endpoint", executionId: f.executionId },
    true,
  );
  const remote = await f.issue();
  const localBase = local.wsEndpoint.replace(/^ws/, "http");
  const requests = [];
  try {
    for (let i = 0; i < 16; i++) {
      const upload = request(`${localBase}/files/pending-${i}`, {
        method: "PUT",
        headers: { "content-length": "2" },
      });
      upload.on("error", () => {});
      upload.write("x");
      requests.push(upload);
    }
    let status;
    for (let i = 0; i < 100; i++) {
      const response = await fetch(`${localBase}/files/missing`);
      status = response.status;
      await response.arrayBuffer();
      if (status === 429) break;
      await delay(10);
    }
    assert.equal(status, 429);
    assert.equal(
      (await fetch(remote.cdpUrl.replace(/^ws/, "http") + "/files/missing")).status,
      429,
    );
  } finally {
    for (const upload of requests) upload.destroy();
  }
});

test("only one host-authorized primary viewer can resize the execution viewport", async (t) => {
  const f = await fixture(t);
  const access = await f.issue({ resize: true });
  await assert.rejects(f.issue({ resize: true }), /viewport_busy/);
  await f.agent(access.cdpUrl);
  const view = await f.viewer(access.viewerUrl);
  const state = await view.wait(
    (message) => message.type === "state" && message.generation,
  );
  const frame = await view.wait((message) => message.type === "frame");
  const fields = {
    targetId: frame.targetId,
    generation: state.generation,
    width: 1440,
    height: 900,
  };
  assert.equal((await view.command("resize", fields)).type, "result");
  assert(
    f.calls.some(
      (call) =>
        call.method === "Emulation.setDeviceMetricsOverride" &&
        call.params.width === 1440 &&
        call.params.height === 900,
    ),
  );
  const before = f.calls.filter(
    (call) => call.method === "Emulation.setDeviceMetricsOverride",
  ).length;
  assert.equal(
    (await view.command("resize", { ...fields, width: 0 })).error,
    "browser_viewport_invalid",
  );
  assert.equal(
    (await view.command("resize", { ...fields, height: 2161 })).error,
    "browser_viewport_invalid",
  );
  assert.equal(
    f.calls.filter((call) => call.method === "Emulation.setDeviceMetricsOverride")
      .length,
    before,
  );
  const secondary = await f.issue();
  const observer = await f.viewer(secondary.viewerUrl);
  await observer.wait((message) => message.type === "frame");
  assert.equal(
    (await observer.command("resize", fields)).error,
    "browser_viewport_denied",
  );
  await f.gateway.command({ op: "access_revoke", executionId: f.executionId }, true);
  await f.issue({ resize: true });
});

const authSpec = {
  targetId: "page",
  origin: "https://example.com",
  fields: [
    { id: "password", label: "Password", type: "password", selector: "#password" },
  ],
};

test("structured authentication waits for the viewer and returns only status after closing its input channel", async (t) => {
  const events = [];
  const f = await fixture(t, {
    automation: {
      async pause() {
        events.push("pause");
      },
      async resume() {
        events.push("resume");
      },
    },
  });
  const access = await f.issue({ control: true });
  const readOnly = await f.issue();
  const agent = await f.agent(access.cdpUrl);
  const view = await f.viewer(access.viewerUrl);
  const spectator = await f.viewer(readOnly.viewerUrl);
  const resultPromise = browserAuthRequest(access.cdpUrl, authSpec);
  const state = await view.wait((message) => message.authentication);
  const prompt = state.authentication;
  assert.deepEqual(prompt.fields, [
    { id: "password", label: "Password", type: "password", required: true },
  ]);
  assert.equal(agent.readyState !== WebSocket.OPEN, true);
  const spectatorState = await spectator.wait((message) => message.control === "human");
  assert.equal(spectatorState.authentication, undefined);
  await assert.rejects(browserAuthRequest(access.cdpUrl, authSpec), /request_busy/);
  assert.equal(
    (
      await view.command("authRespond", {
        requestId: prompt.requestId,
        response: { action: "submit", values: { extra: "rejected" } },
      })
    ).error,
    "browser_auth_response_invalid",
  );
  const secret = "unit-test-secret";
  assert.equal(
    (
      await view.command("authRespond", {
        requestId: prompt.requestId,
        response: { action: "submit", values: { password: secret } },
      })
    ).type,
    "result",
  );
  const result = await resultPromise;
  assert.equal(result.status, "submitted");
  assert.equal(result.controlReleased, true);
  assert.equal(result.resumed, true);
  assert.deepEqual(events, ["pause", "resume"]);
  assert(!JSON.stringify(result).includes(secret));
  assert(!JSON.stringify(view.messages).includes(secret));
  assert.equal(
    (
      await view.command("authRespond", {
        requestId: prompt.requestId,
        response: { action: "submit", values: { password: secret } },
      })
    ).error,
    "browser_auth_request_stale",
  );
});

test("authentication requires trusted stop confirmation and consumes it once", async (t) => {
  const f = await fixture(t);
  const access = await f.issue({ control: true });
  await f.agent(access.cdpUrl);
  const view = await f.viewer(access.viewerUrl);
  assert.equal(
    (await browserAuthRequest(access.cdpUrl, authSpec)).status,
    "unavailable",
  );
  await f.gateway.command(
    {
      op: "access_confirm_stopped",
      executionId: f.executionId,
      automationStopped: true,
    },
    true,
  );
  const resultPromise = browserAuthRequest(access.cdpUrl, authSpec);
  const { authentication } = await view.wait((message) => message.authentication);
  await view.command("authRespond", {
    requestId: authentication.requestId,
    response: { action: "cancel" },
  });
  assert.equal((await resultPromise).status, "declined");
  assert.equal(
    (await browserAuthRequest(access.cdpUrl, authSpec)).status,
    "unavailable",
  );
});

test("caller cancellation and timeout clear authentication without blocking finish", async (t) => {
  const f = await fixture(t, { automation: { async pause() {}, async resume() {} } });
  const access = await f.issue({ control: true });
  await f.agent(access.cdpUrl);
  const view = await f.viewer(access.viewerUrl);
  const cancel = new AbortController();
  const waiting = browserAuthRequest(access.cdpUrl, authSpec, cancel.signal);
  await view.wait((message) => message.authentication);
  cancel.abort();
  await assert.rejects(waiting);
  await delay(50);
  const timed = await browserAuthRequest(access.cdpUrl, { ...authSpec, timeoutMs: 50 });
  assert.equal(timed.status, "expired");
  assert.equal(timed.controlReleased, true);
  const pending = browserAuthRequest(access.cdpUrl, authSpec).catch(() => null);
  await delay(100);
  await f.gateway.command({ op: "finalize", executionId: f.executionId }, true);
  await pending;
});

test("authentication rechecks origin before filling and releases the scoped fence after navigation", async (t) => {
  const f = await fixture(t, { automation: { async pause() {}, async resume() {} } });
  const access = await f.issue({ control: true });
  await f.agent(access.cdpUrl);
  const view = await f.viewer(access.viewerUrl);
  const waiting = browserAuthRequest(access.cdpUrl, authSpec);
  const { authentication } = await view.wait((message) => message.authentication);
  f.navigate("https://other.example/login");
  await view.command("authRespond", {
    requestId: authentication.requestId,
    response: { action: "submit", values: { password: "never-injected" } },
  });
  const result = await waiting;
  assert.equal(result.status, "origin_changed");
  assert.equal(result.controlReleased, true);
  assert(!JSON.stringify(f.calls).includes("never-injected"));
});

test("rejected WebSocket upgrades do not consume viewer authentication slots", async (t) => {
  const f = await fixture(t);
  const access = await f.issue();
  for (let index = 0; index < 64; index++) {
    await new Promise((resolve, reject) => {
      const req = request(
        `${f.origin}/view/socket`,
        {
          headers: {
            Connection: "Upgrade",
            Upgrade: "websocket",
            Origin: f.origin,
            "Sec-WebSocket-Version": "13",
          },
        },
        (res) => {
          assert.equal(res.statusCode, 400);
          res.resume();
          res.on("end", resolve);
        },
      );
      req.on("error", reject);
      req.end();
    });
  }
  const view = await f.viewer(access.viewerUrl);
  await view.wait((message) => message.type === "state");
});

test("takeover retains the viewer's selected tab and exposes its control deadline", async (t) => {
  const f = await fixture(t, { automation: { async pause() {}, async resume() {} } });
  f.addTarget({
    type: "page",
    targetId: "second",
    title: "Second",
    url: "https://example.com/second",
  });
  const access = await f.issue({ control: true });
  await f.agent(access.cdpUrl);
  const view = await f.viewer(access.viewerUrl);
  await view.wait((message) => message.type === "frame");
  assert.equal(
    (await view.command("selectTarget", { targetId: "second" })).type,
    "result",
  );
  const takeover = await view.command("acquire");
  assert.equal(takeover.result.targetId, "second");
  const state = await view.wait((message) => message.leasedByYou === true);
  assert.equal(state.leaseExpiresAt, takeover.result.expiresAt);
});

test("authentication parses UTF-8 metadata across HTTP chunks", async (t) => {
  const f = await fixture(t, { automation: { async pause() {}, async resume() {} } });
  const access = await f.issue({ control: true });
  await f.agent(access.cdpUrl);
  const view = await f.viewer(access.viewerUrl);
  const spec = {
    ...authSpec,
    fields: [{ ...authSpec.fields[0], label: "密码", selector: "#密码" }],
  };
  const body = Buffer.from(JSON.stringify(spec));
  const split = body.indexOf(Buffer.from("密")) + 1;
  const resultPromise = new Promise((resolve, reject) => {
    const req = request(
      `${access.cdpUrl.replace(/^ws/, "http")}/auth`,
      { method: "POST" },
      (res) => {
        let output = "";
        res.on("data", (chunk) => {
          output += chunk;
        });
        res.on("end", () => resolve(JSON.parse(output)));
      },
    );
    req.on("error", reject);
    req.write(body.subarray(0, split));
    setTimeout(() => req.end(body.subarray(split)), 25);
  });
  const { authentication } = await view.wait((message) => message.authentication);
  await view.command("authRespond", {
    requestId: authentication.requestId,
    response: { action: "cancel" },
  });
  await resultPromise;
  assert.equal(authentication.fields[0].label, "密码");
});

test("authentication cleanup preserves a newer host retention deadline", async (t) => {
  const f = await fixture(t, { automation: { async pause() {}, async resume() {} } });
  const access = await f.issue({ control: true });
  await f.agent(access.cdpUrl);
  const view = await f.viewer(access.viewerUrl);
  const waiting = browserAuthRequest(access.cdpUrl, authSpec);
  const { authentication } = await view.wait((message) => message.authentication);
  const extended = await f.gateway.command(
    {
      op: "session",
      executionId: f.executionId,
      action: "renew",
      durationMs: 1_200_000,
    },
    true,
  );
  await view.command("authRespond", {
    requestId: authentication.requestId,
    response: { action: "cancel" },
  });
  await waiting;
  assert.equal((await sessionRequest(access.cdpUrl)).expiresAt, extended.expiresAt);
});
