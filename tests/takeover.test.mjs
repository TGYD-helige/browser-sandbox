import assert from "node:assert/strict";
import test from "node:test";
import { Takeover } from "../dist/takeover.js";

test("a lease is granted only after automation has stopped and requires the same holder", async () => {
  let finish;
  const stopped = new Promise((resolve) => {
    finish = resolve;
  });
  const control = new Takeover(
    "ws://unused",
    () => stopped,
    async () => ({
      close() {},
      async call(method) {
        if (method === "Target.getTargets")
          return {
            targetInfos: [
              { type: "page", targetId: "blank", url: "about:blank" },
              { type: "page", targetId: "page", url: "https://example.com/login" },
            ],
          };
        return { sessionId: "session" };
      },
    }),
  );
  const acquiring = control.command({
    op: "acquire",
    actorId: "human",
    leaseId: "lease",
    automationStopped: true,
  });
  assert.equal(control.state, "stopping");
  await assert.rejects(
    control.command({ op: "release", actorId: "human", leaseId: "lease" }),
    /stale/,
  );
  finish();
  const lease = await acquiring;
  assert.equal(lease.state, "human");
  assert.equal(lease.targetId, "page");
  await assert.rejects(
    control.command({
      op: "release",
      actorId: "other",
      leaseId: "lease",
      epoch: lease.epoch,
      origin: "https://example.com",
    }),
    /stale/,
  );
  assert.equal(
    (
      await control.command({
        op: "release",
        actorId: "human",
        leaseId: "lease",
        epoch: lease.epoch,
        origin: "https://example.com",
      })
    ).state,
    "automation",
  );
  assert.equal(
    (
      await control.command({
        op: "release",
        actorId: "human",
        leaseId: "lease",
        epoch: lease.epoch,
        origin: "https://example.com",
      })
    ).state,
    "automation",
  );
});

test("uncertain automation stop never grants human control", async () => {
  const control = new Takeover("ws://unused", async () => {
    throw Error("still running");
  });
  await assert.rejects(
    control.command({
      op: "acquire",
      actorId: "human",
      leaseId: "lease",
      automationStopped: true,
    }),
    /stop_uncertain/,
  );
  assert.equal(control.state, "uncertain");
});

function browser() {
  const calls = [];
  let url = "https://example.com/login",
    generation = "first";
  return {
    calls,
    navigate(value) {
      url = value;
      generation += "-next";
    },
    connect: async () => ({
      close() {},
      async call(method, params = {}) {
        calls.push({ method, params });
        if (method === "Target.getTargets")
          return { targetInfos: [{ type: "page", targetId: "page", url }] };
        if (method === "Target.attachToTarget") return { sessionId: "session" };
        if (method === "Page.getFrameTree")
          return { frameTree: { frame: { id: "frame", loaderId: generation, url } } };
        if (method === "Page.captureScreenshot") return { data: "jpeg" };
        if (method === "Page.getNavigationHistory")
          return { currentIndex: 1, entries: [{ id: 10 }, { id: 20 }, { id: 30 }] };
        if (method === "Page.getLayoutMetrics")
          return { cssVisualViewport: { clientWidth: 800, clientHeight: 600 } };
        if (method === "Page.createIsolatedWorld") return { executionContextId: 1 };
        if (method === "Runtime.evaluate")
          return {
            result: {
              value: { url: "https://example.com/api/me", text: '{"loggedIn":true}' },
            },
          };
        return {};
      },
    }),
  };
}

test("takeover queries and renews its holder, rejects changed origins on release, and replaces safely", async () => {
  const b = browser();
  const control = new Takeover("ws://unused", async () => {}, b.connect);
  const grant = {
    op: "acquire",
    actorId: "human",
    leaseId: "lease",
    automationStopped: true,
  };
  const lease = await control.command(grant);
  const identity = { actorId: "human", leaseId: "lease", epoch: lease.epoch };
  assert.equal(
    (await control.command({ op: "status", actorId: "other" })).epoch,
    undefined,
  );
  assert.equal(
    (await control.command({ op: "status", actorId: "human" })).leasedByYou,
    true,
  );
  assert.equal((await control.command(grant)).epoch, lease.epoch);
  assert.equal((await control.command({ ...identity, op: "renew" })).state, "human");
  assert(b.calls.some((call) => call.method === "Runtime.terminateExecution"));
  assert(b.calls.some((call) => call.method === "Page.stopLoading"));
  b.navigate("https://other.example/login");
  await assert.rejects(
    control.command({ ...identity, op: "release", origin: "https://other.example" }),
    /origin_mismatch/,
  );
  assert.equal(control.state, "uncertain");
  const replacement = await control.command({
    ...grant,
    leaseId: "new",
    replaceExisting: true,
  });
  assert.notEqual(replacement.epoch, lease.epoch);
  await assert.rejects(control.command({ ...identity, op: "renew" }), /stale/);
});

test("lease expiry fences input and replacement requires another confirmed automation stop", async (t) => {
  const b = browser();
  let now = Date.now(),
    stops = 0;
  t.mock.method(Date, "now", () => now);
  const control = new Takeover(
    "ws://unused",
    async () => {
      stops++;
    },
    b.connect,
  );
  const grant = {
    op: "acquire",
    actorId: "human",
    leaseId: "lease",
    automationStopped: true,
  };
  const lease = await control.command(grant);
  const identity = { actorId: "human", leaseId: "lease", epoch: lease.epoch };
  const frame = await control.command({ ...identity, op: "frame" });
  now = lease.expiresAt;
  await assert.rejects(
    control.command({ ...identity, ...frame, op: "text", text: "expired" }),
    /control_expired/,
  );
  assert.equal(
    (await control.command({ op: "status", actorId: "human" })).state,
    "uncertain",
  );
  await assert.rejects(
    control.command({ ...grant, automationStopped: false }),
    /takeover_request_invalid/,
  );
  assert.equal(stops, 1);
  const replacement = await control.command({ ...grant, leaseId: "replacement" });
  assert.equal(stops, 2);
  assert.equal(replacement.state, "human");
  assert.notEqual(replacement.epoch, lease.epoch);
  await assert.rejects(control.command({ ...identity, op: "renew" }), /control_stale/);
});

test("human input consumes one fresh frame and rejects navigation and out-of-bounds pointers", async () => {
  const b = browser();
  const control = new Takeover("ws://unused", async () => {}, b.connect);
  const lease = await control.command({
    op: "acquire",
    actorId: "human",
    leaseId: "lease",
    automationStopped: true,
  });
  const identity = { actorId: "human", leaseId: "lease", epoch: lease.epoch };
  let frame = await control.command({ ...identity, op: "frame" });
  await assert.rejects(
    control.command({ ...identity, ...frame, op: "pointer", x: 800, y: 10 }),
    /pointer_invalid/,
  );
  frame = await control.command({ ...identity, op: "frame" });
  await control.command({ ...identity, ...frame, op: "text", text: "hello" });
  await assert.rejects(
    control.command({ ...identity, ...frame, op: "text", text: "again" }),
    /frame_stale/,
  );
  frame = await control.command({ ...identity, op: "frame" });
  b.navigate("https://example.com/next");
  await assert.rejects(
    control.command({ ...identity, ...frame, op: "text", text: "stale" }),
    /frame_stale/,
  );
});

test("same-origin JSON readback fences automation and rejects cross-origin or credentialed URLs", async () => {
  const b = browser();
  const control = new Takeover("ws://unused", async () => {}, b.connect);
  const result = await control.command({
    op: "readback",
    url: "https://example.com/api/me",
  });
  assert.equal(JSON.parse(result.text).loggedIn, true);
  assert.equal(control.state, "automation");
  assert(b.calls.some((call) => call.method === "Page.createIsolatedWorld"));
  assert(
    b.calls.some(
      (call) =>
        call.method === "Network.setBypassServiceWorker" &&
        call.params.bypass === false,
    ),
  );
  for (const url of [
    "https://other.example/me",
    "https://user:password@example.com/me",
    "http://example.com/me",
  ]) {
    await assert.rejects(control.command({ op: "readback", url }), /origin_mismatch/);
    assert.equal(control.state, "automation");
  }
});

test("scroll uses bounded viewport coordinates and explicit handoff confirms a new origin from a fresh frame", async () => {
  const b = browser();
  const control = new Takeover("ws://unused", async () => {}, b.connect);
  const lease = await control.command({
    op: "acquire",
    actorId: "human",
    leaseId: "lease",
    automationStopped: true,
  });
  const identity = { actorId: "human", leaseId: "lease", epoch: lease.epoch };
  assert.equal(control.heldBy("human", "lease", lease.epoch), true);
  assert.equal(control.heldBy("other", "lease", lease.epoch), false);
  let frame = await control.command({ ...identity, op: "frame" });
  await assert.rejects(
    control.command({
      ...identity,
      ...frame,
      op: "scroll",
      x: 10,
      y: 20,
      deltaX: 0,
      deltaY: 2001,
    }),
    /pointer_invalid/,
  );
  frame = await control.command({ ...identity, op: "frame" });
  await control.command({
    ...identity,
    ...frame,
    op: "scroll",
    x: 10,
    y: 20,
    deltaX: 0,
    deltaY: 200,
  });
  assert(
    b.calls.some(
      (call) =>
        call.method === "Input.dispatchMouseEvent" && call.params.type === "mouseWheel",
    ),
  );
  b.navigate("https://other.example/complete");
  frame = await control.command({ ...identity, op: "frame" });
  await assert.rejects(
    control.command({
      ...identity,
      ...frame,
      op: "confirmTarget",
      frameRevision: "old",
    }),
    /frame_stale/,
  );
  await control.command({ ...identity, ...frame, op: "confirmTarget" });
  assert.equal(
    (
      await control.command({
        ...identity,
        op: "release",
        origin: "https://other.example",
      })
    ).state,
    "automation",
  );
});

test("embedded navigation and editing keys retain lease and frame checks", async () => {
  const b = browser();
  const control = new Takeover("ws://unused", async () => {}, b.connect);
  const lease = await control.command({
    op: "acquire",
    actorId: "human",
    leaseId: "lease",
    automationStopped: true,
  });
  const identity = { actorId: "human", leaseId: "lease", epoch: lease.epoch };
  const apply = async (action) =>
    control.command({
      ...identity,
      ...(await control.command({ ...identity, op: "frame" })),
      ...action,
    });
  for (const url of [
    "javascript:alert(1)",
    "file:///etc/passwd",
    "https://user:secret@example.com",
  ])
    await assert.rejects(apply({ op: "navigate", url }), /navigation_invalid/);
  await apply({ op: "navigate", url: "https://example.com/next" });
  await apply({ op: "reload" });
  await apply({ op: "key", key: "Back" });
  await apply({ op: "key", key: "Forward" });
  await apply({ op: "key", key: "SelectAll" });
  await apply({ op: "key", key: "Tab", modifiers: 8 });
  await assert.rejects(
    apply({ op: "key", key: "Tab", modifiers: 2 }),
    /operation_invalid/,
  );
  assert(
    b.calls.some(
      (call) =>
        call.method === "Page.navigate" &&
        call.params.url === "https://example.com/next",
    ),
  );
  assert(b.calls.some((call) => call.method === "Page.reload"));
  assert.deepEqual(
    b.calls
      .filter((call) => call.method === "Page.navigateToHistoryEntry")
      .map((call) => call.params.entryId),
    [10, 30],
  );
  assert(
    b.calls.some(
      (call) =>
        call.method === "Input.dispatchKeyEvent" &&
        call.params.key === "a" &&
        call.params.modifiers === 2,
    ),
  );
});
