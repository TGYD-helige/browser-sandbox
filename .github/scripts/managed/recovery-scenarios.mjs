import assert from "node:assert/strict";
import { randomBytes, randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";

const root = process.env.BROWSER_PACKAGE_ROOT ?? "/opt/browser-sandbox";
const { connectCdp } = await import(`${root}/dist/cdp.js`);
const { createBrowserSessions, createControllerRuntime, sessionRequest } = await import(
  `${root}/dist/index.js`
);
const origin = process.env.TEST_CONTROLLER_ORIGIN ?? "http://controller:8080";
const runtime = createControllerRuntime(origin, {
  automation: { pause: async () => {}, resume: async () => {} },
});
const sessions = createBrowserSessions({
  provider: {
    identity: "managed",
    acquire: async () => runtime,
    release: async () => {},
  },
});
const active = new Map();
const report = {};
const phase = process.env.TEST_RECOVERY_PHASE ?? "core";
const status = (executionId) =>
  runtime.request({ op: "controller_status", ...(executionId ? { executionId } : {}) });
async function wait(description, read, predicate, timeout = 20000) {
  const deadline = Date.now() + timeout;
  while (true) {
    const value = await read();
    if (predicate(value)) return value;
    assert(Date.now() < deadline, description);
    await delay(25);
  }
}
async function closeCdp(current) {
  current.cdp?.close();
  await current.cdp?.closed;
  current.cdp = undefined;
}
async function open(options = {}) {
  const handle = await sessions.open(options);
  const current = { handle, cdp: undefined };
  active.set(handle.executionId, current);
  current.cdp = await connectCdp(handle.wsEndpoint);
  await current.cdp.call("Browser.getVersion");
  return current;
}
async function finish(current) {
  await closeCdp(current);
  await current.handle.finalize();
  active.delete(current.handle.executionId);
}
async function retained(current) {
  await sessionRequest(current.handle.wsEndpoint, "retain", 180000);
  await closeCdp(current);
}
async function form(current) {
  const { targetId } = await current.cdp.call("Target.createTarget", {
    url: "about:blank",
  });
  const { sessionId } = await current.cdp.call("Target.attachToTarget", {
    targetId,
    flatten: true,
  });
  const html = "<title>Recovery fixture</title><input id=username type=text>";
  let failure;
  current.cdp.subscribe((event) => {
    if (event.method === "Fetch.requestPaused" && event.sessionId === sessionId)
      current.cdp
        .call(
          "Fetch.fulfillRequest",
          {
            requestId: event.params.requestId,
            responseCode: 200,
            responseHeaders: [{ name: "content-type", value: "text/html" }],
            body: Buffer.from(html).toString("base64"),
          },
          sessionId,
        )
        .catch((error) => {
          failure = error;
        });
  });
  await current.cdp.call(
    "Fetch.enable",
    { patterns: [{ urlPattern: "https://recovery.example.test/*" }] },
    sessionId,
  );
  await current.cdp.call(
    "Page.navigate",
    { url: "https://recovery.example.test/login" },
    sessionId,
  );
  await wait(
    "Controlled sign-in page did not commit",
    async () => {
      if (failure) throw failure;
      return (
        await current.cdp.call(
          "Runtime.evaluate",
          { expression: "!!document.querySelector('#username')" },
          sessionId,
        )
      ).result.value;
    },
    Boolean,
  );
  return targetId;
}
try {
  const limits = (await status()).limits;
  if (phase === "core") {
    const current = await open();
    const id = current.handle.executionId;
    const targetId = await form(current);
    await wait(
      "Connected clients did not protect worker",
      () => status(id),
      (value) => value.execution.idleProtection === "clients",
    );
    await retained(current);
    await wait(
      "Retention did not protect worker",
      () => status(id),
      (value) => value.execution.idleProtection === "retention",
    );
    const identity = { actorId: randomUUID(), leaseId: randomUUID() };
    const lease = await runtime.request({
      op: "takeover",
      executionId: id,
      action: { op: "acquire", ...identity, targetId, automationStopped: true },
    });
    await runtime.request({ op: "session", executionId: id, action: "release" });
    await wait(
      "Human takeover did not protect worker",
      () => status(id),
      (value) => value.execution.idleProtection === "takeover",
    );
    await runtime.request({
      op: "session",
      executionId: id,
      action: "retain",
      durationMs: 180000,
    });
    await runtime.request({
      op: "takeover",
      executionId: id,
      action: { op: "release", ...identity, epoch: lease.epoch },
    });
    const caller = new AbortController();
    const auth = fetch(`${current.handle.wsEndpoint.replace(/^ws/, "http")}/auth`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        targetId,
        origin: "https://recovery.example.test",
        fields: [
          { id: "username", label: "Username", type: "text", selector: "#username" },
        ],
        timeoutMs: 60000,
      }),
      signal: caller.signal,
    });
    const cancelled = assert.rejects(auth);
    cancelled.catch(() => {});
    try {
      await wait(
        "Pending sign-in did not protect worker",
        () => status(id),
        (value) => value.execution.idleProtection === "auth",
      );
    } finally {
      caller.abort();
      await cancelled;
    }
    await finish(current);
    report.idleBlockers = true;

    const occupied = [];
    for (let index = 0; index < limits.maxWorkers; index++) {
      const item = await open();
      occupied.push(item);
      await retained(item);
    }
    const queued = await sessions.open();
    active.set(queued.executionId, { handle: queued });
    const callerQueue = new AbortController();
    const pending = fetch(`${queued.wsEndpoint.replace(/^ws/, "http")}/files/wait`, {
      signal: callerQueue.signal,
    });
    const queueCancelled = assert.rejects(pending);
    queueCancelled.catch(() => {});
    try {
      await wait("Creation was not queued", status, (value) => value.queued === 1);
      const overflow = await sessions.open();
      active.set(overflow.executionId, { handle: overflow });
      const response = await fetch(
        `${overflow.wsEndpoint.replace(/^ws/, "http")}/files/overflow`,
      );
      assert.equal(response.status, 502);
      assert.equal((await response.json()).error, "browser_worker_queue_full");
      await overflow.finalize();
      active.delete(overflow.executionId);
    } finally {
      callerQueue.abort();
      await queueCancelled;
    }
    await wait(
      "Disconnected waiter retained its queue entry",
      status,
      (value) => value.queued === 0 && value.pending === 0,
    );
    assert.equal((await status()).workers, limits.maxWorkers);
    await queued.finalize();
    active.delete(queued.executionId);
    for (const item of occupied) await finish(item);
    report.queueOverflowAndDisconnect = true;

    const unused = await sessions.open({ profileId: `unused-${randomUUID()}` });
    active.set(unused.executionId, { handle: unused });
    await wait(
      "Unused registration did not expire",
      () => status(unused.executionId),
      (value) => value.execution.phase === "finished",
      limits.idleRegistrationMs + 20000,
    );
    assert.equal((await status()).workers, 0);
    await unused.finalize();
    active.delete(unused.executionId);
    report.unusedTtl = true;
  } else if (phase === "ttl-volume" || phase === "ttl-host") {
    const profileId = `abandoned-${randomUUID()}`;
    let latest,
      failSave = false;
    const host = {
      async load() {
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
      async save() {
        if (failSave) throw Error("browser_synthetic_save_failure");
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
        if (latest?.archive.sha256 !== result.archive.sha256) {
          assert.equal(result.previousSha256, latest?.archive.sha256 ?? null);
          latest = { archive: result.archive, key: result.archive.keyMaterial.key };
        }
        return { confirmedSha: result.archive.sha256 };
      },
    };
    const options = { profileId, ...(phase === "ttl-host" ? { storage: host } : {}) };
    const cookie = (value) => ({
      name: "maximum_lifetime",
      value,
      domain: "recovery.example.test",
      path: "/",
      secure: true,
      httpOnly: true,
      expires: Math.floor(Date.now() / 1000) + 3600,
    });
    if (phase === "ttl-host") {
      assert(process.env.BROWSER_HOST_ARCHIVE_URL, "Host archive URL required");
      const seeded = await open(options);
      await seeded.cdp.call("Storage.setCookies", {
        cookies: [cookie("old-confirmed")],
      });
      await finish(seeded);
    }
    const abandoned = await open(options);
    await abandoned.cdp.call("Storage.setCookies", {
      cookies: [cookie("saved-at-maximum")],
    });
    await retained(abandoned);
    if (phase === "ttl-host") {
      const confirmed = latest.archive.sha256;
      failSave = true;
      await wait(
        "Failed save did not retain ownership",
        () => status(abandoned.handle.executionId),
        (value) =>
          value.execution.phase === "finalizing" &&
          value.execution.statusError === "browser_synthetic_save_failure",
        limits.maxExecutionMs + 60000,
      );
      assert.equal(latest.archive.sha256, confirmed);
      await assert.rejects(sessions.open(options), /browser_profile_busy/);
      assert.equal((await status()).workers, 1);
      failSave = false;
    }
    const completed = await wait(
      "Maximum lifetime did not finalize Profile",
      () => status(abandoned.handle.executionId),
      (value) => value.execution.phase === "finished",
      limits.maxExecutionMs + 60000,
    );
    assert.equal(completed.execution.reclaimReason, "maximum-lifetime");
    await abandoned.handle.finalize();
    active.delete(abandoned.handle.executionId);
    const restored = await open(options);
    assert(
      (await restored.cdp.call("Storage.getCookies")).cookies.some(
        (item) => item.name === "maximum_lifetime" && item.value === "saved-at-maximum",
      ),
    );
    await finish(restored);
    report.maximumLifetimeSavedProfile = true;
    if (phase === "ttl-host") report.failedSavePreservedConfirmedArchive = true;
  } else throw Error("Unknown recovery scenario");
  console.log(JSON.stringify({ phase, ...report }));
} finally {
  for (const current of active.values()) {
    await closeCdp(current);
    await current.handle.finalize().catch(() => {});
  }
  runtime.close();
}
