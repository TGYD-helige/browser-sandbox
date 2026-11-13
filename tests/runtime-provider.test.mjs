import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import WebSocket, { WebSocketServer } from "ws";
import { createGateway } from "../dist/gateway.js";
import {
  createBrowserSessions,
  createLocalRuntimeProvider,
} from "../dist/runtime-provider.js";

async function gateway(options = {}) {
  const root = await mkdtemp(join(tmpdir(), "browser-provider-"));
  const socketPath = join(root, "control.sock");
  try {
    const instance = await createGateway({
      WebSocket,
      WebSocketServer,
      port: 0,
      dataRoot: join(root, "data"),
      socketPath,
      healthy: async () => true,
      launch: () => assert.fail("registration must not launch a browser"),
      ...options,
    });
    return {
      socketPath,
      async stop() {
        try {
          await instance.stop();
        } finally {
          await rm(root, { recursive: true, force: true });
        }
      },
    };
  } catch (error) {
    await rm(root, { recursive: true, force: true });
    throw error;
  }
}

function fakeProvider(request) {
  const released = [];
  const runtime = {
    request,
    attachStorage: () => assert.fail("storage was not requested"),
  };
  return {
    released,
    provider: {
      acquire: async () => runtime,
      release: async (placement) => released.push(placement.executionId),
    },
  };
}

test("local sessions get distinct executions and UIDs that are reused only after finalization", async () => {
  const runtime = await gateway();
  try {
    const sessions = createBrowserSessions({
      provider: createLocalRuntimeProvider(runtime.socketPath),
      firstUid: 100001,
      uidCount: 2,
    });
    const first = await sessions.open();
    const second = await sessions.open();
    assert.notEqual(first.executionId, second.executionId);
    assert.notEqual(first.wsEndpoint, second.wsEndpoint);
    assert.deepEqual([first.executorUid, second.executorUid], [100001, 100002]);
    await assert.rejects(sessions.open(), /browser_executor_uids_exhausted/);

    assert.deepEqual(await first.finalize(), { used: false, stopped: true });
    const third = await sessions.open();
    assert.equal(third.executorUid, 100001);
    // A repeated finalize must not free the UID now owned by another session.
    await first.finalize();
    await assert.rejects(sessions.open(), /browser_executor_uids_exhausted/);
    await Promise.all([second.finalize(), third.finalize()]);
  } finally {
    await runtime.stop();
  }
});

test("the shared local provider refuses tenant placement without consuming a UID", async () => {
  const runtime = await gateway();
  try {
    const sessions = createBrowserSessions({
      provider: createLocalRuntimeProvider(runtime.socketPath),
      uidCount: 1,
    });
    await assert.rejects(
      sessions.open({ tenantId: "tenant-a" }),
      /browser_tenant_isolation_unavailable/,
    );
    await (await sessions.open()).finalize();
  } finally {
    await runtime.stop();
  }
});

test("an unconfirmed finalization keeps the UID reserved until a retry succeeds", async () => {
  let failFinalize = true;
  const { provider, released } = fakeProvider(async (request) => {
    if (request.op === "register")
      return { wsEndpoint: `ws://runtime/${request.executionId}` };
    if (failFinalize) throw Error("browser_profile_save_unconfirmed");
    return { stopped: true };
  });
  const sessions = createBrowserSessions({ provider, uidCount: 1 });
  const session = await sessions.open();
  await assert.rejects(session.finalize(), /browser_profile_save_unconfirmed/);
  await assert.rejects(sessions.open(), /browser_executor_uids_exhausted/);
  assert.deepEqual(released, []);
  failFinalize = false;
  assert.deepEqual(await session.finalize(), { stopped: true });
  assert.deepEqual(released, [session.executionId]);
  assert.equal((await sessions.open()).executorUid, session.executorUid);
});

test("invalid UID ranges are rejected", () => {
  const provider = createLocalRuntimeProvider("/nonexistent");
  for (const range of [
    { firstUid: 99999 },
    { uidCount: 0 },
    { firstUid: 1999999999, uidCount: 2 },
  ])
    assert.throws(
      () => createBrowserSessions({ provider, ...range }),
      /browser_executor_identity_invalid/,
    );
});

test("a UID held outside this pool is skipped instead of reused", async () => {
  const runtime = await gateway();
  try {
    const provider = createLocalRuntimeProvider(runtime.socketPath);
    const outside = await createBrowserSessions({ provider, uidCount: 1 }).open();
    const sessions = createBrowserSessions({ provider, uidCount: 2 });
    await assert.rejects(sessions.open(), /browser_executor_uid_in_use/);
    const next = await sessions.open();
    assert.equal(next.executorUid, outside.executorUid + 1);
    await Promise.all([outside.finalize(), next.finalize()]);
  } finally {
    await runtime.stop();
  }
});

test("a failed registration whose cleanup is unconfirmed reports both errors and keeps its UID", async () => {
  const { provider, released } = fakeProvider(async (request) => {
    throw Error(
      request.op === "register"
        ? "browser_control_timeout"
        : "browser_control_unavailable",
    );
  });
  const sessions = createBrowserSessions({ provider, uidCount: 1 });
  await assert.rejects(sessions.open(), (error) => {
    assert.equal(error.message, "browser_session_cleanup_unconfirmed");
    assert.match(error.executionId, /^[0-9a-f-]{36}$/);
    assert.equal(error.executorUid, 100001);
    assert.deepEqual(
      error.errors.map((item) => item.message),
      ["browser_control_timeout", "browser_control_unavailable"],
    );
    return true;
  });
  assert.deepEqual(released, []);
  await assert.rejects(sessions.open(), /browser_executor_uids_exhausted/);
});
