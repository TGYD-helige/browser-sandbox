import assert from "node:assert/strict";
import { test } from "node:test";
import { WORKER_PROTOCOL, workerIdle } from "../dist/worker-protocol.js";

const status = (session = {}, extra = {}) => ({
  protocol: "browser-sandbox.worker/v1",
  session: {
    state: "live",
    generation: "generation-1",
    connectedClients: 0,
    hardExpiresAt: 10000,
    ...session,
  },
  takeover: "automation",
  pendingAuth: 0,
  ...extra,
});

test("idle reclamation keeps clients, retained pages, takeover and pending sign-in", () => {
  assert.equal(WORKER_PROTOCOL, "browser-sandbox.worker/v1");
  assert.equal(workerIdle(status(), 1000), true);
  assert.equal(workerIdle(status({ connectedClients: 1 }), 1000), false);
  assert.equal(workerIdle(status({ state: "retained", expiresAt: 2000 }), 1000), false);
  assert.equal(workerIdle(status({ state: "starting" }), 1000), false);
  assert.equal(workerIdle(status({}, { takeover: "human" }), 1000), false);
  assert.equal(workerIdle(status({}, { takeover: "stopping" }), 1000), false);
  assert.equal(workerIdle(status({}, { takeover: "uncertain" }), 1000), false);
  assert.equal(workerIdle(status({}, { pendingAuth: 1 }), 1000), false);
  assert.equal(workerIdle(status({ state: "retained", expiresAt: 1000 }), 1000), true);
});

test("failed, incomplete and incompatible status never authorizes idle deletion", () => {
  for (const value of [
    undefined,
    null,
    {},
    status({}, { protocol: "browser-sandbox.worker/v2" }),
    status({}, { pendingAuth: undefined }),
    status({}, { takeover: undefined }),
    status({ connectedClients: -1 }),
    status({ connectedClients: undefined }),
    status({ state: "retained", expiresAt: undefined }),
    status({ state: "retained", expiresAt: NaN }),
    status({ state: "unrecognized" }),
  ])
    assert.equal(workerIdle(value, 1000), false);
});
