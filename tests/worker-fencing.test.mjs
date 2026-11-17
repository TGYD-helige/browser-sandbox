import assert from "node:assert/strict";
import { test } from "node:test";
import { createWorkerFence } from "../dist/worker-protocol.js";

test("a higher controller epoch cancels old work and fences requests and callbacks", async () => {
  let entered;
  const started = new Promise((resolve) => {
    entered = resolve;
  });
  let oldSignal;
  let oldPeer;
  const fence = createWorkerFence(async (request, peer, signal) => {
    if (request.op === "hold") {
      oldSignal = signal;
      oldPeer = peer;
      entered();
      await new Promise((resolve) =>
        signal.addEventListener("abort", resolve, { once: true }),
      );
    }
    return { accepted: true };
  });
  const peer = {
    signal: new AbortController().signal,
    callback: async () => ({ saved: true }),
  };
  const pending = fence({ epoch: 1, op: "hold" }, peer, peer.signal);
  const rejection = assert.rejects(pending, /browser_controller_fenced/);
  await started;
  await fence({ epoch: 2, op: "worker_status" }, peer, peer.signal);
  assert.equal(oldSignal.aborted, true);
  assert.equal(oldPeer.signal.aborted, true);
  await rejection;
  await assert.rejects(oldPeer.callback("commit", {}), /browser_controller_fenced/);
  for (const epoch of [0, 1, undefined])
    await assert.rejects(
      fence({ epoch, op: "finalize" }, peer, peer.signal),
      /browser_controller_fenced/,
    );
  for (const epoch of [-1, 1.5, "3"])
    await assert.rejects(
      fence({ epoch, op: "finalize" }, peer, peer.signal),
      /browser_controller_epoch_invalid/,
    );
});
