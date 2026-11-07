import assert from "node:assert/strict";
import { hostRequest } from "../../dist/host-client.js";
import { execution, until, workload } from "./benchmark-fixture.mjs";

export async function benchmarkQueue({ socket, samples }) {
  const measurements = {
    cancelAndFinalize: [],
    queuedWait: [],
    queueAndStartup: [],
    successorReady: [],
  };
  for (let sample = -3; sample < samples; sample++) {
    const clients = [],
      holders = [];
    try {
      for (let i = 0; i < 2; i++) {
        const client = await execution(socket);
        clients.push(client);
        holders.push(await client.connect());
      }
      const queued = await execution(socket);
      clients.push(queued);
      const cancelled = new AbortController();
      const pending = queued.connect(cancelled.signal);
      const rejected = assert.rejects(pending);
      await until(
        async () =>
          (
            await hostRequest(socket, {
              op: "session",
              executionId: queued.executionId,
              action: "status",
            })
          ).state === "starting",
        "The third browser did not enter startup",
      );
      let start = performance.now();
      cancelled.abort();
      await rejected;
      await queued.finish();
      const cancelTime = performance.now() - start;
      await assert.rejects(queued.connect(AbortSignal.timeout(2000)), /browser_cdp_/);
      const successor = await execution(socket);
      clients.push(successor);
      const requested = performance.now();
      const ready = successor.connect();
      let admitted = false;
      ready.then(
        () => {
          admitted = true;
        },
        () => {},
      );
      await until(
        async () =>
          (
            await hostRequest(socket, {
              op: "session",
              executionId: successor.executionId,
              action: "status",
            })
          ).state === "starting",
        "Successor did not enter startup",
      );
      const queuedAt = performance.now();
      for (let i = 0; i < 50; i++)
        assert((await holders[0].call("Browser.getVersion")).product);
      const waited = performance.now() - queuedAt;
      assert.equal(admitted, false, "A queued successor bypassed browser capacity");
      start = performance.now();
      await clients[0].finish();
      await ready;
      if (sample >= 0) {
        measurements.cancelAndFinalize.push(cancelTime);
        measurements.queuedWait.push(waited);
        measurements.queueAndStartup.push(performance.now() - requested);
        measurements.successorReady.push(performance.now() - start);
      }
    } finally {
      for (const client of clients.reverse()) await client.finish();
    }
  }
  return workload(
    "queue-cancel-recovery",
    measurements,
    {},
    {
      note: "Two live browsers hold capacity. Cancel a third starting connection, finalize it, reject its revoked grant, then hold a successor in the observed starting state while an active browser completes fifty commands, release one slot, and await the successor's real CDP reply. Queued wait is the observed starting-to-release interval; queue-and-startup includes admission and launch.",
    },
  );
}
