import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";

export function assertFencedOwner(events, executionId) {
  const own = events.filter((event) => event.executionId === executionId);
  assert(
    own.some(
      (event) => event.type === "fenced" && event.error === "browser_controller_fenced",
    ),
    "Resumed controller did not report its own fencing",
  );
  assert(
    !own.some((event) => event.type === "removed"),
    "Resumed old controller reported worker removal",
  );
}

export async function observeSingleRemoval(
  readCount,
  { durationMs = 6000, now = () => performance.now(), pause = delay } = {},
) {
  const started = now();
  while (true) {
    assert.equal(await readCount(), 1, "Orphan was reclaimed more than once");
    const remaining = durationMs - (now() - started);
    if (remaining <= 0) return now() - started;
    await pause(Math.min(remaining, 100));
  }
}

export function summarizeFailover(values) {
  const sorted = [...values].sort((a, b) => a - b);
  return {
    medianMs: sorted[Math.floor(sorted.length / 2)],
    maxMs: sorted.at(-1),
    samples: sorted.length,
  };
}

// Rollout can close a Service-selected replica after CDP checks but before status.
export async function readStatusDuringTakeover(read, signal) {
  while (true) {
    signal.throwIfAborted();
    try {
      const status = await read(signal);
      signal.throwIfAborted();
      return status;
    } catch (error) {
      signal.throwIfAborted();
      if (
        error.message !== "browser_controller_owner_moved" &&
        !["UND_ERR_SOCKET", "ECONNRESET", "ECONNREFUSED"].includes(error.cause?.code)
      )
        throw error;
    }
    await delay(100, undefined, { signal });
  }
}
