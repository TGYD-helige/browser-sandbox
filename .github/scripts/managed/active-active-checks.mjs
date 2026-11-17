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
    !own.some((event) => ["removed", "finalized", "commit"].includes(event.type)),
    "Resumed old controller performed a destructive action",
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
