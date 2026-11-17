import assert from "node:assert/strict";
import { test } from "node:test";
import {
  assertFencedOwner,
  observeSingleRemoval,
  summarizeFailover,
} from "./active-active-checks.mjs";

test("resumed controller evidence requires its own fencing report and excludes destructive events", () => {
  assert.throws(() => assertFencedOwner([], "execution"));
  const fenced = {
    executionId: "execution",
    type: "fenced",
    error: "browser_controller_fenced",
  };
  for (const type of ["removed", "finalized", "commit"])
    assert.throws(() =>
      assertFencedOwner([fenced, { executionId: "execution", type }], "execution"),
    );
  assert.doesNotThrow(() => assertFencedOwner([fenced], "execution"));
});

test("orphan evidence continues observing after the first removal and detects a late duplicate", async () => {
  // Synthetic event timing: the duplicate appears after the initial successful observation.
  let clock = 0,
    reads = 0;
  const options = {
    durationMs: 600,
    now: () => clock,
    pause: async (ms) => {
      clock += ms;
    },
  };
  await assert.rejects(
    observeSingleRemoval(async () => {
      reads++;
      return clock < 300 ? 1 : 2;
    }, options),
  );
  assert(reads > 1);
  clock = 0;
  assert.equal(await observeSingleRemoval(async () => 1, options), 600);
});

test("five failover observations report a maximum rather than a population P95", () => {
  assert.deepEqual(summarizeFailover([5, 1, 3, 4, 2]), {
    medianMs: 3,
    maxMs: 5,
    samples: 5,
  });
});
