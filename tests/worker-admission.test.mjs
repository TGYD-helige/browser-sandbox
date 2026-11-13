import assert from "node:assert/strict";
import { test } from "node:test";
import { createWorkerAdmission } from "../dist/worker-admission.js";

test("admission bounds workers, creations and queued requests independently", async () => {
  const admission = createWorkerAdmission({
    maxWorkers: 2,
    maxPending: 1,
    maxQueued: 1,
    creationTimeoutMs: 10000,
  });
  const first = await admission.acquire();
  const pending = admission.acquire();
  await assert.rejects(admission.acquire(), /browser_worker_queue_full/);
  first.ready();
  const second = await pending;
  second.ready();
  const waiting = admission.acquire();
  await assert.rejects(admission.acquire(), /browser_worker_queue_full/);
  first.release();
  const third = await waiting;
  third.release();
  second.release();
  assert.deepEqual(admission.status(), { workers: 0, pending: 0, queued: 0 });
});

test("disconnect removes a queued request without starting work", async () => {
  const admission = createWorkerAdmission({
    maxWorkers: 1,
    maxPending: 1,
    maxQueued: 1,
    creationTimeoutMs: 10000,
  });
  const first = await admission.acquire();
  const client = new AbortController();
  const queued = admission.acquire(client.signal);
  client.abort(Error("client_disconnected"));
  await assert.rejects(queued, /client_disconnected/);
  assert.deepEqual(admission.status(), { workers: 1, pending: 1, queued: 0 });
  first.release();
  assert.deepEqual(admission.status(), { workers: 0, pending: 0, queued: 0 });
});

test("the creation deadline includes queue time and keeps partial workers charged until cleanup", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const admission = createWorkerAdmission({
    maxWorkers: 2,
    maxPending: 1,
    maxQueued: 1,
    creationTimeoutMs: 100,
  });
  const first = await admission.acquire();
  t.mock.timers.tick(40);
  const queued = admission.acquire();
  t.mock.timers.tick(50);
  first.ready();
  const second = await queued;
  t.mock.timers.tick(50);
  assert.equal(first.signal.aborted, false);
  assert.equal(second.signal.aborted, true);
  assert.throws(() => second.ready(), /browser_worker_creation_timeout/);
  assert.deepEqual(admission.status(), { workers: 2, pending: 1, queued: 0 });
  second.release();
  second.release();
  first.release();
  assert.deepEqual(admission.status(), { workers: 0, pending: 0, queued: 0 });
});

test("disconnect during creation aborts work and late readiness cannot publish", async () => {
  const admission = createWorkerAdmission({
    maxWorkers: 1,
    maxPending: 1,
    maxQueued: 0,
    creationTimeoutMs: 10000,
  });
  const client = new AbortController();
  const slot = await admission.acquire(client.signal);
  client.abort(Error("client_disconnected"));
  assert.equal(slot.signal.aborted, true);
  assert.throws(() => slot.ready(), /client_disconnected/);
  await assert.rejects(admission.acquire(), /browser_worker_queue_full/);
  slot.release();
  const next = await admission.acquire();
  next.ready();
  next.release();
});

test("expired queue entries are removed before capacity becomes available", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const admission = createWorkerAdmission({
    maxWorkers: 1,
    maxPending: 1,
    maxQueued: 1,
    creationTimeoutMs: 100,
  });
  const first = await admission.acquire();
  first.ready();
  const queued = admission.acquire();
  t.mock.timers.tick(100);
  await assert.rejects(queued, /browser_worker_creation_timeout/);
  assert.deepEqual(admission.status(), { workers: 1, pending: 0, queued: 0 });
  first.release();
});
