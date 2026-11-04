import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import test from "node:test";
import { createBrowserLimiter } from "../dist/limiter.js";

class Socket extends EventEmitter {
  destroy() {
    if (!this.destroyed) {
      this.destroyed = true;
      this.emit("close");
    }
  }
}
class Limiter {
  jobs = [];
  admission = Promise.resolve();
  get length() {
    return this.jobs.length;
  }
  push(job) {
    this.jobs.push(job);
    return this.length;
  }
  indexOf(job) {
    return this.jobs.indexOf(job);
  }
  splice(index, count) {
    this.jobs.splice(index, count);
  }
  limit(fn) {
    return (...args) =>
      new Promise((resolve) => {
        this.admission.then(() =>
          this.push(Object.assign(() => resolve(fn()), { args })),
        );
      });
  }
}
const req = { parsed: new URL("http://localhost/chromium?trackingId=test") };
const tick = () => new Promise(setImmediate);
test("a disconnected queued request releases capacity and settles without launching", async () => {
  const limiter = createBrowserLimiter(Limiter),
    socket = new Socket();
  const result = assert.rejects(
    limiter.limit(() => assert.fail("must not launch"))(req, socket),
    /browser_launch_cancelled/,
  );
  await tick();
  assert.equal(limiter.length, 1);
  socket.destroyed = true;
  socket.emit("close");
  await result;
  assert.equal(limiter.length, 0);
  assert.equal(socket.listenerCount("close"), 0);
});
test("started requests retain their original completion and cleanup", async () => {
  const limiter = createBrowserLimiter(Limiter),
    socket = new Socket(),
    gate = Promise.withResolvers();
  const result = limiter.limit(() => gate.promise)(req, socket);
  await tick();
  const job = limiter.jobs.shift();
  job();
  socket.destroyed = true;
  socket.emit("close");
  gate.resolve("completed");
  assert.equal(await result, "completed");
  assert.equal(socket.listenerCount("close"), 0);
});
test("disconnect during asynchronous admission cannot leave a late queued request", async () => {
  const limiter = createBrowserLimiter(Limiter),
    socket = new Socket(),
    gate = Promise.withResolvers();
  limiter.admission = gate.promise;
  const result = assert.rejects(
    limiter.limit(() => assert.fail("must not launch"))(req, socket),
    /browser_launch_cancelled/,
  );
  socket.destroyed = true;
  socket.emit("close");
  await result;
  gate.resolve();
  await tick();
  assert.equal(limiter.length, 0);
});

test("a TCP half-close releases queued capacity before the server socket closes", async () => {
  const limiter = createBrowserLimiter(Limiter),
    socket = new Socket();
  const result = assert.rejects(
    limiter.limit(() => assert.fail("must not launch"))(req, socket),
    /browser_launch_cancelled/,
  );
  await tick();
  socket.readableEnded = true;
  socket.emit("end");
  await result;
  assert.equal(socket.destroyed, true);
  assert.equal(limiter.length, 0);
  assert.equal(socket.listenerCount("end"), 0);
  assert.equal(socket.listenerCount("close"), 0);
});
