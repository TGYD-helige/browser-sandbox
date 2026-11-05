import assert from "node:assert/strict";
import test from "node:test";
import { createAutomationControl } from "../dist/automation-control.js";

const entry = () => ({
  executionId: "execution",
  peers: new Set(),
  session: { status: () => ({ generation: "first" }) },
});
test("a timed-out transition remains locked until the underlying callback settles", async () => {
  let settle, entered;
  const started = new Promise((resolve) => {
    entered = resolve;
  });
  const delayed = new Promise((resolve) => {
    settle = resolve;
  });
  const control = createAutomationControl({
    async pause() {},
    async resume() {
      entered();
      await delayed;
    },
  });
  const execution = entry();
  const abort = new AbortController();
  const work = control.run(execution, abort.signal, () =>
    control.resume(execution, abort.signal),
  );
  await started;
  abort.abort();
  await assert.rejects(work, /control_timeout/);
  assert.throws(
    () => control.run(execution, new AbortController().signal, async () => {}),
    /control_busy/,
  );
  settle();
  await new Promise((resolve) => setImmediate(resolve));
  await control.run(execution, new AbortController().signal, async () => {});
});

test("host stop confirmation belongs to one browser generation and is consumed once", async () => {
  const control = createAutomationControl();
  const execution = entry();
  const signal = new AbortController().signal;
  control.confirm(execution);
  await control.pause(execution, signal);
  await assert.rejects(control.pause(execution, signal), /stop_required/);
  control.confirm(execution);
  execution.session.status = () => ({ generation: "replacement" });
  assert.equal(control.canPause(execution), false);
  await assert.rejects(control.pause(execution, signal), /stop_required/);
});
