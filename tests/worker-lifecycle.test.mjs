import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import test from "node:test";
import { fileURLToPath } from "node:url";

const main = fileURLToPath(new URL("../dist/main.js", import.meta.url));
const fillStderr = "process.stderr.write('x'.repeat(4 * 1024 * 1024) + '\\n');";

function startFailingWorker(t) {
  const child = spawn(
    process.execPath,
    [
      "--import",
      `data:text/javascript,${encodeURIComponent(fillStderr)}`,
      main,
      "worker",
    ],
    {
      env: { ...process.env, BROWSER_WORKER_PROTOCOL: "invalid" },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  const exit = once(child, "exit");
  t.after(async () => {
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
    child.stderr.destroy();
    await exit;
  });
  return child;
}

test("worker startup flushes its failure record when a blocked stderr resumes", {
  skip: process.platform === "win32",
  timeout: 3000,
}, async (t) => {
  const child = startFailingWorker(t);
  const closed = once(child, "close");
  await once(child.stdout, "data", { signal: AbortSignal.timeout(2000) });
  let tail = "";
  child.stderr.on("data", (chunk) => {
    tail = (tail + chunk).slice(-512);
  });
  assert.deepEqual(await closed, [1, null]);
  assert.match(
    tail,
    /"event":"worker_start_failed","code":"browser_worker_protocol_unsupported"/,
  );
});

test("worker startup exits with status 1 even if stderr stays blocked", {
  skip: process.platform === "win32",
  timeout: 3000,
}, async (t) => {
  const child = startFailingWorker(t);
  assert.deepEqual(await once(child, "exit", { signal: AbortSignal.timeout(2000) }), [
    1,
    null,
  ]);
});
