import assert from "node:assert/strict";
import { fork } from "node:child_process";
import { once } from "node:events";
import { createServer } from "node:http";
import { test } from "node:test";

test("benchmark worker reports a fixed connection phase without leaking the endpoint", async (t) => {
  const server = createServer((_req, res) => res.writeHead(503).end());
  t.after(() => new Promise((resolve) => server.close(resolve)));
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const child = fork(new URL("./benchmark-image.mjs", import.meta.url), ["worker"], {
    env: {
      ...process.env,
      BROWSER_WS_ENDPOINT: `ws://127.0.0.1:${server.address().port}/private?token=secret`,
    },
    stdio: ["ignore", "ignore", "ignore", "ipc"],
  });
  t.after(async () => {
    if (child.exitCode === null && child.signalCode === null) {
      const exited = once(child, "exit");
      child.kill("SIGKILL");
      await exited;
    }
  });
  const response = once(child, "message", { signal: AbortSignal.timeout(10000) });
  child.send({ action: "ready", url: "http://fixture.invalid" });
  const [message] = await response;
  assert.deepEqual(message, {
    error: "browser_benchmark_worker_failed",
    phase: "connect",
    reason: "browser_cdp_connect_failed",
  });
  const exited = once(child, "exit");
  child.disconnect();
  await exited;
});
