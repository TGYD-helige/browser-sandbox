import childProcess from "node:child_process";
import { readFile } from "node:fs/promises";
import { get } from "node:http";
import { syncBuiltinESMExports } from "node:module";
import { setTimeout as delay } from "node:timers/promises";

async function observeDebugger(child, executionId, started) {
  const deadline = performance.now() + 10000;
  const { debugPort } = JSON.parse(
    await readFile(`/browser-data/${executionId}/owner.json`, "utf8"),
  );
  while (
    child.exitCode === null &&
    child.signalCode === null &&
    performance.now() < deadline
  ) {
    const ready = await new Promise((resolve) => {
      const req = get(
        {
          hostname: "127.0.0.1",
          port: debugPort,
          path: "/json/version",
          agent: false,
          signal: AbortSignal.timeout(100),
        },
        (res) => {
          res.resume();
          resolve(res.statusCode === 200);
        },
      );
      req.on("error", () => resolve(false));
    });
    if (ready) {
      console.log(
        JSON.stringify({
          event: "test_native_debugger_observed",
          wrapperSpawnAt: performance.timeOrigin + started,
          wrapperSpawnToDebuggerMs: performance.now() - started,
        }),
      );
      return;
    }
    await delay(10);
  }
}

if (process.argv[2] === "worker") {
  const spawn = childProcess.spawn;
  childProcess.spawn = function (...args) {
    const executionId =
      Array.isArray(args[1]) &&
      args[1]
        .find((arg) => /^--browser-execution-id=[a-f0-9-]{36}$/.test(arg))
        ?.split("=")[1];
    const started = performance.now();
    const child = spawn.apply(this, args);
    if (executionId) void observeDebugger(child, executionId, started).catch(() => {});
    return child;
  };
  syncBuiltinESMExports();
}
