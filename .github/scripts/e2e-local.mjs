import { execFile, spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { promisify } from "node:util";
import WebSocket, { WebSocketServer } from "ws";
import { createGateway } from "../../dist/gateway.js";

const executable = process.env.CHROME_EXECUTABLE;
if (!executable)
  throw Error("CHROME_EXECUTABLE is required for local client-contract checks");
const root = await mkdtemp(join(tmpdir(), "browser-local-e2e-"));
const gateway = await createGateway({
  WebSocket,
  WebSocketServer,
  port: 0,
  dataRoot: join(root, "data"),
  socketPath: join(root, "control.sock"),
  healthy: async () => true,
  async launch({ profileDir, signal }) {
    const child = spawn(
      executable,
      [
        "--headless=new",
        "--remote-debugging-port=0",
        `--user-data-dir=${profileDir}`,
        "--no-first-run",
        "--no-default-browser-check",
        "about:blank",
      ],
      { stdio: "ignore" },
    );
    const exited = once(child, "exit");
    const abort = () => child.kill("SIGTERM");
    signal.addEventListener("abort", abort, { once: true });
    try {
      let endpoint;
      for (let i = 0; i < 200; i++) {
        signal.throwIfAborted();
        if (child.exitCode !== null) throw Error("Local Chrome exited");
        try {
          const [port, path] = (
            await readFile(join(profileDir, "DevToolsActivePort"), "utf8")
          )
            .trim()
            .split("\n");
          endpoint = `ws://127.0.0.1:${port}${path}`;
          break;
        } catch {
          await delay(50);
        }
      }
      if (!endpoint) throw Error("Local Chrome startup timed out");
      return {
        endpoint,
        closed: exited,
        async close() {
          child.kill("SIGTERM");
          await exited;
          signal.removeEventListener("abort", abort);
        },
      };
    } catch (error) {
      child.kill("SIGTERM");
      await exited;
      throw error;
    }
  },
});
try {
  for (const harness of process.argv.slice(2).length
    ? process.argv.slice(2)
    : ["pi", "dsh", "opencode"]) {
    if (!["pi", "dsh", "opencode"].includes(harness))
      throw Error("Choose pi, dsh, or opencode");
    const executionId = randomUUID();
    const { wsEndpoint } = await gateway.command({ op: "register", executionId });
    try {
      const work = promisify(execFile)(
        process.execPath,
        [`.github/scripts/e2e/${harness}.mjs`],
        {
          env: { ...process.env, BROWSER_WS_ENDPOINT: wsEndpoint },
          timeout: 360000,
          maxBuffer: 8 * 1024 * 1024,
        },
      );
      work.child.stdin.end();
      const result = await work;
      process.stdout.write(result.stdout);
    } finally {
      await gateway.command({ op: "finalize", executionId }, true);
    }
  }
} catch (error) {
  const diagnostic = error.stderr
    ?.split("\n")
    .find((line) =>
      /^E2E_FAILURE [A-Za-z]+ at (\.github\/scripts\/e2e\/(pi|dsh|opencode|session-mcp|fixture)\.mjs:\d+:\d+|unknown)$/.test(
        line,
      ),
    );
  if (diagnostic) console.error(diagnostic);

  console.error(
    `Local harness E2E failed (exit ${error.code ?? "unknown"}); raw subprocess output withheld`,
  );
  process.exitCode = 1;
} finally {
  await gateway.stop();
  await rm(root, { recursive: true, force: true });
}
