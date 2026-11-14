import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { promisify } from "node:util";
import { connectCdp } from "/project/dist/cdp.js";
import { diagnoseScreenshot } from "./screenshot-diagnostics.mjs";

const home = await mkdtemp(join(tmpdir(), "managed-harness-"));
const cli = join(
  dirname(
    createRequire("/project/.github/scripts/e2e/package.json").resolve(
      "agent-browser/package.json",
    ),
  ),
  "bin/agent-browser.js",
);
const session = `managed-${randomUUID()}`;
const call = async (...args) => {
  let output;
  try {
    output = (
      await promisify(execFile)(
        process.execPath,
        [
          cli,
          "--json",
          "--session",
          session,
          "--cdp",
          process.env.BROWSER_WS_ENDPOINT,
          ...args,
        ],
        {
          env: { ...process.env, AGENT_BROWSER_SOCKET_DIR: home },
          timeout: 30000,
          maxBuffer: 2 * 1024 * 1024,
        },
      )
    ).stdout;
  } catch {
    if (args[0] === "screenshot") {
      const report = await diagnoseScreenshot(
        process.env.BROWSER_WS_ENDPOINT,
        connectCdp,
      ).catch(() => ({ error: "browser_screenshot_diagnostic_failed" }));
      console.error("managed_screenshot_diagnostics " + JSON.stringify(report));
    }
    throw Error(`managed_harness_${args[0]}_failed`);
  }
  const result = JSON.parse(output);
  assert(result.success);
  return result.data;
};
try {
  await assert.rejects(
    fetch(`${process.env.BROWSER_WORKER_ORIGIN}/health`, {
      signal: AbortSignal.timeout(2000),
    }),
    "Agent reached private worker",
  );
  await assert.rejects(
    fetch("http://controller:8082/host", { signal: AbortSignal.timeout(2000) }),
    "Agent reached host-control listener",
  );
  await call("open", "data:text/html,<h1>Managed harness</h1>");
  await call("snapshot");
  const screenshot = join(home, "screenshot.png");
  await call("screenshot", screenshot);
  assert.equal(
    (await readFile(screenshot)).subarray(0, 8).toString("hex"),
    "89504e470d0a1a0a",
  );
  await call("eval", 'document.querySelector("h1").textContent');
  console.log(
    "agent-browser navigation, snapshot, evaluation, and screenshot passed through controller",
  );
} finally {
  await call("close").catch(() => {});
  await rm(home, { recursive: true, force: true });
}
