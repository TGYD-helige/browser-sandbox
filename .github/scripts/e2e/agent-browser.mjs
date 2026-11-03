import "./diagnostics.mjs";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm, stat } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { promisify } from "node:util";
import { sessionRequest } from "../../../dist/session.js";
import { fixture, inspect, prepareFiles } from "./fixture.mjs";

const home = await mkdtemp(join(tmpdir(), "browser-cli-"));
const cli = join(
  dirname(createRequire(import.meta.url).resolve("agent-browser/package.json")),
  "bin/agent-browser.js",
);
const endpoint = process.env.BROWSER_WS_ENDPOINT;
const session = `e2e-${randomUUID().slice(0, 8)}`;
const site = await fixture();
const files = await prepareFiles(site, endpoint);
const call = async (...args) => {
  const work = promisify(execFile)(
    process.execPath,
    [cli, "--json", "--session", session, "--cdp", endpoint, ...args],
    {
      env: { ...process.env, AGENT_BROWSER_SOCKET_DIR: home },
      timeout: 30000,
      maxBuffer: 2 * 1024 * 1024,
    },
  );
  work.child.stdin.end();
  let output;
  try {
    output = (await work).stdout;
  } catch {
    console.error(`E2E_TOOL_FAILURE files browser_cli_${args[0]} unclassified`);
    throw Error(`agent_browser_${args[0]}_failed`);
  }
  const result = JSON.parse(output);
  assert(result.success, `agent-browser ${args[0]} failed`);
  return result.data;
};
try {
  await call("open", site.url);
  const info = await call("session", "info");
  assert(Number.isInteger(info.pid) && info.active);
  const before = await call("eval", `(${inspect})()`);
  await call("snapshot");
  const screenshot = join(home, "page.png");
  await call("screenshot", screenshot);
  assert((await stat(screenshot)).size > 0);
  await call("upload", "input[type=file]", files.path);
  await call("click", "#download");
  await files.verify();
  const after = await call("eval", `(${inspect})()`);
  assert.deepEqual(after.result.marker, before.result.marker);
  assert(after.result.ticks >= before.result.ticks);
  const challenge = await call("get", "text", "#challenge");
  await call("fill", "#answer", challenge.text);
  await call("click", "button");
  await site.verify();
  assert.equal((await call("session", "info")).pid, info.pid);
  await call("close");
  const alive = (pid) => {
    try {
      process.kill(pid, 0);
      return true;
    } catch (error) {
      if (error.code === "ESRCH") return false;
      throw error;
    }
  };
  for (let i = 0; i < 100 && alive(info.pid); i++) await delay(50);
  assert(!alive(info.pid), "agent-browser daemon did not stop");
  await call("open", site.url);
  const restarted = await call("session", "info");
  assert.notEqual(restarted.pid, info.pid);
  await call("close");
  for (let i = 0; i < 100 && alive(restarted.pid); i++) await delay(50);
  assert(!alive(restarted.pid), "restarted agent-browser daemon did not stop");
  for (let i = 0; i < 100 && (await sessionRequest(endpoint)).connectedClients; i++)
    await delay(50);
  assert.equal((await sessionRequest(endpoint)).connectedClients, 0);
  console.log(
    "agent-browser tools: real CLI/daemon lifecycle, screenshot, upload/download, and form submission passed",
  );
} finally {
  await call("close").catch(() => {});
  await sessionRequest(endpoint, "stop");
  await site.close();
  await rm(home, { recursive: true, force: true });
}
