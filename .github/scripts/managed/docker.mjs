import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdir, writeFile } from "node:fs/promises";
import { setTimeout as delay } from "node:timers/promises";
import { promisify } from "node:util";

const exec = promisify(execFile);
const docker = async (...args) =>
  (
    await exec("docker", args, { timeout: 180000, maxBuffer: 4 * 1024 * 1024 })
  ).stdout.trim();
const installation = `test-${randomUUID().slice(0, 8)}`;
const image = process.env.BROWSER_IMAGE ?? "browser-sandbox:managed";
let controller, network;
await mkdir("managed-results", { recursive: true });
try {
  network = await docker("network", "create", `agents-${installation}`);
  controller = await docker(
    "run",
    "-d",
    "--name",
    `controller-${installation}`,
    "--network",
    network,
    "--network-alias",
    "controller",
    "--cap-drop",
    "ALL",
    "--security-opt",
    "no-new-privileges",
    "--mount",
    "type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock",
    "-e",
    `BROWSER_INSTALLATION=${installation}`,
    "-e",
    "BROWSER_CONTROLLER_ORIGIN=http://controller:8080",
    "-e",
    "TIMEOUT=180000",
    image,
    "controller",
  );
  const deadline = Date.now() + 30000;
  while (true) {
    try {
      await docker(
        "exec",
        controller,
        "node",
        "-e",
        "fetch('http://127.0.0.1:8080/health').then(r=>{if(!r.ok)process.exit(1)})",
      );
      break;
    } catch (error) {
      if (Date.now() > deadline) throw error;
      await delay(100);
    }
  }
  await docker(
    "cp",
    ".github/scripts/managed/client.mjs",
    `${controller}:/opt/browser-sandbox/managed-client.mjs`,
  );
  const output = await docker(
    "exec",
    controller,
    "node",
    "/opt/browser-sandbox/managed-client.mjs",
  );
  await writeFile("managed-results/docker.json", output + "\n");
  console.log(output);
  assert.equal(
    await docker(
      "ps",
      "-aq",
      "--filter",
      `label=browser-sandbox.installation=${installation}`,
    ),
    "",
    "Finalization left a worker container",
  );
  assert.equal(
    await docker(
      "network",
      "ls",
      "-q",
      "--filter",
      `label=browser-sandbox.installation=${installation}`,
    ),
    "",
    "Finalization left a worker network",
  );
} catch (error) {
  if (controller) {
    const logs = await docker("logs", controller).catch(() => "");
    await writeFile(
      "managed-results/diagnostics.txt",
      [
        ...new Set(
          logs.match(/\bbrowser_[a-z_]+\b|\b(?:EPERM|ENOENT|EACCES|ENOSPC)\b/g) ?? [],
        ),
      ].join("\n"),
    );
  }
  throw error;
} finally {
  for (const id of (
    await docker(
      "ps",
      "-aq",
      "--filter",
      `label=browser-sandbox.installation=${installation}`,
    ).catch(() => "")
  )
    .split("\n")
    .filter(Boolean))
    await docker("rm", "-f", "-v", id).catch(() => {});
  if (controller) await docker("rm", "-f", controller).catch(() => {});
  for (const id of (
    await docker(
      "network",
      "ls",
      "-q",
      "--filter",
      `label=browser-sandbox.installation=${installation}`,
    ).catch(() => "")
  )
    .split("\n")
    .filter(Boolean))
    await docker("network", "rm", id).catch(() => {});
  if (network) await docker("network", "rm", network).catch(() => {});
}
