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
const installation = `recovery-${randomUUID().slice(0, 8)}`;
const image = process.env.BROWSER_IMAGE ?? "browser-sandbox:managed";
let controller, network, volume;
const workerIds = () =>
  docker("ps", "-aq", "--filter", `label=browser-sandbox.installation=${installation}`);
const ready = async () => {
  const deadline = Date.now() + 45000;
  while (true) {
    try {
      await docker(
        "exec",
        controller,
        "node",
        "-e",
        "fetch('http://127.0.0.1:8080/health').then(r=>{if(!r.ok)process.exit(1)})",
      );
      return;
    } catch (error) {
      if (Date.now() >= deadline) throw error;
      await delay(100);
    }
  }
};
await mkdir("managed-results", { recursive: true });
try {
  network = await docker("network", "create", `agents-${installation}`);
  volume = await docker("volume", "create", `state-${installation}`);
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
    "--mount",
    `type=volume,src=${volume},dst=/controller-data`,
    "-e",
    `BROWSER_INSTALLATION=${installation}`,
    "-e",
    "BROWSER_CONTROLLER_ORIGIN=http://controller:8080",
    "-e",
    "BROWSER_IDLE_REGISTRATION_MS=1000",
    "-e",
    "TIMEOUT=180000",
    image,
    "controller",
  );
  await ready();
  await docker(
    "cp",
    ".github/scripts/managed/recovery-client.mjs",
    `${controller}:/opt/browser-sandbox/recovery-client.mjs`,
  );
  const run = async (phase) =>
    JSON.parse(
      await docker(
        "exec",
        controller,
        "node",
        "/opt/browser-sandbox/recovery-client.mjs",
        phase,
      ),
    );
  const results = [await run("prepare")];
  const original = await workerIds();
  assert(original && !original.includes("\n"));
  await docker("stop", "-t", "30", controller);
  assert.equal(
    await workerIds(),
    original,
    "Docker worker must survive controller downtime; Docker supplies no independent container lifetime limit",
  );
  assert.equal(
    await docker("inspect", "--format", "{{.State.Running}}", original),
    "true",
  );
  await docker("start", controller);
  await ready();
  assert.equal(await workerIds(), original);
  results.push(await run("resume"));
  assert.equal(await workerIds(), "");
  await docker("restart", "-t", "30", controller);
  await ready();
  results.push(await run("replay"));
  await writeFile(
    "managed-results/recovery-docker.json",
    JSON.stringify(results, null, 2) + "\n",
  );
  console.log(JSON.stringify(results));
} catch (error) {
  const logs = controller ? await docker("logs", controller).catch(() => "") : "";
  await writeFile(
    "managed-results/recovery-errors.txt",
    [
      ...new Set(
        logs.match(/\bbrowser_[a-z_]+\b|\b(?:EPERM|ENOENT|EACCES|ENOSPC)\b/g) ?? [],
      ),
    ].join("\n"),
  );
  throw error;
} finally {
  for (const id of (await workerIds().catch(() => "")).split("\n").filter(Boolean))
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
  if (volume) await docker("volume", "rm", volume).catch(() => {});
}
