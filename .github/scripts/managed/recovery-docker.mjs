import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { promisify } from "node:util";
import { archiveFixture } from "../benchmark-storage.mjs";

const exec = promisify(execFile);
const docker = async (...args) =>
  (
    await exec("docker", args, { timeout: 240000, maxBuffer: 4 * 1024 * 1024 })
  ).stdout.trim();
const installation = `recovery-${randomUUID().slice(0, 8)}`;
const image = process.env.BROWSER_IMAGE ?? "browser-sandbox:managed";
const nextImage = `browser-sandbox:protocol-${installation}`;
const controllerName = `controller-${installation}`;
let controller, network, volume, archive, archiveDirectory, archiveUrl;
const results = [];
const workerIds = () =>
  docker("ps", "-aq", "--filter", `label=browser-sandbox.installation=${installation}`);
const wait = async (description, read, predicate, timeout = 45000) => {
  const deadline = Date.now() + timeout;
  while (true) {
    const value = await read();
    if (predicate(value)) return value;
    assert(Date.now() < deadline, description);
    await delay(25);
  }
};
const ready = () =>
  wait(
    "Controller did not become ready",
    () =>
      docker(
        "exec",
        controller,
        "node",
        "-e",
        "fetch('http://127.0.0.1:8080/health').then(r=>{if(!r.ok)process.exit(1)})",
      ).then(
        () => true,
        () => false,
      ),
    Boolean,
  );
const create = async (candidate = image, store = true, workerImage) => {
  controller = await docker(
    "run",
    "-d",
    "--name",
    controllerName,
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
    "BROWSER_IDLE_REGISTRATION_MS=500",
    "-e",
    "BROWSER_MAX_EXECUTION_MS=45000",
    "-e",
    "BROWSER_MAX_WORKERS=1",
    "-e",
    "BROWSER_MAX_PENDING=1",
    "-e",
    "BROWSER_MAX_QUEUED=1",
    "-e",
    "TIMEOUT=180000",
    "-e",
    `BROWSER_STORAGE_EGRESS=${JSON.stringify([{ address: archiveUrl.hostname, port: Number(archiveUrl.port) }])}`,
    ...(store
      ? [
          "-e",
          "BROWSER_PROFILE_STORE_ROOT=/controller-data/profiles",
          "-e",
          `BROWSER_PROFILE_STORE_ORIGIN=http://${controllerName}:8083`,
        ]
      : []),
    ...(workerImage ? ["-e", `BROWSER_IMAGE=${workerImage}`] : []),
    candidate,
    "controller",
  );
  await ready();
  for (const name of ["recovery-client.mjs", "recovery-scenarios.mjs"])
    await docker(
      "cp",
      `.github/scripts/managed/${name}`,
      `${controller}:/opt/browser-sandbox/${name}`,
    );
};
const replace = async (candidate = image, store = true, workerImage) => {
  await docker("stop", "-t", "30", controller);
  await docker("rm", controller);
  controller = undefined;
  await create(candidate, store, workerImage);
};
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
const scenarios = async (phase) =>
  JSON.parse(
    await docker(
      "exec",
      "-e",
      `TEST_RECOVERY_PHASE=${phase}`,
      "-e",
      `BROWSER_HOST_ARCHIVE_URL=${archiveUrl}`,
      controller,
      "node",
      "/opt/browser-sandbox/recovery-scenarios.mjs",
    ),
  );
await mkdir("managed-results", { recursive: true });
try {
  await docker(
    "build",
    "--build-arg",
    `BROWSER_IMAGE=${image}`,
    "-f",
    ".github/scripts/managed/Dockerfile.protocol",
    "-t",
    nextImage,
    ".",
  );
  network = await docker("network", "create", `agents-${installation}`);
  volume = await docker("volume", "create", `state-${installation}`);
  archiveDirectory = await mkdtemp(join(tmpdir(), "recovery-archive-"));
  archive = await archiveFixture(archiveDirectory, { host: "0.0.0.0" });
  archiveUrl = new URL(archive.url);
  archiveUrl.hostname =
    process.platform === "darwin"
      ? await docker(
          "run",
          "--rm",
          "--network",
          network,
          "--entrypoint",
          "node",
          image,
          "-e",
          "require('node:dns').lookup('host.docker.internal',(error,result)=>{if(error)process.exit(1);console.log(result)})",
        )
      : JSON.parse(await docker("network", "inspect", "bridge"))[0].IPAM.Config[0]
          .Gateway;
  await create();
  results.push(
    await scenarios("core"),
    await scenarios("ttl-volume"),
    await run("prepare"),
  );
  const original = await workerIds();
  assert(original && !original.includes("\n"));
  const rule = [
    "INPUT",
    "-p",
    "tcp",
    "--dport",
    "8081",
    "-j",
    "REJECT",
    "--reject-with",
    "tcp-reset",
  ];
  await docker("exec", original, "iptables", "-I", ...rule);
  try {
    results.push(await run("unknown"));
    assert.equal(await workerIds(), original);
  } finally {
    await docker("exec", original, "iptables", "-D", ...rule);
  }
  await docker("stop", "-t", "30", controller);
  assert.equal(
    await workerIds(),
    original,
    "Docker has no independent worker container lifetime limit",
  );
  assert.equal(
    await docker("inspect", "--format", "{{.State.Running}}", original),
    "true",
  );
  await docker("start", controller);
  await ready();
  // A real image with a synthetic protocol identity must reject old workers without deleting them.
  await replace(nextImage);
  results.push(await run("incompatible"));
  assert.equal(await workerIds(), original);
  await replace(image);
  results.push(await run("resume"));
  assert.equal(await workerIds(), "");
  await docker("restart", "-t", "30", controller);
  await ready();
  results.push(await run("replay"));
  results.push(await run("prepare"));
  const lostWorker = await workerIds();
  await docker("kill", lostWorker);
  results.push(await run("lost"));
  const survivingWorker = await workerIds();
  assert(
    survivingWorker === "" || survivingWorker === lostWorker,
    "Loss silently allocated a replacement worker",
  );
  results.push(await run("finish-lost"));
  assert.equal(await workerIds(), "");
  await replace(image, true, `sha256:${"f".repeat(64)}`);
  results.push(await run("image-failure"));
  assert.equal(await workerIds(), "");
  await replace(image, false);
  results.push(await scenarios("ttl-host"));
  assert.equal(await workerIds(), "");
  await writeFile(
    "managed-results/recovery-docker.json",
    `${JSON.stringify(results, null, 2)}\n`,
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
  await archive?.close();
  if (archiveDirectory) await rm(archiveDirectory, { recursive: true, force: true });
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
  await docker("image", "rm", nextImage).catch(() => {});
}
