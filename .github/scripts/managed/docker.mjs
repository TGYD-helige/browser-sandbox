import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { mkdir, writeFile } from "node:fs/promises";
import { setTimeout as delay } from "node:timers/promises";

import { docker as command, diagnostics } from "./command.mjs";

const docker = (...args) => command(args);
const installation = `test-${randomUUID().slice(0, 8)}`;
const image = process.env.BROWSER_IMAGE ?? "browser-sandbox:managed";
let controller, network, volume, hostStorage, noStoreController;
await mkdir("managed-results", { recursive: true });
try {
  network = await docker("network", "create", `agents-${installation}`);
  volume = await docker("volume", "create", `profiles-${installation}`);
  hostStorage = spawn(process.execPath, [".github/scripts/managed/host-storage.mjs"], {
    stdio: ["ignore", "pipe", "pipe"],
  });
  await Promise.race([
    once(hostStorage.stdout, "data").then(([data]) =>
      assert.equal(String(data).trim(), "ready"),
    ),
    once(hostStorage, "exit").then(() => {
      throw Error("Host archive fixture exited before readiness");
    }),
  ]);
  const host =
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
    `type=volume,src=${volume},dst=/profile-store`,
    "-e",
    "BROWSER_PROFILE_STORE_ROOT=/profile-store/profiles",
    "-e",
    `BROWSER_STORAGE_EGRESS=${JSON.stringify([{ address: host, port: 9083 }])}`,
    "-e",
    `BROWSER_PROFILE_STORE_ORIGIN=http://controller-${installation}:8083`,
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
  const state = JSON.parse(
    await docker("exec", controller, "cat", "/tmp/managed-state.json"),
  );
  const controllerProcesses = await docker("top", controller, "-eo", "pid,args");
  assert(
    !controllerProcesses.includes("/opt/google/chrome/chrome"),
    "Controller started Chrome",
  );
  const worker = JSON.parse(await docker("inspect", state.workerId))[0];
  assert(
    Object.keys(worker.HostConfig.PortBindings ?? {}).length === 0,
    "Worker published ports",
  );
  assert(
    !worker.Mounts.some((mount) => mount.Destination.includes("docker.sock")),
    "Worker received Docker credentials",
  );
  const agent = await docker(
    "create",
    "--network",
    network,
    "--cap-drop",
    "ALL",
    "--security-opt",
    "no-new-privileges",
    "-e",
    `BROWSER_WS_ENDPOINT=${state.endpoint}`,
    "-e",
    `BROWSER_WORKER_ORIGIN=http://${Object.values(worker.NetworkSettings.Networks)[0].IPAddress}:8081`,
    "browser-sandbox-managed-harness:local",
    "/project/managed-harness.mjs",
  );
  try {
    await docker(
      "cp",
      ".github/scripts/managed/harness.mjs",
      `${agent}:/project/managed-harness.mjs`,
    );
    await docker(
      "cp",
      ".github/scripts/managed/screenshot-diagnostics.mjs",
      `${agent}:/project/screenshot-diagnostics.mjs`,
    );
    await docker(
      "cp",
      ".github/scripts/managed/screenshot-tab.mjs",
      `${agent}:/project/screenshot-tab.mjs`,
    );
    const output = await docker("start", "-a", agent);
    assert.equal(
      JSON.parse(await docker("inspect", agent))[0].State.ExitCode,
      0,
      "Harness failed",
    );
    console.log(output);
  } finally {
    await docker("rm", "-f", agent);
  }
  await docker(
    "exec",
    controller,
    "node",
    "/opt/browser-sandbox/managed-client.mjs",
    "cleanup",
  );
  await writeFile(
    "managed-results/docker.json",
    JSON.stringify(state.report, null, 2) + "\n",
  );
  await docker(
    "cp",
    ".github/scripts/managed/profiles-client.mjs",
    `${controller}:/opt/browser-sandbox/profiles-client.mjs`,
  );
  noStoreController = await docker(
    "run",
    "-d",
    "--name",
    `no-store-${installation}`,
    "--network",
    network,
    "--cap-drop",
    "ALL",
    "--security-opt",
    "no-new-privileges",
    "--mount",
    "type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock",
    "-e",
    `BROWSER_INSTALLATION=${installation}-no-store`,
    "-e",
    `BROWSER_CONTROLLER_ORIGIN=http://no-store-${installation}:8080`,
    image,
    "controller",
  );
  const noStoreDeadline = Date.now() + 30000;
  while (true) {
    try {
      await docker(
        "exec",
        noStoreController,
        "node",
        "-e",
        "Promise.all([fetch('http://127.0.0.1:8080/health').then(r=>{if(!r.ok)throw Error()}),new Promise((resolve,reject)=>{const s=require('node:net').connect(8082,'127.0.0.1');s.once('connect',()=>{s.end();resolve()});s.once('error',reject)})]).catch(()=>process.exit(1))",
      );
      break;
    } catch (error) {
      if (Date.now() > noStoreDeadline) throw error;
      await delay(100);
    }
  }
  await docker(
    "cp",
    ".github/scripts/managed/profiles-client.mjs",
    `${noStoreController}:/opt/browser-sandbox/profiles-client.mjs`,
  );
  const missingStorage = JSON.parse(
    await docker(
      "exec",
      "-e",
      "BROWSER_PROFILE_PHASE=missing-storage",
      "-e",
      "BROWSER_TEST_CONTROL_ORIGIN=http://127.0.0.1:8082",
      noStoreController,
      "node",
      "/opt/browser-sandbox/profiles-client.mjs",
    ),
  );
  assert.equal(missingStorage.missingStorageRejected, true);
  assert.equal(
    await docker(
      "ps",
      "-aq",
      "--filter",
      `label=browser-sandbox.installation=${installation}-no-store`,
    ),
    "",
  );
  const profiles = await docker(
    "exec",
    "-e",
    `BROWSER_HOST_ARCHIVE_URL=http://${host}:9083/archive`,
    controller,
    "node",
    "/opt/browser-sandbox/profiles-client.mjs",
  );
  await writeFile(
    "managed-results/profiles-docker.json",
    JSON.stringify({ ...JSON.parse(profiles), ...missingStorage }) + "\n",
  );
  console.log(profiles);
  await docker(
    "exec",
    "-e",
    "BROWSER_PROFILE_PHASE=restart-seed",
    controller,
    "node",
    "/opt/browser-sandbox/profiles-client.mjs",
  );
  const held = JSON.parse(
    await docker(
      "exec",
      "-e",
      "BROWSER_PROFILE_PHASE=lease-hold",
      controller,
      "node",
      "/opt/browser-sandbox/profiles-client.mjs",
    ),
  );
  await docker("kill", "--signal=KILL", controller);
  await docker("start", controller);
  const restartedDeadline = Date.now() + 30000;
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
      if (Date.now() > restartedDeadline) throw error;
      await delay(100);
    }
  }
  const restored = await docker(
    "exec",
    "-e",
    "BROWSER_PROFILE_PHASE=restart-restore",
    controller,
    "node",
    "/opt/browser-sandbox/profiles-client.mjs",
  );
  console.log(restored);
  console.log(
    await docker(
      "exec",
      "-e",
      "BROWSER_PROFILE_PHASE=lease-check",
      controller,
      "node",
      "/opt/browser-sandbox/profiles-client.mjs",
    ),
  );
  const heldId = await docker(
    "ps",
    "-aq",
    "--filter",
    `label=browser-sandbox.execution=${held.executionId}`,
  );
  assert(heldId, "Restart lost the leased worker");
  const heldNetwork = JSON.parse(await docker("inspect", heldId))[0].Config.Labels[
    "browser-sandbox.network"
  ];
  await docker("stop", "--timeout", "5", heldId);
  await docker("rm", "-v", heldId);
  await docker("network", "disconnect", "-f", heldNetwork, controller);
  await docker("network", "rm", heldNetwork);
  const profilePhase = (phase, executionId) =>
    docker(
      "exec",
      "-e",
      `BROWSER_PROFILE_PHASE=${phase}`,
      ...(executionId ? ["-e", `BROWSER_TEST_EXECUTION_ID=${executionId}`] : []),
      controller,
      "node",
      "/opt/browser-sandbox/profiles-client.mjs",
    );
  await profilePhase("lease-reuse");
  await profilePhase("interrupt-seed");
  const digestCode =
    "const fs=require('node:fs'),crypto=require('node:crypto');const index=JSON.parse(fs.readFileSync('/profile-store/profiles/index.json'));process.stdout.write(index.profiles[crypto.createHash('sha256').update('managed-upload-interrupt').digest('hex')].archive.sha256)";
  const confirmed = await docker("exec", controller, "node", "-e", digestCode);
  const interrupted = JSON.parse(await profilePhase("interrupt-hold"));
  const interruptedId = await docker(
    "ps",
    "-aq",
    "--filter",
    `label=browser-sandbox.execution=${interrupted.executionId}`,
  );
  assert(interruptedId);
  const interruptedNetwork = JSON.parse(await docker("inspect", interruptedId))[0]
    .Config.Labels["browser-sandbox.network"];
  // Permit initial packets, then reset the real upload after its body starts; no timing sleeps.
  await docker(
    "exec",
    interruptedId,
    "iptables",
    "-w",
    "-I",
    "OUTPUT",
    "-p",
    "tcp",
    "--dport",
    "8083",
    "-m",
    "owner",
    "--uid-owner",
    "0",
    "-m",
    "statistic",
    "--mode",
    "nth",
    "--every",
    "8",
    "--packet",
    "7",
    "-j",
    "REJECT",
    "--reject-with",
    "tcp-reset",
  );
  await profilePhase("interrupt-finish", interrupted.executionId);
  assert.equal(await docker("exec", controller, "node", "-e", digestCode), confirmed);
  // Simulate loss of this test-owned failed worker; the confirmed archive is the recovery source.
  await docker("rm", "-f", "-v", interruptedId);
  await docker("network", "disconnect", "-f", interruptedNetwork, controller);
  await docker("network", "rm", interruptedNetwork);
  await docker("kill", "--signal=KILL", controller);
  await docker("start", controller);
  const recoveryDeadline = Date.now() + 30000;
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
      if (Date.now() > recoveryDeadline) throw error;
      await delay(100);
    }
  }
  console.log(await profilePhase("interrupt-restore"));
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
    const records = [];
    const owned = (
      await docker(
        "ps",
        "-aq",
        "--filter",
        `label=browser-sandbox.installation=${installation}`,
      ).catch(() => "")
    )
      .split("\n")
      .filter(Boolean);
    for (const [kind, id] of [
      ["controller", controller],
      ...owned.map((id) => ["worker", id]),
    ]) {
      const details = JSON.parse(await docker("inspect", id).catch(() => "[]"))[0];
      const logs = await docker("logs", id).catch(() => "");
      records.push({ kind, ...diagnostics(logs, details?.State) });
    }
    await writeFile(
      "managed-results/diagnostics.json",
      JSON.stringify(records, null, 2) + "\n",
    );
  }
  throw error;
} finally {
  for (const owner of [installation, `${installation}-no-store`])
    for (const id of (
      await docker(
        "ps",
        "-aq",
        "--filter",
        `label=browser-sandbox.installation=${owner}`,
      ).catch(() => "")
    )
      .split("\n")
      .filter(Boolean))
      await docker("rm", "-f", "-v", id).catch(() => {});
  if (noStoreController) await docker("rm", "-f", noStoreController).catch(() => {});
  if (controller) await docker("rm", "-f", controller).catch(() => {});
  if (volume) await docker("volume", "rm", volume).catch(() => {});
  if (hostStorage && hostStorage.exitCode === null) {
    const stopped = once(hostStorage, "exit");
    hostStorage.kill("SIGTERM");
    await stopped;
  }
  for (const owner of [installation, `${installation}-no-store`])
    for (const id of (
      await docker(
        "network",
        "ls",
        "-q",
        "--filter",
        `label=browser-sandbox.installation=${owner}`,
      ).catch(() => "")
    )
      .split("\n")
      .filter(Boolean))
      await docker("network", "rm", id).catch(() => {});
  if (network) await docker("network", "rm", network).catch(() => {});
}
