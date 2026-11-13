import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { promisify } from "node:util";

const exec = promisify(execFile);
const run = (args, input) => {
  const work = exec("docker", args, { timeout: 360000, maxBuffer: 8 * 1024 * 1024 });
  work.child.stdin.end(input);
  return work;
};
// A comma-separated list runs each harness as its own execution and UID against one runtime.
const harnesses = (process.argv[2] ?? "").split(",");
assert(
  harnesses.every((name) =>
    ["pi", "dsh", "opencode", "agent-browser"].includes(name),
  ) ||
    (harnesses.length === 1 && harnesses[0] === "unused"),
  "Choose pi, dsh, opencode, agent-browser, a comma-separated list of them, or unused",
);
assert.equal(new Set(harnesses).size, harnesses.length, "List each harness once");
// About 1 GiB per browser, matching the default two browsers in 2 GiB.
const browsers = Math.max(2, harnesses.length);
const mode = process.env.E2E_MODE ?? "tools";
assert(["tools", "model"].includes(mode), "Choose tools or model");
if (mode === "model")
  for (const key of ["OPENAI_BASE_URL", "OPENAI_API_KEY", "OPENAI_MODEL_NAME"])
    assert(process.env[key], `${key} is required`);
const root = await mkdtemp(join(tmpdir(), "browser-e2e-"));
const image = process.env.BROWSER_IMAGE ?? "browser-sandbox:local";
let container, extraction, filesVolume;
const control = async (request) =>
  JSON.parse(
    (
      await run(
        [
          "exec",
          "-i",
          container,
          "node",
          "--input-type=module",
          "-e",
          "import {hostRequest} from '/opt/browser-sandbox/dist/index.js'; let s=''; for await(const c of process.stdin)s+=c; console.log(JSON.stringify(await hostRequest(process.env.BROWSER_CONTROL_SOCKET,JSON.parse(s))));",
        ],
        JSON.stringify(request),
      )
    ).stdout,
  );
try {
  filesVolume = (await run(["volume", "create"])).stdout.trim();
  extraction = (await run(["create", image])).stdout.trim();
  await run([
    "cp",
    `${extraction}:/opt/browser-sandbox/seccomp.json`,
    join(root, "seccomp.json"),
  ]);
  container = (
    await run([
      "run",
      "-d",
      "--init",
      "--sysctl",
      "net.ipv4.ip_local_reserved_ports=3001,42000-42999",
      "--cap-drop",
      "ALL",
      ...[
        "SETUID",
        "SETGID",
        "CHOWN",
        "KILL",
        "DAC_OVERRIDE",
        "SYS_CHROOT",
        "NET_ADMIN",
      ].flatMap((cap) => ["--cap-add", cap]),
      "--security-opt",
      "no-new-privileges",
      "--security-opt",
      `seccomp=${join(root, "seccomp.json")}`,
      "--shm-size",
      "512m",
      "--memory",
      `${browsers}g`,
      "-e",
      `CONCURRENT=${browsers}`,
      "--mount",
      `type=volume,src=${filesVolume},dst=/browser-data`,
      "--mount",
      `type=volume,src=${filesVolume},dst=/tmp/browser-data`,
      image,
    ])
  ).stdout.trim();
  let healthy = false;
  for (let i = 0; i < 90; i++) {
    try {
      await run([
        "exec",
        container,
        "node",
        "/opt/browser-sandbox/dist/healthcheck.js",
      ]);
      healthy = true;
      break;
    } catch {
      await delay(1000);
    }
  }
  assert(healthy, "Browser renderer probe failed");
  // Quality and image workflows run this without a dist build, so allocate identities inline.
  const runHarness = async (harness, index) => {
    const executionId = randomUUID();
    const executorUid = 100001 + index;
    const executionFiles = `/tmp/browser-data/${executionId}/files`;
    try {
      const { wsEndpoint } = await control({
        op: "register",
        executionId,
        executorUid,
      });
      const args = [
        "run",
        "--rm",
        "--init",
        "--network",
        `container:${container}`,
        "--user",
        `${executorUid}:${executorUid}`,
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
        "--memory",
        "2g",
        "--mount",
        `type=volume,src=${filesVolume},volume-subpath=${executionId}/files,dst=${executionFiles},readonly`,
        "--tmpfs",
        "/tmp:rw,size=256m,mode=1777",
        "-e",
        `BROWSER_WS_ENDPOINT=${wsEndpoint}`,
        "-e",
        `E2E_MODE=${mode}`,
        "-e",
        "E2E_SCENARIO",
        "-e",
        `BROWSER_FILES_DIRECTORY=${executionFiles}`,
      ];
      if (mode === "model")
        for (const key of ["OPENAI_BASE_URL", "OPENAI_API_KEY", "OPENAI_MODEL_NAME"])
          args.push("-e", key);
      const result = await run([
        ...args,
        "browser-sandbox-e2e:local",
        `/project/.github/scripts/e2e/${harness}.mjs`,
      ]);
      if (harness === "unused" && process.env.PERFORMANCE_OUTPUT)
        await writeFile(
          process.env.PERFORMANCE_OUTPUT,
          JSON.stringify(JSON.parse(result.stdout), null, 2),
        );
      else process.stdout.write(result.stdout);
      await control({ op: "finalize", executionId });
      console.log(`${harness} ${mode}: host finalization passed`);
    } catch (error) {
      error.harness = harness;
      throw error;
    }
  };
  const failures = (await Promise.allSettled(harnesses.map(runHarness)))
    .filter((outcome) => outcome.status === "rejected")
    .map((outcome) => outcome.reason);
  if (failures.length) throw new AggregateError(failures);
  if (harnesses.length > 1)
    console.log(`${harnesses.join(",")} ${mode}: concurrent executions passed`);
} catch (outcome) {
  for (const error of outcome instanceof AggregateError ? outcome.errors : [outcome])
    await report(error, error.harness ?? harnesses.join(","));
  process.exitCode = 1;
} finally {
  if (container) await run(["rm", "-f", container]);
  if (extraction) await run(["rm", "-f", extraction]);
  if (filesVolume) await run(["volume", "rm", filesVolume]);
  await rm(root, { recursive: true, force: true });
}

async function report(error, harness) {
  if (harness === "unused" && process.env.PERFORMANCE_OUTPUT && error.stdout) {
    try {
      await writeFile(
        process.env.PERFORMANCE_OUTPUT,
        JSON.stringify(JSON.parse(error.stdout), null, 2),
      );
    } catch {
      /* Preserve only valid benchmark JSON. */
    }
  }
  const toolFailure = error.stderr
    ?.split("\n")
    .find((line) =>
      /^E2E_TOOL_FAILURE (retain|resume|files) (browser_[a-z_]+|unknown) [a-z0-9_,]+$/.test(
        line,
      ),
    );
  if (toolFailure) console.error(toolFailure);
  const diagnostic = error.stderr
    ?.split("\n")
    .find((line) =>
      /^E2E_FAILURE [A-Za-z]+ at (\.github\/scripts\/e2e\/(pi|dsh|opencode|agent-browser|unused|session-mcp|fixture)\.mjs:\d+:\d+|unknown)$/.test(
        line,
      ),
    );
  if (diagnostic) console.error(diagnostic);

  // Provider errors can contain credentials; only harness-owned diagnostics are printable.
  console.error(
    `${harness} ${mode} E2E failed (exit ${error.code ?? "unknown"}); raw subprocess output withheld`,
  );
}
