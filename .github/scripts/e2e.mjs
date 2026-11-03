import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
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
const harness = process.argv[2];
assert(
  ["pi", "dsh", "opencode", "agent-browser"].includes(harness),
  "Choose pi, dsh, opencode, or agent-browser",
);
const mode = process.env.E2E_MODE ?? "tools";
assert(["tools", "model"].includes(mode), "Choose tools or model");
if (mode === "model")
  for (const key of ["OPENAI_BASE_URL", "OPENAI_API_KEY", "OPENAI_MODEL_NAME"])
    assert(process.env[key], `${key} is required`);
const root = await mkdtemp(join(tmpdir(), "browser-e2e-"));
const image = process.env.BROWSER_IMAGE ?? "browser-sandbox:local";
let container, extraction, filesVolume;
const executionId = randomUUID();
const executionFiles = `/tmp/browser-data/${executionId}/files`;
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
      "2g",
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
  const { wsEndpoint } = await control({
    op: "register",
    executionId,
    executorUid: 100001,
  });
  const args = [
    "run",
    "--rm",
    "--init",
    "--network",
    `container:${container}`,
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
  process.stdout.write(result.stdout);
  await control({ op: "finalize", executionId });
  console.log(`${harness} ${mode}: host finalization passed`);
} catch (error) {
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
      /^E2E_FAILURE [A-Za-z]+ at (\.github\/scripts\/e2e\/(pi|dsh|opencode|agent-browser|session-mcp|fixture)\.mjs:\d+:\d+|unknown)$/.test(
        line,
      ),
    );
  if (diagnostic) console.error(diagnostic);

  // Provider errors can contain credentials; only harness-owned diagnostics are printable.
  console.error(
    `${harness} ${mode} E2E failed (exit ${error.code ?? "unknown"}); raw subprocess output withheld`,
  );
  process.exitCode = 1;
} finally {
  if (container) await run(["rm", "-f", container]);
  if (extraction) await run(["rm", "-f", extraction]);
  if (filesVolume) await run(["volume", "rm", filesVolume]);
  await rm(root, { recursive: true, force: true });
}
