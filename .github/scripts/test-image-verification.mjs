import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const run = promisify(execFile);
test("only benchmark containers omit periodic healthchecks while keeping explicit readiness", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "image-verification-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const trace = join(root, "trace");
  await writeFile(
    join(root, "docker"),
    '#!/bin/sh\nprintf "%s\\n" "$*" >> "$TRACE"\ncase "$1" in create) echo extraction ;; run) case "$*" in *benchmark-cpu.mjs*affinity*) echo 2,5 ;; *) echo runtime ;; esac ;; esac\n',
    { mode: 0o700 },
  );
  for (const benchmark of [false, true]) {
    await writeFile(trace, "");
    await run(
      "bash",
      [fileURLToPath(new URL("verify-image.sh", import.meta.url)), "fixture:image"],
      {
        cwd: root,
        env: {
          ...process.env,
          PATH: `${root}:${process.env.PATH}`,
          TRACE: trace,
          BENCHMARK_OUTPUT: benchmark ? join(root, "performance.json") : "",
          VERIFY_EXTENSIONS: "0",
        },
      },
    );
    const calls = (await readFile(trace, "utf8")).trim().split("\n");
    const launch = calls.find((line) => line.startsWith("run -d "));
    assert.equal(launch.includes("--no-healthcheck"), benchmark);
    assert.equal(launch.includes("--cpuset-cpus 2,5"), benchmark);
    assert.match(launch, /--security-opt no-new-privileges/);
    assert(
      calls.some(
        (line) => line === "exec runtime node /opt/browser-sandbox/dist/healthcheck.js",
      ),
    );
    assert(
      calls.some(
        (line) =>
          line ===
          "exec runtime node /opt/browser-sandbox/.github/scripts/image-smoke.mjs",
      ),
    );
    assert(calls.some((line) => line === "rm -f runtime"));
  }
});
