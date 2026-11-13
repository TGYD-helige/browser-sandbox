import assert from "node:assert/strict";
import { test } from "node:test";
import { diagnostics, docker } from "./managed/command.mjs";

test("managed Docker failures keep actionable diagnostics without argv or execution references", async () => {
  const endpoint = "ws://controller:8080/execution/" + "a".repeat(64);
  await assert.rejects(
    docker(["create", "-e", `BROWSER_WS_ENDPOINT=${endpoint}`], async () => {
      throw Object.assign(Error(`Command failed: docker create ${endpoint}`), {
        cmd: `docker create ${endpoint}`,
        stderr: `browser_worker_unavailable ${endpoint} ${"b".repeat(64)}`,
      });
    }),
    (error) => {
      assert.match(error.message, /managed_docker_create_failed/);
      assert.match(error.message, /browser_worker_unavailable/);
      assert(!error.message.includes(endpoint));
      assert(!/[a-f0-9]{64}/.test(error.message));
      assert.equal(error.cmd, undefined);
      return true;
    },
  );
});

test("managed diagnostics retain worker state and known codes without log text or secret fields", () => {
  const record = diagnostics(
    "browser_worker_unavailable ws://private/secret SECRET=value SIGSEGV",
    {
      Running: false,
      OOMKilled: false,
      ExitCode: 1,
      Config: { Env: ["SECRET=value"] },
    },
  );
  assert.deepEqual(record, {
    running: false,
    oomKilled: false,
    exitCode: 1,
    codes: ["browser_worker_unavailable", "SIGSEGV"],
  });
});
