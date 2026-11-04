import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const run = promisify(execFile);
test("release tag check allows confirmed absence and fences existing tags and uncertain registry failures", async () => {
  const root = await mkdtemp(join(tmpdir(), "release-tag-"));
  try {
    await writeFile(
      join(root, "docker"),
      '#!/bin/sh\nprintf "%s" "$REGISTRY_OUTPUT"\nexit "$REGISTRY_STATUS"\n',
      { mode: 0o700 },
    );
    const check = (status, output) =>
      run(
        "bash",
        [
          fileURLToPath(
            new URL("../.github/scripts/check-image-tag.sh", import.meta.url),
          ),
          "ghcr.io/example/browser:v1.0.0",
        ],
        {
          env: {
            ...process.env,
            PATH: `${root}:${process.env.PATH}`,
            REGISTRY_STATUS: String(status),
            REGISTRY_OUTPUT: output,
          },
        },
      );
    await check(1, "no such manifest: ghcr.io/example/browser:v1.0.0");
    await assert.rejects(check(0, "{}"), /Version already exists/);
    await assert.rejects(
      check(1, "unauthorized: authentication required"),
      /Unable to confirm/,
    );
    await assert.rejects(check(1, "connection timed out"), /Unable to confirm/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
