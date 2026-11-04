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

test("release source tag preflight rejects existing refs and uncertain GitHub failures", async () => {
  const root = await mkdtemp(join(tmpdir(), "release-source-"));
  try {
    await writeFile(
      join(root, "gh"),
      '#!/bin/sh\nprintf "%s" "$GITHUB_RESPONSE"\nexit "$GITHUB_STATUS"\n',
      { mode: 0o700 },
    );
    const check = (status, response) =>
      run(
        "bash",
        [
          fileURLToPath(
            new URL("../.github/scripts/check-release-tag.sh", import.meta.url),
          ),
          "v1.0.0",
        ],
        {
          env: {
            ...process.env,
            PATH: `${root}:${process.env.PATH}`,
            GITHUB_REPOSITORY: "example/browser",
            GITHUB_STATUS: String(status),
            GITHUB_RESPONSE: response,
          },
        },
      );
    await check(1, "HTTP/2.0 404 Not Found\nContent-Type: application/json\n");
    await assert.rejects(
      check(0, "HTTP/2.0 200 OK\n"),
      /Git version tag already exists/,
    );
    await assert.rejects(check(1, "HTTP/2.0 403 Forbidden\n"), /Unable to confirm/);
    await assert.rejects(check(1, "connection timed out"), /Unable to confirm/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("publication reserves the verified Git commit atomically and stops before pushing on a tag conflict", async () => {
  const root = await mkdtemp(join(tmpdir(), "release-publish-"));
  const { readFile } = await import("node:fs/promises");
  try {
    await writeFile(join(root, "amd64.txt"), "ghcr.io/example/browser@sha256:amd64");
    await writeFile(join(root, "arm64.txt"), "ghcr.io/example/browser@sha256:arm64");
    await writeFile(
      join(root, "docker"),
      '#!/bin/sh\nprintf "docker %s\\n" "$*" >> "$TRACE"\nif [ "$1 $2" = "manifest inspect" ]; then printf "no such manifest: %s" "$3"; exit 1; fi\n',
      { mode: 0o700 },
    );
    await writeFile(
      join(root, "gh"),
      '#!/bin/sh\nprintf "gh %s\\n" "$*" >> "$TRACE"\nif [ "$1" = api ]; then exit "$RESERVE_STATUS"; fi\n',
      { mode: 0o700 },
    );
    const trace = join(root, "trace");
    const publish = (status) =>
      run(
        "bash",
        [
          fileURLToPath(
            new URL("../.github/scripts/publish-release.sh", import.meta.url),
          ),
          "ghcr.io/example/browser",
          "v1.0.0",
        ],
        {
          cwd: root,
          env: {
            ...process.env,
            PATH: `${root}:${process.env.PATH}`,
            TRACE: trace,
            RESERVE_STATUS: String(status),
            GITHUB_REPOSITORY: "example/browser",
            GITHUB_SHA: "a".repeat(40),
          },
        },
      );
    await assert.rejects(publish(1));
    let requests = await readFile(trace, "utf8");
    assert(requests.includes(`ref=refs/tags/v1.0.0 -f sha=${"a".repeat(40)}`));
    assert(!requests.includes("buildx imagetools create"));
    assert(!requests.includes("gh release create"));
    await writeFile(trace, "");
    await publish(0);
    requests = await readFile(trace, "utf8");
    assert(
      requests.indexOf("gh api --method POST") <
        requests.indexOf("docker buildx imagetools create"),
    );
    assert(requests.includes("gh release create v1.0.0 --verify-tag"));
    assert(!requests.includes("--target"));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
