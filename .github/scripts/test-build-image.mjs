import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { promisify } from "node:util";

const run = promisify(execFile);
const workflow = await readFile(
  new URL("../workflows/build-image.yml", import.meta.url),
  "utf8",
);
function script(name) {
  const step = workflow.split(`      - name: ${name}\n`)[1];
  assert.ok(step, `Missing workflow step: ${name}`);
  const block = step.split("        run: |\n")[1].match(/^(?: {10}.*\n|\n)+/)[0];
  return block.replace(/^ {10}/gm, "");
}

test("architecture pushes have a deadline and never publish a digest after failure", async () => {
  const root = await mkdtemp(join(tmpdir(), "build-image-push-"));
  try {
    const env = {
      ...process.env,
      PATH: `${root}:${process.env.PATH}`,
      IMAGE: "registry.example.com/example/browser",
      GITHUB_RUN_ID: "123",
      GITHUB_RUN_ATTEMPT: "2",
      ARCH: "arm64",
      TRACE: join(root, "trace"),
    };
    await writeFile(
      join(root, "timeout"),
      '#!/bin/sh\nprintf "deadline %s\\n" "$*" >> "$TRACE"\n[ "$1" = --signal=TERM ] && [ "$2" = --kill-after=30s ] && [ "$3" = 10m ] || exit 99\nshift 3\nexec "$@"\n',
      { mode: 0o700 },
    );
    await writeFile(
      join(root, "script"),
      '#!/bin/sh\n[ "$1" = --quiet ] && [ "$2" = --return ] && [ "$3" = --command ] || exit 98\nexec bash -c "$4"\n',
      { mode: 0o700 },
    );
    await writeFile(
      join(root, "docker"),
      '#!/bin/sh\nprintf "docker %s\\n" "$*" >> "$TRACE"\nif [ "$1" = push ]; then exit "$PUSH_STATUS"; fi\nif [ "$1 $2" = "image inspect" ]; then printf "%s@sha256:verified\\n" "$IMAGE"; fi\n',
      { mode: 0o700 },
    );
    for (const status of [124, 9, 0]) {
      await writeFile(env.TRACE, "");
      const push = run("bash", ["-c", script("Push verified architecture image")], {
        cwd: root,
        env: { ...env, PUSH_STATUS: String(status) },
      });
      if (status === 0) await push;
      else await assert.rejects(push, (error) => error.code === status);
      const trace = await readFile(env.TRACE, "utf8");
      assert.match(trace, /deadline --signal=TERM --kill-after=30s 10m script/);
      assert.match(
        trace,
        /docker push registry.example.com\/example\/browser:build-123-2-arm64/,
      );
      assert.equal(trace.includes("docker image inspect"), status === 0);
    }
    assert.equal(
      await readFile(join(root, "arm64.txt"), "utf8"),
      `${env.IMAGE}@sha256:verified\n`,
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("image publication checks source tags and only updates latest when selected", async () => {
  const root = await mkdtemp(join(tmpdir(), "build-image-"));
  try {
    const env = {
      ...process.env,
      PATH: `${root}:${process.env.PATH}`,
      GITHUB_REF: "refs/heads/master",
      REGISTRY: "registry.example.com",
      NAMESPACE: "example",
      IMAGE_NAMES: "browser browser-alias",
      GITHUB_OUTPUT: join(root, "output"),
      GITHUB_STEP_SUMMARY: join(root, "summary"),
      TRACE: join(root, "trace"),
    };
    const git = (...args) => run("git", args, { cwd: root });
    await git("init", "--quiet");
    await git(
      "-c",
      "user.name=Test",
      "-c",
      "user.email=test@example.com",
      "commit",
      "--allow-empty",
      "-qm",
      "Initial",
    );
    env.GITHUB_SHA = (await git("rev-parse", "HEAD")).stdout.trim();
    await git("tag", "v1.0.0");
    await git(
      "-c",
      "user.name=Test",
      "-c",
      "user.email=test@example.com",
      "tag",
      "-a",
      "v1.0.1",
      "-m",
      "Annotated",
    );
    const validate = (tag, overrides = {}) =>
      run("bash", ["-c", script("Validate publication configuration")], {
        cwd: root,
        env: { ...env, IMAGE_TAG: tag, ...overrides },
      });
    await validate("v1.0.0");
    await validate("v1.0.1");
    await assert.rejects(validate("v9.0.0"), /Create and push/);
    await assert.rejects(validate("v1.0.0", { GITHUB_SHA: "other" }), /must point/);
    for (const tag of ["latest", "sha-reserved", "bad/tag", "", "$(exit 0)"]) {
      await assert.rejects(validate(tag));
    }
    await assert.rejects(validate("v1.0.0", { GITHUB_REF: "refs/heads/other" }));
    await writeFile(
      join(root, "amd64.txt"),
      "registry.example.com/example/browser@sha256:amd64",
    );
    await writeFile(
      join(root, "arm64.txt"),
      "registry.example.com/example/browser@sha256:arm64",
    );
    await writeFile(
      join(root, "docker"),
      '#!/bin/sh\nprintf "%s\\n" "$@" >> "$TRACE"\n',
      { mode: 0o700 },
    );
    for (const latest of ["false", "true"]) {
      await writeFile(env.TRACE, "");
      await run("bash", ["-c", script("Publish verified multi-architecture images")], {
        cwd: root,
        env: { ...env, IMAGE_TAG: "v1.0.0", PUBLISH_LATEST: latest },
      });
      const args = (await readFile(env.TRACE, "utf8")).trim().split("\n");
      const expected = env.IMAGE_NAMES.split(" ").flatMap((name) => {
        const image = `${env.REGISTRY}/${env.NAMESPACE}/${name}`;
        return [
          "buildx",
          "imagetools",
          "create",
          "--tag",
          `${image}:sha-${env.GITHUB_SHA}`,
          "--tag",
          `${image}:v1.0.0`,
          ...(latest === "true" ? ["--tag", `${image}:latest`] : []),
          "registry.example.com/example/browser@sha256:amd64",
          "registry.example.com/example/browser@sha256:arm64",
        ];
      });
      assert.deepEqual(args, expected);
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
