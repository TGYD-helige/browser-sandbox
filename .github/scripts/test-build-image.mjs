import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const run = promisify(execFile);
const workflow = await readFile(
  new URL("../workflows/build-image.yml", import.meta.url),
  "utf8",
);
const source = (name) => fileURLToPath(new URL(name, import.meta.url));
const digest = `sha256:${"a".repeat(64)}`;
async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), "build-image-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  return {
    root,
    env: {
      ...process.env,
      PATH: `${root}:${process.env.PATH}`,
      REGISTRY: "registry.example.com",
      NAMESPACE: "example",
      IMAGE_NAMES: "browser browser-alias",
      IMAGE: "registry.example.com/example/browser",
      ARCH: "arm64",
      DOCKER_CONFIG: root,
      GITHUB_RUN_ID: "123",
      GITHUB_RUN_ATTEMPT: "2",
      TRACE: join(root, "trace"),
      GITHUB_REF: "refs/heads/master",
      GITHUB_OUTPUT: join(root, "output"),
      GITHUB_STEP_SUMMARY: join(root, "summary"),
    },
  };
}
const stub = (root, name, body) =>
  writeFile(join(root, name), `#!/bin/bash\nset -eu\n${body}\n`, { mode: 0o700 });
function validation() {
  const step = workflow.split("      - name: Validate publication configuration\n")[1];
  return step
    .split("        run: |\n")[1]
    .match(/^(?: {10}.*\n|\n)+/)[0]
    .replace(/^ {10}/gm, "");
}

test("source tags may precede the workflow commit but must belong to its master history", async (t) => {
  const { root, env } = await fixture(t);
  const git = (...args) =>
    run("git", ["-c", "user.name=Test", "-c", "user.email=test@example.com", ...args], {
      cwd: root,
    });
  await git("init", "--quiet");
  await git("commit", "--allow-empty", "-qm", "Source");
  const sourceSha = (await git("rev-parse", "HEAD")).stdout.trim();
  await git("tag", "v1.0.0");
  await git("tag", "-a", "v1.0.1", "-m", "Annotated");
  await git("commit", "--allow-empty", "-qm", "Workflow fix");
  env.GITHUB_SHA = (await git("rev-parse", "HEAD")).stdout.trim();
  const unrelated = (
    await git("commit-tree", "HEAD^{tree}", "-m", "Unrelated")
  ).stdout.trim();
  await git("tag", "unrelated", unrelated);
  const validate = (tag, overrides = {}) =>
    run("bash", ["-c", validation()], {
      cwd: root,
      env: { ...env, IMAGE_TAG: tag, ...overrides },
    });
  await validate("v1.0.0");
  await validate("v1.0.1");
  assert.match(
    await readFile(env.GITHUB_OUTPUT, "utf8"),
    new RegExp(`source_sha=${sourceSha}`),
  );
  assert.notEqual(sourceSha, env.GITHUB_SHA);
  await assert.rejects(validate("unrelated"), /must be reachable/);
  await assert.rejects(validate("v9.0.0"), /Create and push/);
  for (const tag of ["latest", "sha-reserved", "bad/tag", "", "$(exit 0)"])
    await assert.rejects(validate(tag));
  await assert.rejects(validate("v1.0.0", { GITHUB_REF: "refs/heads/other" }));
});

test("archive transfer checks integrity and architecture, and emits no digest after failed upload", async (t) => {
  const { root, env } = await fixture(t);
  const archive = "sanitized synthetic image archive";
  await writeFile(join(root, "image.tar"), archive);
  await writeFile(
    join(root, "image.tar.sha256"),
    `${createHash("sha256").update(archive).digest("hex")}  image.tar\n`,
  );
  await stub(
    root,
    "timeout",
    'printf "deadline %s\\n" "$*" >> "$TRACE"\n[ "$1" = --signal=TERM ] && [ "$2" = --kill-after=30s ] && [ "$3" = 15m ]\nshift 3\nexec "$@"',
  );
  await stub(
    root,
    "skopeo",
    `printf "skopeo %s\\n" "$*" >> "$TRACE"
if [[ "$1" == inspect ]]; then printf '{"os":"linux","architecture":"%s"}\\n' "\${CONFIG_ARCH:-arm64}"; exit 0; fi
[[ "$1" == copy ]]
[[ "\${PUSH_STATUS:-0}" == 0 ]] || exit "$PUSH_STATUS"
printf '%s\\n' "\${COPY_DIGEST:-${digest}}" > arm64.digest`,
  );
  const push = (overrides = {}) =>
    run("bash", [source("push-image-archive.sh")], {
      cwd: root,
      env: { ...env, ...overrides },
    });
  await assert.rejects(push({ CONFIG_ARCH: "amd64" }));
  assert(!(await readFile(env.TRACE, "utf8")).includes("skopeo copy"));
  for (const status of [124, 9]) {
    await assert.rejects(
      push({ PUSH_STATUS: String(status) }),
      (error) => error.code === status,
    );
    await assert.rejects(readFile(join(root, "arm64.txt")), { code: "ENOENT" });
  }
  await assert.rejects(
    push({ COPY_DIGEST: "invalid" }),
    /Invalid published image digest/,
  );
  await push();
  assert.equal(
    await readFile(join(root, "arm64.txt"), "utf8"),
    `${env.IMAGE}@${digest}\n`,
  );
  assert.match(
    await readFile(env.TRACE, "utf8"),
    /docker-archive:image.tar docker:\/\/registry.example.com\/example\/browser:build-123-2-arm64/,
  );
  await writeFile(env.TRACE, "");
  await writeFile(join(root, "image.tar"), "corrupt");
  await assert.rejects(push(), /checksum did NOT match/);
  assert.equal(await readFile(env.TRACE, "utf8"), "");
});

test("publication combines both architectures and tags the source commit, with latest opt-in only", async (t) => {
  const { root, env } = await fixture(t);
  env.SOURCE_SHA = "b".repeat(40);
  env.GITHUB_SHA = "c".repeat(40);
  env.IMAGE_TAG = "v1.0.0";
  for (const arch of ["amd64", "arm64"])
    await writeFile(join(root, `${arch}.txt`), `${env.IMAGE}@sha256:${arch}\n`);
  await stub(
    root,
    "crane",
    `printf '%s\\n' "$*" >> "$TRACE"
if [[ "$1" == digest ]]; then echo '${digest}'; fi
if [[ "$1" == copy && "\${FAIL_COPY:-false}" == true ]]; then exit 9; fi`,
  );
  const publish = (overrides = {}) =>
    run("bash", [source("publish-image-index.sh")], {
      cwd: root,
      env: { ...env, ...overrides },
    });
  for (const latest of ["false", "true"]) {
    await writeFile(env.TRACE, "");
    await publish({ PUBLISH_LATEST: latest });
    const calls = (await readFile(env.TRACE, "utf8")).trim().split("\n");
    const staging = `${env.IMAGE}:build-123-2-index`;
    assert.deepEqual(calls, [
      `index append --docker-empty-base --manifest ${env.IMAGE}@sha256:amd64 --manifest ${env.IMAGE}@sha256:arm64 --tag ${staging}`,
      `digest ${staging}`,
      ...env.IMAGE_NAMES.split(" ").flatMap((name) => {
        const image = `${env.REGISTRY}/${env.NAMESPACE}/${name}`;
        return [
          `copy ${env.IMAGE}@${digest} ${image}:sha-${env.SOURCE_SHA}`,
          `tag ${image}@${digest} v1.0.0`,
          ...(latest === "true" ? [`tag ${image}@${digest} latest`] : []),
        ];
      }),
    ]);
  }
  await writeFile(env.TRACE, "");
  await assert.rejects(publish({ PUBLISH_LATEST: "false", FAIL_COPY: "true" }));
  assert(
    !(await readFile(env.TRACE, "utf8"))
      .split("\n")
      .some((line) => line.startsWith("tag ")),
  );
});
