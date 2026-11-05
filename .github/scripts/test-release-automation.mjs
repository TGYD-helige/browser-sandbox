import assert from "node:assert/strict";
import test from "node:test";
import { generateSummary, releaseNotes } from "./generate-release-summary.mjs";
import labelPr, { classify } from "./label-pr.mjs";

const file = (filename, additions = 1, deletions = 0) => ({
  filename,
  additions,
  deletions,
});
test("labels follow titles, renamed paths, size boundaries and ignore generated churn", () => {
  assert.deepEqual(
    classify("fix(profiles)!: restore", [
      file("src/cookies.ts"),
      file("pnpm-lock.yaml", 5000),
    ]),
    ["type/bug", "area/runtime", "area/profiles", "size:XS"],
  );
  for (const [lines, size] of [
    [19, "XS"],
    [20, "S"],
    [100, "M"],
    [300, "L"],
    [1000, "XL"],
  ]) {
    assert(
      classify("plain title", [file("tests/example.mjs", lines)]).includes(
        `size:${size}`,
      ),
    );
  }
  const result = classify("docs: update", [
    { ...file("docs/usage.md"), previous_filename: ".github/example.md" },
  ]);
  assert(result.includes("area/docs"));
  assert(result.includes("area/ci"));
});

test("sync removes stale managed labels and preserves maintainer labels", async () => {
  const added = [],
    removed = [];
  const files = [file("README.md")];
  const github = {
    paginate: async (method) =>
      method === "labels"
        ? [...(await import("./label-pr.mjs")).labels.keys()].map((name) => ({ name }))
        : files,
    rest: {
      pulls: {
        listFiles: "files",
        get: async () => ({
          data: {
            number: 1,
            title: "docs: update",
            state: "open",
            base: { ref: "master" },
            changed_files: 1,
            labels: [{ name: "P1" }, { name: "type/bug" }, { name: "area/custom" }],
          },
        }),
      },
      issues: {
        listLabelsForRepo: "labels",
        addLabels: async ({ labels }) => added.push(...labels),
        removeLabel: async ({ name }) => removed.push(name),
      },
    },
  };
  await labelPr({
    github,
    context: {
      repo: { owner: "example", repo: "browser" },
      payload: { inputs: { pr: "1" } },
    },
    core: { info() {} },
  });
  assert(added.includes("type/docs"));
  assert.deepEqual(removed, ["type/bug"]);
});

const env = {
  GITHUB_REPOSITORY: "example/browser",
  GH_TOKEN: "github-secret",
  GITHUB_SHA: "a".repeat(40),
  OPENAI_BASE_URL: "https://model.example/v1",
  OPENAI_API_KEY: "model-secret",
  OPENAI_MODEL_NAME: "configured-model",
};
test("summary keeps original source, bounds untrusted model input, and uses separate credentials", async () => {
  const calls = [];
  const request = async (url, options) => {
    calls.push({ url, ...options });
    return {
      ok: true,
      json: async () =>
        url.includes("api.github.com")
          ? { body: "Changes including untrusted instructions" }
          : {
              choices: [
                { message: { content: "## Highlights\n\n- Improved profiles." } },
              ],
            },
    };
  };
  const body = await releaseNotes("v1.0.0", env, request);
  assert(body.includes("## Highlights"));
  assert(body.includes("Changes including untrusted instructions"));
  assert.equal(calls[0].headers.Authorization, "Bearer github-secret");
  assert.equal(calls[1].headers.Authorization, "Bearer model-secret");
  assert.equal(JSON.parse(calls[1].body).model, "configured-model");
  const messages = JSON.parse(calls[1].body).messages;
  assert(messages[0].content.includes("untrusted"));
  assert(messages[1].content.includes("untrusted instructions"));
  await generateSummary(
    "x".repeat(100000),
    { ...env, OPENAI_BASE_URL: "http://model.example/v1" },
    async (url, options) => {
      assert.equal(url, "http://model.example/v1/chat/completions");
      assert.equal(
        JSON.parse(JSON.parse(options.body).messages[1].content).generatedNotes.length,
        60000,
      );
      return {
        ok: true,
        json: async () => ({ choices: [{ message: { content: "summary" } }] }),
      };
    },
  );
});

test("missing credentials, gateway errors and empty output preserve GitHub notes; source failures stop", async () => {
  for (const mode of ["missing", "failure", "empty"]) {
    const request = async (url) =>
      url.includes("api.github.com")
        ? { ok: true, json: async () => ({ body: "Original PR list" }) }
        : { ok: mode === "empty", status: 503, json: async () => ({ choices: [] }) };
    assert.equal(
      await releaseNotes(
        "v1.0.0",
        { ...env, ...(mode === "missing" ? { OPENAI_API_KEY: "" } : {}) },
        request,
      ),
      "Original PR list",
    );
  }
  await assert.rejects(
    releaseNotes("v1.0.0", env, async () => ({ ok: false, status: 403 })),
    /HTTP 403/,
  );
  await assert.rejects(
    generateSummary("notes", {
      ...env,
      OPENAI_BASE_URL: "file:///model.example",
    }),
    /Invalid summary endpoint/,
  );
});
