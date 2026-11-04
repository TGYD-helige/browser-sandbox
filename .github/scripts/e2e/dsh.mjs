import "./diagnostics.mjs";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { boot } from "@deepseek-ai/dsh-app-boot";
import { stringify } from "yaml";
import { sessionRequest } from "../../../dist/session.js";
import {
  complete,
  evaluated,
  fixture,
  HANDOFF_MS,
  inspect,
  pageId,
  prepareFiles,
  transferFiles,
} from "./fixture.mjs";

const home = await mkdtemp(join(tmpdir(), "browser-dsh-"));
process.env.DSH_HOME = home;
const site = await fixture();
const endpoint = process.env.BROWSER_WS_ENDPOINT;
const files = await prepareFiles(site, endpoint);
const resolveCore = createRequire(import.meta.resolve("@deepseek-ai/dsh-sdk-minimal"));
const plugin = (name) => {
  try {
    return import.meta.resolve(name);
  } catch {
    return pathToFileURL(resolveCore.resolve(name)).href;
  }
};
const model = process.env.OPENAI_MODEL_NAME ?? "unused-tools-only";
const rows = [
  ["timer", "@deepseek-ai/cordis-plugin-timer"],
  ["llm", "@deepseek-ai/dsh-llm"],
  ["session", "@deepseek-ai/dsh-session"],
  ["session-projection", "@deepseek-ai/dsh-session-projection"],
  [
    "system-prompt",
    "@deepseek-ai/dsh-system-prompt",
    {
      includeHarnessIdentity: false,
      includeRuntimeContext: false,
      personaPrefix: "Use the provided browser tools to complete the user's task.",
    },
  ],
  ["tools", "@deepseek-ai/dsh-tools"],
  ["agent", "@deepseek-ai/dsh-agent"],
  ["agent-loop", "@deepseek-ai/dsh-agent-loop", { agents: [] }],
  ["filesystem", "@deepseek-ai/dsh-fs-local"],
  [
    "working-directory",
    "@deepseek-ai/dsh-working-directory",
    { defaultDirectory: home },
  ],
  [
    "sessions",
    "@deepseek-ai/dsh-session-persistence-jsonl",
    { root: join(home, "sessions"), compression: "none" },
  ],
  [
    "llm-pi-ai",
    "@deepseek-ai/dsh-llm-pi-ai",
    {
      providers: {
        integration: {
          api: "openai-completions",
          baseURL: process.env.OPENAI_BASE_URL ?? "http://127.0.0.1:1/v1",
          apiKeyEnv: "OPENAI_API_KEY",
          defaultMaxTokens: 4096,
          models: [{ id: model, name: model, input: ["text"], contextWindow: 128000 }],
        },
      },
    },
  ],
  ["browser-use", "@deepseek-ai/dsh-browser-use"],
  [
    "browser-provider",
    "@deepseek-ai/dsh-experimental-browser-use-chrome-devtools-mcp",
    { mode: "attach", endpoint, toolCallTimeoutMs: 30000 },
  ],
];
const config = join(home, "cordis.yml");
await writeFile(
  config,
  stringify(
    rows.map(([id, name, config]) => ({
      id,
      name: plugin(name),
      ...(config ? { config } : {}),
    })),
  ),
);
let ctx, handle;
try {
  ctx = await boot("browser-e2e", config, []);
  assert.equal(ctx.browserUse.providerName, "chrome-devtools-mcp");
  const sessionCalls = [];
  ctx.tools.register({
    name: "browser_session",
    description:
      "Manage the already-open browser for this execution. Retain requires a live browser: open a page and wait for success first. Renew requires retained state and cannot extend the hard deadline. Release ends retention. Stop ends the browser, preserving its Profile.",
    parameters: {
      type: "object",
      properties: {
        action: {
          type: "string",
          enum: ["status", "retain", "renew", "release", "stop"],
        },
        durationMs: { type: "integer", minimum: 1, maximum: 1800000 },
      },
      required: ["action"],
      additionalProperties: false,
    },
    output: {
      schema: { type: "object" },
      render: (_args, value) => [{ type: "text", text: JSON.stringify(value) }],
    },
    async execute(args, exec) {
      const status = await sessionRequest(
        endpoint,
        args.action,
        args.durationMs,
        exec.signal,
      );
      sessionCalls.push({ action: args.action, status });
      return JSON.parse(JSON.stringify(status));
    },
  });
  const create = async () => {
    const result = await ctx.agents.create({
      sessionId: randomUUID(),
      agentOptions: { provider: "integration", model },
      meta: { cwd: home },
    });
    const schemas = ctx.tools.schemas(result.agent);
    assert(
      schemas.some((tool) => tool.name === "mcp__chrome-devtools-mcp__new_page"),
      "DSH browser tools were not registered",
    );
    return result;
  };
  handle = await create();
  const call = async (name, args) => {
    const result = await ctx.tools.execute({
      callId: randomUUID(),
      name: `mcp__chrome-devtools-mcp__${name}`,
      arguments: args,
      agent: handle.agent,
      signal: AbortSignal.timeout(30000),
    });
    assert(!result.isError, `DSH ${name} failed`);
    return result;
  };
  if (process.env.E2E_MODE === "model") {
    for (const key of ["OPENAI_BASE_URL", "OPENAI_API_KEY", "OPENAI_MODEL_NAME"])
      assert(process.env[key], `${key} is required`);
    const results = [];
    ctx.on("session/event", (session, event) => {
      if (session === handle.agent.session && event.type === "tool/result")
        results.push(event.data.message);
    });
    const phase = async (prompt) => {
      handle.agent.followup({
        id: randomUUID(),
        role: "user",
        source: { kind: "user" },
        content: [{ type: "text", text: prompt }],
      });
      const deadline = setTimeout(
        () => handle.agent.cancel({ kind: "user", reason: "E2E deadline" }),
        150000,
      );
      try {
        await handle.agent.whenIdle();
      } finally {
        clearTimeout(deadline);
      }
    };
    await phase(
      `Call mcp__chrome-devtools-mcp__new_page to open ${site.url} and wait for success. Inspect that page, use mcp__chrome-devtools-mcp__upload_file to upload ${files.path} through the Upload test file input, and click Download test file. Then call browser_session action retain with durationMs ${HANDOFF_MS}. Verify retained state and finish this turn, leaving the original page ready for a handoff.`,
    );
    assert(sessionCalls.some((call) => call.action === "retain"));
    const retained = await sessionRequest(endpoint);
    assert.equal(retained.state, "retained");
    const generation = retained.generation;
    console.log(
      `DSH model retain: ${results.length} tool results; runtime state ${retained.state}`,
    );
    await handle.dispose();
    for (let i = 0; i < 100 && (await sessionRequest(endpoint)).connectedClients; i++)
      await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal((await sessionRequest(endpoint)).connectedClients, 0);
    await new Promise((resolve) => setTimeout(resolve, 1150));
    handle = await create();
    await phase(
      `First call browser_session action status, then action renew with durationMs ${HANDOFF_MS}. After those succeed, use browser tools to find and select the existing page at ${site.url}, copy its displayed challenge into the Answer field, and click Finish. Confirm Completed, then call browser_session action release.`,
    );
    assert(
      sessionCalls.some(
        (call) => call.action === "renew" && call.status.generation === generation,
      ),
    );
    assert(sessionCalls.some((call) => call.action === "release"));
    assert.equal((await sessionRequest(endpoint)).state, "live");
    assert(
      results.length > 0 && results.every((result) => !result.isError),
      "DSH model did not successfully execute browser tools",
    );
  } else {
    const opened = await call("new_page", { url: site.url });
    const id = pageId(opened, site.url);
    await transferFiles(call, id, files);
    const before = evaluated(
      await call("evaluate_script", { pageId: id, function: inspect }),
    );
    const session = await sessionRequest(endpoint, "retain", 30000);
    await handle.dispose();
    await new Promise((resolve) => setTimeout(resolve, 1150));
    handle = await create();
    const resumed = pageId(await call("list_pages", {}), site.url);
    await call("select_page", { pageId: resumed });
    const after = evaluated(
      await call("evaluate_script", { pageId: resumed, function: inspect }),
    );
    assert.equal(before.marker, after.marker);
    assert(after.ticks > before.ticks);
    assert.equal((await sessionRequest(endpoint)).generation, session.generation);
    await call("evaluate_script", { pageId: resumed, function: complete });
    await sessionRequest(endpoint, "release");
  }
  await site.verify();
  await files.verify();
  console.log(
    `DSH ${process.env.E2E_MODE ?? "tools"}: real provider, browser tools, upload/download, and form submission passed`,
  );
} finally {
  await handle?.dispose();
  await ctx?.fiber.dispose();
  await sessionRequest(endpoint, "stop");
  await site.close();
  await rm(home, { recursive: true, force: true });
}
