import "./diagnostics.mjs";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { sessionRequest } from "../../../dist/session.js";
import {
  assertPng,
  complete,
  fixture,
  HANDOFF_MS,
  inspect,
  pageId,
  prepareFiles,
  uploadUid,
} from "./fixture.mjs";

const require = createRequire(import.meta.url);
const cli = join(
  dirname(require.resolve("opencode-ai/package.json")),
  "bin/opencode.exe",
);
const chrome = join(
  dirname(require.resolve("chrome-devtools-mcp/package.json")),
  "build/src/bin/chrome-devtools-mcp.js",
);
const endpoint = process.env.BROWSER_WS_ENDPOINT;
const modelMode = process.env.E2E_MODE === "model";
if (modelMode)
  for (const key of ["OPENAI_BASE_URL", "OPENAI_API_KEY", "OPENAI_MODEL_NAME"])
    assert(process.env[key], `${key} is required`);
const home = await mkdtemp(join(tmpdir(), "browser-opencode-"));
const site = await fixture();
const files = await prepareFiles(site, endpoint);
let scripted;
let plan = [];
let step = 0;
try {
  // Drive the real OpenCode tool loop without a model secret on fork PRs.
  if (!modelMode) {
    scripted = createServer(async (req, res) => {
      try {
        let input = "";
        for await (const chunk of req) input += chunk;
        const request = JSON.parse(input);
        const next = request.tools?.length ? plan[step++] : undefined;
        if (next) assert(request.tools.some((tool) => tool.function.name === next[0]));
        let args = next?.[1];
        if (
          next &&
          [
            "browser_evaluate_script",
            "browser_select_page",
            "browser_upload_file",
            "browser_take_snapshot",
            "browser_take_screenshot",
          ].includes(next[0])
        ) {
          const pages = request.messages
            .filter((message) => message.role === "tool")
            .map((message) => String(message.content))
            .join("\n");
          args = {
            ...args,
            pageId: pageId({ content: [{ type: "text", text: pages }] }, site.url),
          };
        }
        if (next?.[0] === "browser_upload_file") {
          const snapshot = request.messages
            .filter((message) => message.role === "tool")
            .map((message) => String(message.content))
            .join("\n");
          args = {
            ...args,
            uid: uploadUid({ content: [{ type: "text", text: snapshot }] }),
          };
        }
        const delta = next
          ? {
              tool_calls: [
                {
                  index: 0,
                  id: randomUUID(),
                  type: "function",
                  function: { name: next[0], arguments: JSON.stringify(args) },
                },
              ],
            }
          : { content: "Done." };
        res.writeHead(200, { "content-type": "text/event-stream" });
        for (const [value, finish] of [
          [delta, null],
          [{}, next ? "tool_calls" : "stop"],
        ])
          res.write(
            `data: ${JSON.stringify({
              id: "scripted",
              object: "chat.completion.chunk",
              created: 0,
              model: "scripted",
              choices: [{ index: 0, delta: value, finish_reason: finish }],
            })}\n\n`,
          );
        res.end("data: [DONE]\n\n");
      } catch {
        res.writeHead(400).end();
      }
    });
    for (let attempt = 0; attempt < 10; attempt++) {
      scripted.listen(0, "127.0.0.1");
      await once(scripted, "listening");
      const port = scripted.address().port;
      if (port < 42000 || port >= 43000) break;
      await new Promise((resolve) => scripted.close(resolve));
    }
    assert(scripted.listening, "No unprotected scripted provider port available");
  }
  const model = modelMode ? process.env.OPENAI_MODEL_NAME : "scripted";
  await writeFile(
    join(home, "opencode.json"),
    JSON.stringify({
      $schema: "https://opencode.ai/config.json",
      model: `integration/${model}`,
      small_model: `integration/${model}`,
      share: "disabled",
      autoupdate: false,
      enabled_providers: ["integration"],
      provider: {
        integration: {
          npm: "@ai-sdk/openai-compatible",
          options: {
            baseURL: modelMode
              ? process.env.OPENAI_BASE_URL
              : `http://127.0.0.1:${scripted.address().port}/v1`,
            apiKey: modelMode ? "{env:OPENAI_API_KEY}" : "scripted",
          },
          models: {
            [model]: { name: model, limit: { context: 128000, output: 4096 } },
          },
        },
      },
      permission: { "*": "deny", "browser_*": "allow", "runtime_*": "allow" },
      mcp: {
        browser: {
          type: "local",
          command: [
            process.execPath,
            chrome,
            `--wsEndpoint=${endpoint}`,
            "--no-usage-statistics",
          ],
          timeout: 30000,
        },
        runtime: {
          type: "local",
          command: [
            process.execPath,
            fileURLToPath(new URL("./session-mcp.mjs", import.meta.url)),
          ],
          timeout: 30000,
        },
      },
    }),
    { mode: 0o600 },
  );
  let marker;
  let generation;
  for (const phase of ["retain", "resume"]) {
    const session = (action) => ["runtime_session", { action, durationMs: HANDOFF_MS }];
    plan =
      phase === "retain"
        ? [
            ["browser_new_page", { url: site.url }],
            ["browser_evaluate_script", { function: inspect }],
            ["browser_take_snapshot", {}],
            [
              "browser_take_screenshot",
              { format: "png", filePath: join(home, "screenshot.png") },
            ],
            ["browser_upload_file", { filePaths: [files.path] }],
            [
              "browser_evaluate_script",
              {
                function:
                  "() => { document.querySelector('#download').click(); return 'downloading'; }",
              },
            ],
            session("retain"),
          ]
        : [
            session("status"),
            session("renew"),
            ["browser_list_pages", {}],
            ["browser_select_page", {}],
            ["browser_evaluate_script", { function: inspect }],
            ["browser_evaluate_script", { function: complete }],
            session("release"),
          ];
    step = 0;
    const prompt =
      phase === "retain"
        ? `Open ${site.url} using browser_new_page, inspect the page using browser_take_snapshot, upload ${files.path} through the Upload test file input using browser_upload_file, click Download test file, then call runtime_session action retain with durationMs ${HANDOFF_MS}. Finish this turn leaving the page unchanged for another agent.`
        : `First call runtime_session action status and then renew with durationMs ${HANDOFF_MS}. Use browser_list_pages and browser_select_page to find the existing page at ${site.url}. Read its displayed challenge, fill the Answer field, and click Finish using browser tools. Confirm Completed, then call runtime_session action release. Preserve the existing page until submission.`;
    let output;
    try {
      const work = promisify(execFile)(cli, ["run", "--format", "json", prompt], {
        cwd: home,
        env: {
          ...process.env,
          HOME: home,
          XDG_CONFIG_HOME: join(home, "config"),
          XDG_DATA_HOME: join(home, "data"),
          XDG_CACHE_HOME: join(home, "cache"),
          XDG_STATE_HOME: join(home, "state"),
          OPENCODE_CONFIG: join(home, "opencode.json"),
          OPENCODE_DISABLE_DEFAULT_PLUGINS: "true",
          OPENCODE_DISABLE_MODELS_FETCH: "true",
        },
        timeout: 150000,
        maxBuffer: 8 * 1024 * 1024,
      });
      work.child.stdin.end();
      output = (await work).stdout;
    } catch {
      throw Error(`OpenCode ${phase} failed; raw provider output withheld`);
    }
    const events = output
      .split("\n")
      .filter((line) => line.startsWith("{"))
      .map((line) => JSON.parse(line));
    assert(
      !events.some((event) => event.type === "error"),
      "OpenCode reported an error",
    );
    const calls = events
      .filter((event) => event.type === "tool_use")
      .map((event) => event.part);
    assert(
      calls.length > 0 && calls.every((call) => call.state.status === "completed"),
      "OpenCode tool execution failed",
    );
    const control = calls.filter((call) => call.tool === "runtime_session");
    assert(
      control.some(
        (call) =>
          call.state.input.action === (phase === "retain" ? "retain" : "release"),
      ),
      "OpenCode did not control its browser session",
    );
    if (phase === "retain") {
      if (!modelMode) assertPng(await readFile(join(home, "screenshot.png")));
      const retained = await sessionRequest(endpoint);
      assert.equal(retained.state, "retained");
      generation = retained.generation;
      for (let i = 0; i < 100 && (await sessionRequest(endpoint)).connectedClients; i++)
        await delay(50);
      assert.equal((await sessionRequest(endpoint)).connectedClients, 0);
      await delay(1150);
      assert.equal((await sessionRequest(endpoint)).generation, generation);
      if (!modelMode)
        marker = calls.find((call) => call.tool === "browser_evaluate_script").state
          .output;
    } else {
      assert(
        control.some(
          (call) =>
            call.state.input.action === "renew" &&
            JSON.parse(call.state.output).generation === generation,
        ),
        "OpenCode did not renew the original browser",
      );
      if (!modelMode) {
        const after = calls.find((call) => call.tool === "browser_evaluate_script")
          .state.output;
        const parsed = (text) => JSON.parse(/```json\s*([\s\S]*?)\s*```/.exec(text)[1]);
        assert.equal(parsed(after).marker, parsed(marker).marker);
        assert(parsed(after).ticks > parsed(marker).ticks);
      }
    }
  }
  await site.verify();
  await files.verify();
  console.log(
    `OpenCode ${process.env.E2E_MODE ?? "tools"}: real CLI, MCP browser tools, session handoff, upload/download, and form submission passed`,
  );
} finally {
  scripted?.closeAllConnections();
  if (scripted) await new Promise((resolve) => scripted.close(resolve));
  await sessionRequest(endpoint, "stop");
  await site.close();
  await rm(home, { recursive: true, force: true });
}
