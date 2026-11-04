import "./diagnostics.mjs";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { DevToolsClient } from "@amaster.ai/pi-browser-use";
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
  uploadUid,
} from "./fixture.mjs";

const run = promisify(execFile);
const home = await mkdtemp(join(tmpdir(), "browser-pi-"));
process.env.PI_CODING_AGENT_DIR = home;
process.env.PI_AGENT_HOME = home;
const site = await fixture();
const endpoint = process.env.BROWSER_WS_ENDPOINT;
const files = await prepareFiles(site, endpoint);
const config = {
  sessionMode: "existing",
  toolProfile: "full",
  wsEndpoint: endpoint,
  session: { enabled: true, key: home, retentionMs: HANDOFF_MS },
};
const modelMode = process.env.E2E_MODE === "model";
let scripted;
let plan = [];
let step = 0;
let failure;
try {
  if (!modelMode) {
    scripted = createServer(async (req, res) => {
      try {
        let input = "";
        for await (const chunk of req) {
          input += chunk;
          assert(Buffer.byteLength(input) <= 1024 * 1024);
        }
        const request = JSON.parse(input);
        const next = plan[step];
        let args = next?.[1];
        if (next) {
          assert(request.tools.some((tool) => tool.function.name === next[0]));
          step++;
          const output = {
            content: [
              {
                type: "text",
                text: request.messages
                  .filter((message) => message.role === "tool")
                  .map((message) => String(message.content))
                  .join("\n"),
              },
            ],
          };
          if (
            [
              "browser_take_snapshot",
              "browser_upload_file",
              "browser_evaluate_script",
            ].includes(next[0])
          )
            args = { ...args, pageId: pageId(output, site.url) };
          if (next[0] === "browser_upload_file")
            args = { ...args, uid: uploadUid(output) };
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
            `data: ${JSON.stringify({ id: "scripted", object: "chat.completion.chunk", created: 0, model: "scripted", choices: [{ index: 0, delta: value, finish_reason: finish }] })}\n\n`,
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
  if (modelMode)
    for (const key of ["OPENAI_BASE_URL", "OPENAI_API_KEY", "OPENAI_MODEL_NAME"])
      assert(process.env[key], `${key} is required`);
  await writeFile(
    join(home, "models.json"),
    JSON.stringify({
      providers: {
        integration: {
          baseUrl: modelMode
            ? process.env.OPENAI_BASE_URL
            : `http://127.0.0.1:${scripted.address().port}/v1`,
          api: "openai-completions",
          apiKey: modelMode ? "$OPENAI_API_KEY" : "scripted-test-key",
          authHeader: true,
          models: [
            {
              id: model,
              name: model,
              reasoning: false,
              input: ["text"],
              contextWindow: 128000,
              maxTokens: 4096,
            },
          ],
        },
      },
    }),
    { mode: 0o600 },
  );
  await writeFile(
    join(home, "settings.json"),
    JSON.stringify({ "pi-browser-use": config }),
    { mode: 0o600 },
  );
  const cli = join(
    dirname(fileURLToPath(import.meta.resolve("@earendil-works/pi-coding-agent"))),
    "cli.js",
  );
  const extension = fileURLToPath(import.meta.resolve("@amaster.ai/pi-browser-use"));
  let marker;
  let generation;
  for (const phase of ["retain", "resume"]) {
    step = 0;
    plan =
      phase === "retain"
        ? [
            ["browser_new_page", { url: site.url }],
            ["browser_take_snapshot", {}],
            ["browser_upload_file", { filePaths: [files.path] }],
            [
              "browser_evaluate_script",
              {
                function:
                  "() => { document.querySelector('#download').click(); return 'downloading'; }",
              },
            ],
            ["browser_evaluate_script", { function: inspect }],
            ["browser_session", { action: "retain" }],
          ]
        : [
            ["browser_session", { action: "status" }],
            ["browser_list_pages", {}],
            ["browser_evaluate_script", { function: inspect }],
            ["browser_evaluate_script", { function: complete }],
            ["browser_session", { action: "release" }],
          ];
    const prompt =
      phase === "retain"
        ? `Use browser tools to open ${site.url}. Inspect its page, upload the file at ${files.path} using browser_upload_file to the Upload test file input, and click Download test file. Then call browser_session action retain and finish. Leave the page unchanged for the next agent.`
        : `Call browser_session action status, find the existing page at ${site.url}, copy its displayed challenge into the Answer field, and click Finish using browser tools. Then call browser_session action release. Do not open a new challenge page or navigate before completing the form.`;
    let output;
    try {
      const task = run(
        process.execPath,
        [
          cli,
          "--provider",
          "integration",
          "--model",
          model,
          "--thinking",
          "off",
          "--no-session",
          "--mode",
          "json",
          "--no-approve",
          "--no-extensions",
          "--extension",
          extension,
          "--no-context-files",
          "--no-skills",
          "--no-prompt-templates",
          "--no-mcp",
          "--no-builtin-tools",
          "--tools",
          phase === "retain"
            ? "browser_list_pages,browser_new_page,browser_take_snapshot,browser_evaluate_script,browser_upload_file,browser_click,browser_session"
            : "browser_list_pages,browser_take_snapshot,browser_evaluate_script,browser_fill,browser_click,browser_session",
          "-p",
          prompt,
        ],
        {
          cwd: home,
          env: { ...process.env, PI_TELEMETRY: "0" },
          timeout: 150000,
          maxBuffer: 8 * 1024 * 1024,
        },
      );
      task.child.stdin.end();
      output = (await task).stdout;
    } catch (error) {
      console.error(
        `E2E_TOOL_FAILURE ${phase} unknown ${error.killed ? "timeout" : "process_failed"}`,
      );
      throw Error(`Pi agent ${phase} failed; raw provider output withheld`);
    }
    const events = output
      .split("\n")
      .filter((line) => line.startsWith("{"))
      .map((line) => JSON.parse(line));
    const starts = new Map(
      events
        .filter((event) => event.type === "tool_execution_start")
        .map((event) => [event.toolCallId, event]),
    );
    const results = events.filter((event) => event.type === "tool_execution_end");
    const failed = results.find((event) => event.isError || event.result?.isError);
    if (failed || !results.length) {
      const allowed = [
        "browser_list_pages",
        "browser_new_page",
        "browser_take_snapshot",
        "browser_evaluate_script",
        "browser_upload_file",
        "browser_fill",
        "browser_click",
        "browser_session",
      ];
      const name = allowed.includes(failed?.toolName) ? failed.toolName : "unknown";
      const summary = JSON.stringify(
        failed?.result ?? events.filter((event) => event.type === "message_end"),
      ).toLowerCase();
      const categories = [
        "timeout",
        "connect",
        "unauthorized",
        "forbidden",
        "invalid",
        "closed",
        "pageid",
        "disabled",
        "uid",
        "element",
        "snapshot",
        "stale",
        "required",
        "not found",
        "401",
        "400",
        "429",
        "500",
      ]
        .filter((word) => summary.includes(word))
        .map((word) => word.replaceAll(" ", "_"));
      console.error(
        `E2E_TOOL_FAILURE ${phase} ${name} ${categories.join(",") || "unclassified"}`,
      );
    }
    assert(
      results.length > 0 &&
        results.every((event) => !event.isError && !event.result?.isError),
      "Pi browser tool execution failed",
    );
    assert(
      results.some(
        (event) =>
          event.toolName === "browser_session" &&
          starts.get(event.toolCallId)?.args?.action ===
            (phase === "retain" ? "retain" : "release"),
      ),
      "Pi did not manage its browser session",
    );
    if (!modelMode) {
      assert.equal(step, plan.length, "Pi did not finish the scripted tool plan");
      const inspected = results.find(
        (event) =>
          event.toolName === "browser_evaluate_script" &&
          starts.get(event.toolCallId)?.args?.function === inspect,
      );
      const current = evaluated(inspected.result);
      if (phase === "retain") {
        marker = current;
        generation = (await sessionRequest(endpoint)).generation;
      } else {
        assert.equal(current.marker, marker.marker, "Pi replaced the retained page");
        assert(current.ticks > marker.ticks, "Pi did not preserve the live page");
        assert.equal((await sessionRequest(endpoint)).generation, generation);
      }
    }
    if (phase === "resume")
      assert(
        results.some(
          (event) =>
            event.toolName === "browser_session" &&
            JSON.parse(event.result.content.find((item) => item.type === "text").text)
              .resumed === true,
        ),
        "Pi did not resume the original session",
      );
  }
  await site.verify();
  await files.verify();
  if (!modelMode) {
    let client = new DevToolsClient(config);
    try {
      const opened = await client.callTool("new_page", { url: site.url });
      assert(!opened.isError);
      const id = pageId(opened, site.url);
      await transferFiles((name, args) => client.callTool(name, args), id, files);
      const before = evaluated(
        await client.callTool("evaluate_script", { pageId: id, function: inspect }),
      );
      assert.equal((await client.sessionControl("retain")).state, "retained");
      await client.close();
      await new Promise((resolve) => setTimeout(resolve, 150));
      client = new DevToolsClient(config);
      assert.equal((await client.sessionControl("status")).resumed, true);
      const after = evaluated(
        await client.callTool("evaluate_script", { pageId: id, function: inspect }),
      );
      assert.equal(after.marker, before.marker);
      assert(after.ticks > before.ticks);
      await client.sessionControl("release");
      const retained = await sessionRequest(endpoint, "retain", 30000);
      await client.close();
      for (let i = 0; i < 100 && (await sessionRequest(endpoint)).connectedClients; i++)
        await new Promise((resolve) => setTimeout(resolve, 50));
      assert.equal((await sessionRequest(endpoint)).connectedClients, 0);
      await new Promise((resolve) => setTimeout(resolve, 1150));
      assert.equal((await sessionRequest(endpoint)).generation, retained.generation);
      assert.equal((await sessionRequest(endpoint, "renew", 30000)).state, "retained");
      client = new DevToolsClient({ sessionMode: "existing", wsEndpoint: endpoint });
      const resumed = pageId(await client.callTool("list_pages", {}), site.url);
      const runtimeResumed = evaluated(
        await client.callTool("evaluate_script", {
          pageId: resumed,
          function: inspect,
        }),
      );
      assert.equal(runtimeResumed.marker, before.marker);
      assert(runtimeResumed.ticks > after.ticks);
      assert(
        !(
          await client.callTool("evaluate_script", {
            pageId: resumed,
            function: complete,
          })
        ).isError,
      );
      await sessionRequest(endpoint, "release");
    } finally {
      await client.close();
    }
  }
  await site.verify();
  await files.verify();
  console.log(
    `Pi ${process.env.E2E_MODE ?? "tools"}: real browser tools, session handoff, upload/download, and form submission passed`,
  );
} catch (error) {
  failure = error;
} finally {
  for (const cleanup of [
    async () => {
      const client = new DevToolsClient(config);
      try {
        await client.sessionControl("release");
      } finally {
        await client.close();
      }
    },
    () => sessionRequest(endpoint, "stop"),
    async () => {
      if (scripted) {
        scripted.closeAllConnections();
        await new Promise((resolve) => scripted.close(resolve));
      }
    },
    () => site.close(),
    () => rm(home, { recursive: true, force: true }),
  ]) {
    try {
      await cleanup();
    } catch (error) {
      failure ??= error;
    }
  }
}
if (failure) throw failure;
