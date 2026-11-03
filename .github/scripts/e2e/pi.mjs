import "./diagnostics.mjs";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
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
  wsEndpoint: endpoint,
  session: { enabled: true, key: home, retentionMs: HANDOFF_MS },
};
try {
  if (process.env.E2E_MODE === "model") {
    for (const key of ["OPENAI_BASE_URL", "OPENAI_API_KEY", "OPENAI_MODEL_NAME"])
      assert(process.env[key], `${key} is required`);
    await writeFile(
      join(home, "models.json"),
      JSON.stringify({
        providers: {
          integration: {
            baseUrl: process.env.OPENAI_BASE_URL,
            api: "openai-completions",
            apiKey: "$OPENAI_API_KEY",
            authHeader: true,
            models: [
              {
                id: process.env.OPENAI_MODEL_NAME,
                name: process.env.OPENAI_MODEL_NAME,
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
    for (const phase of ["retain", "resume"]) {
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
            process.env.OPENAI_MODEL_NAME,
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
      } catch {
        throw Error(`Pi model ${phase} failed; raw provider output withheld`);
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
  } else {
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
} finally {
  const client = new DevToolsClient(config);
  try {
    await client.sessionControl("release");
  } finally {
    await client.close();
  }
  await sessionRequest(endpoint, "stop");
  await site.close();
  await rm(home, { recursive: true, force: true });
}
