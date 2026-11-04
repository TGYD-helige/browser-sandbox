import "./diagnostics.mjs";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { sessionRequest } from "../../../dist/session.js";

const run = promisify(execFile);
const cli = join(
  dirname(fileURLToPath(import.meta.resolve("@earendil-works/pi-coding-agent"))),
  "cli.js",
);
const extension = fileURLToPath(import.meta.resolve("@amaster.ai/pi-browser-use"));
const samples = Number(process.env.BENCHMARK_SAMPLES ?? 20);
assert(Number.isInteger(samples) && samples >= 10 && samples <= 100);
const server = createServer(async (req, res) => {
  for await (const _chunk of req) {
    /* Drain the scripted provider request. */
  }
  res.writeHead(200, { "content-type": "text/event-stream" });
  for (const [delta, finish_reason] of [
    [{ role: "assistant", content: "Done." }, null],
    [{}, "stop"],
  ])
    res.write(
      `data: ${JSON.stringify({ id: "unused-benchmark", object: "chat.completion.chunk", created: 0, model: "scripted", choices: [{ index: 0, delta, finish_reason }] })}\n\n`,
    );
  res.end("data: [DONE]\n\n");
});
server.listen(18082, "127.0.0.1");
await once(server, "listening");
const durations = { ordinary: [], managed: [], overhead: [] };
try {
  for (let sample = -3; sample < samples; sample++) {
    const pair = {};
    // Alternate order to reduce warm-cache/order bias between matched samples.
    for (const mode of sample % 2 ? ["managed", "ordinary"] : ["ordinary", "managed"]) {
      const home = await mkdtemp(join(tmpdir(), "unused-pi-"));
      try {
        await writeFile(
          join(home, "models.json"),
          JSON.stringify({
            providers: {
              integration: {
                baseUrl: "http://127.0.0.1:18082/v1",
                api: "openai-completions",
                apiKey: "scripted-test-key",
                models: [
                  {
                    id: "scripted",
                    name: "scripted",
                    input: ["text"],
                    contextWindow: 128000,
                    maxTokens: 32,
                  },
                ],
              },
            },
          }),
        );
        await writeFile(
          join(home, "settings.json"),
          JSON.stringify({
            "pi-browser-use":
              mode === "managed"
                ? {
                    sessionMode: "existing",
                    wsEndpoint: process.env.BROWSER_WS_ENDPOINT,
                  }
                : { sessionMode: "persistent", userDataDir: join(home, "profile") },
          }),
        );
        const start = performance.now();
        const task = run(
          process.execPath,
          [
            cli,
            "--provider",
            "integration",
            "--model",
            "scripted",
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
            "-p",
            "Reply Done.",
          ],
          {
            cwd: home,
            env: {
              ...process.env,
              PI_CODING_AGENT_DIR: home,
              PI_AGENT_HOME: home,
              PI_TELEMETRY: "0",
            },
            timeout: 60000,
            maxBuffer: 1024 * 1024,
          },
        );
        task.child.stdin.end();
        const { stdout } = await task;
        pair[mode] = performance.now() - start;
        const events = stdout
          .split("\n")
          .filter((line) => line.startsWith("{"))
          .map((line) => JSON.parse(line));
        assert(
          events.some(
            (event) =>
              event.type === "message_end" &&
              event.message?.role === "assistant" &&
              event.message?.content?.some((part) => part.text === "Done."),
          ),
          "The real Pi CLI did not finish the scripted task",
        );
        assert(!events.some((event) => event.type === "tool_execution_start"));
        assert.equal(
          (await sessionRequest(process.env.BROWSER_WS_ENDPOINT)).state,
          "unused",
        );
      } finally {
        await rm(home, { recursive: true, force: true });
      }
    }
    if (sample >= 0) {
      for (const mode of ["ordinary", "managed"]) durations[mode].push(pair[mode]);
      durations.overhead.push(pair.managed - pair.ordinary);
    }
  }
  const summary = (values) => {
    const sorted = [...values].sort((a, b) => a - b);
    return {
      samples: sorted.length,
      medianMs: sorted[Math.floor(sorted.length / 2)],
      p95Ms: sorted[Math.ceil(sorted.length * 0.95) - 1],
    };
  };
  const phases = Object.fromEntries(
    Object.entries(durations).map(([name, values]) => [name, summary(values)]),
  );
  const limitMs = phases.ordinary.p95Ms * 1.25 + 25;
  const passed = phases.managed.p95Ms <= limitMs;
  console.log(
    JSON.stringify(
      {
        harness: "Pi",
        budget: "managed P95 <= ordinary P95 * 1.25 + 25 ms",
        limitMs,
        passed,
        node: process.version,
        architecture: process.arch,
        samples,
        warmups: 3,
        measurements: durations,
        phases,
        note: "Matched local-persistent/managed-endpoint Pi CLI tasks with the same browser extension and local scripted provider, no browser tools invoked. Startup includes task completion and process exit, excludes host registration/finalization (measured by the image benchmark). Not a model/network or production SLA measurement.",
      },
      null,
      2,
    ),
  );
  if (!passed) process.exitCode = 1;
} finally {
  server.closeAllConnections();
  await new Promise((resolve) => server.close(resolve));
}
