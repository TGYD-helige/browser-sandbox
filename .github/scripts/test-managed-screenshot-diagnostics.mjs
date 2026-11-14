import assert from "node:assert/strict";
import { test } from "node:test";
import { diagnoseScreenshot } from "./managed/screenshot-diagnostics.mjs";

test("failed screenshot diagnostics compare raw capture with activation without exposing target URLs", async () => {
  const calls = [];
  let closed = 0,
    captures = 0;
  const report = await diagnoseScreenshot("fixture-endpoint", async () => ({
    closed: Promise.resolve(),
    close: () => {
      closed++;
    },
    call: async (method, params) => {
      calls.push([method, params]);
      if (method === "Target.getTargets")
        return {
          targetInfos: [
            {
              targetId: "page",
              type: "page",
              url: "data:text/html,Managed secret",
              title: "secret",
            },
          ],
        };
      if (method === "Target.attachToTarget") return { sessionId: "session" };
      if (method === "Page.captureScreenshot") {
        if (++captures === 1) throw Error("browser_cdp_timeout");
        return { data: "cG5n" };
      }
      return {};
    },
  }));
  assert.deepEqual(report, {
    targets: [{ id: "page", type: "page" }],
    selected: "page",
    raw: { ok: false, error: "browser_cdp_timeout" },
    afterActivate: { ok: true, byteSize: 3 },
  });
  assert(calls.some(([method]) => method === "Target.activateTarget"));
  assert.deepEqual(calls.find(([method]) => method === "Page.captureScreenshot")[1], {
    format: "png",
    fromSurface: true,
  });
  assert.equal(closed, 3);
  assert(!JSON.stringify(report).includes("secret"));
});

test("screenshot command diagnostics retain timeout and exit metadata without raw CLI output", async () => {
  const report = await diagnoseScreenshot(
    "fixture-endpoint",
    async () => ({
      closed: Promise.resolve(),
      close() {},
      call: async () => ({ targetInfos: [] }),
    }),
    {
      killed: true,
      code: null,
      signal: "SIGTERM",
      stdout: '{"error":"cdp timeout secret https://capability.invalid/token"}',
      stderr: "private-output",
    },
  );
  assert.deepEqual(report.command, {
    killed: true,
    exitCode: null,
    signal: "SIGTERM",
    categories: ["timeout", "cdp"],
  });
  assert(!JSON.stringify(report).includes("secret"));
  assert(!JSON.stringify(report).includes("capability"));
  assert(!JSON.stringify(report).includes("private-output"));
});
