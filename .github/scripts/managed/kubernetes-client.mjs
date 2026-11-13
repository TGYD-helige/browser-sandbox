import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { promisify } from "node:util";

const root = process.env.BROWSER_PACKAGE_ROOT ?? "/project";
const { connectCdp } = await import(`${root}/dist/cdp.js`);
const { createControllerRuntime, sessionRequest } = await import(
  `${root}/dist/index.js`
);
const runtime = createControllerRuntime(process.env.TEST_CONTROLLER_ORIGIN);
const executionId = process.env.TEST_EXECUTION_ID ?? randomUUID();
const action = process.env.TEST_ACTION ?? "functional";
let cdp;
let finalized = false;
const closeCdp = async () => {
  if (cdp) {
    cdp.close();
    await cdp.closed;
    cdp = undefined;
  }
};
async function connect(endpoint) {
  cdp = await connectCdp(endpoint);
  return cdp;
}
try {
  if (action === "lost") {
    await assert.rejects(runtime.request({ op: "session", executionId }), /browser_/);
    console.log(JSON.stringify({ workerLossReported: true }));
  } else {
    const { wsEndpoint } = await runtime.request({ op: "register", executionId });
    assert.equal((await sessionRequest(wsEndpoint)).state, "unused");
    const started = performance.now();
    await connect(wsEndpoint);
    await cdp.call("Browser.getVersion");
    const firstBrowserMs = performance.now() - started;
    const { targetId } = await cdp.call("Target.createTarget", { url: "about:blank" });
    const { sessionId } = await cdp.call("Target.attachToTarget", {
      targetId,
      flatten: true,
    });
    const evaluate = async (expression) =>
      (
        await cdp.call(
          "Runtime.evaluate",
          { expression, returnByValue: true, awaitPromise: true },
          sessionId,
        )
      ).result.value;
    await cdp.call("Page.enable", {}, sessionId);
    await cdp.call(
      "Page.navigate",
      {
        url: "data:text/html,<h1>Kubernetes managed browser</h1><input type=file><a download='download.txt'>Download</a>",
      },
      sessionId,
    );
    const deadline = Date.now() + 10000;
    while (!(await evaluate("!!document.querySelector('input')"))) {
      assert(Date.now() < deadline, "navigation did not commit");
      await delay(25);
    }
    const marker = randomUUID();
    assert.equal(await evaluate(`window.marker=${JSON.stringify(marker)}`), marker);
    const screenshot = await cdp.call("Page.captureScreenshot", {}, sessionId);
    assert(Buffer.from(screenshot.data, "base64").length > 100);
    await sessionRequest(wsEndpoint, "retain", 60000);
    const before = await sessionRequest(wsEndpoint);
    const renewed = await sessionRequest(wsEndpoint, "renew", 1800000);
    assert(renewed.expiresAt >= before.expiresAt);
    assert(renewed.expiresAt <= renewed.hardExpiresAt);
    await closeCdp();
    await connect(wsEndpoint);
    assert(
      (await cdp.call("Target.getTargets")).targetInfos.some(
        (target) => target.targetId === targetId,
      ),
    );
    const reattached = await cdp.call("Target.attachToTarget", {
      targetId,
      flatten: true,
    });
    assert.equal(
      (
        await cdp.call(
          "Runtime.evaluate",
          { expression: "window.marker", returnByValue: true },
          reattached.sessionId,
        )
      ).result.value,
      marker,
    );
    if (action === "hold") {
      console.log(
        JSON.stringify({
          executionId,
          targetId,
          firstBrowserMs,
          architecture: process.arch,
        }),
      );
    } else {
      const base = wsEndpoint.replace(/^ws/, "http") + "/files/";
      const upload = await fetch(base + "upload.txt", {
        method: "PUT",
        body: "managed upload",
      });
      assert.equal(upload.status, 201);
      const { path } = await upload.json();
      const { root: document } = await cdp.call(
        "DOM.getDocument",
        {},
        reattached.sessionId,
      );
      const { nodeId } = await cdp.call(
        "DOM.querySelector",
        { nodeId: document.nodeId, selector: "input" },
        reattached.sessionId,
      );
      await cdp.call(
        "DOM.setFileInputFiles",
        { nodeId, files: [path] },
        reattached.sessionId,
      );
      assert.equal(
        (
          await cdp.call(
            "Runtime.evaluate",
            {
              expression: "document.querySelector('input').files[0].text()",
              awaitPromise: true,
              returnByValue: true,
            },
            reattached.sessionId,
          )
        ).result.value,
        "managed upload",
      );
      await cdp.call(
        "Runtime.evaluate",
        {
          expression:
            "const a=document.querySelector('a');a.href=URL.createObjectURL(new Blob(['managed download']));a.click()",
          userGesture: true,
        },
        reattached.sessionId,
      );
      const downloadDeadline = Date.now() + 10000;
      while (true) {
        const download = await fetch(base + "download.txt");
        if (download.ok) {
          assert.equal(await download.text(), "managed download");
          break;
        }
        assert.equal(download.status, 404);
        assert(Date.now() < downloadDeadline, "download did not finish");
        await delay(50);
      }
      const grant = await runtime.request({
        op: "access_create",
        executionId,
        durationMs: 10000,
      });
      await closeCdp();
      await connect(grant.cdpUrl);
      assert(
        (await cdp.call("Target.getTargets")).targetInfos.some(
          (target) => target.targetId === targetId,
        ),
      );
      await runtime.request({ op: "access_revoke", executionId });
      await closeCdp();
      await assert.rejects(connectCdp(grant.cdpUrl), /browser_cdp_/);
      const fresh = await runtime.request({ op: "endpoint", executionId });
      await connect(fresh.wsEndpoint);
      const { targetId: sandboxTarget } = await cdp.call("Target.createTarget", {
        url: "chrome://sandbox",
      });
      const { sessionId: sandboxSession } = await cdp.call("Target.attachToTarget", {
        targetId: sandboxTarget,
        flatten: true,
      });
      const sandboxDeadline = Date.now() + 10000;
      while (true) {
        const text =
          (
            await cdp.call(
              "Runtime.evaluate",
              { expression: "document.body?.innerText", returnByValue: true },
              sandboxSession,
            )
          ).result.value ?? "";
        if (/Seccomp-BPF sandbox\s+Yes/.test(text)) break;
        assert(Date.now() < sandboxDeadline, "renderer seccomp sandbox is not enabled");
        await delay(50);
      }
      await cdp.call("Target.closeTarget", { targetId: sandboxTarget });
      await closeCdp();
      // Reuse the existing agent-browser harness package through the controller URL.
      const home = await mkdtemp(join(tmpdir(), "managed-agent-"));
      const cli =
        process.env.TEST_AGENT_BROWSER_CLI ??
        "/project/.github/scripts/e2e/node_modules/agent-browser/bin/agent-browser.js";
      const cliSession = `managed-${randomUUID().slice(0, 8)}`;
      const call = async (...args) =>
        JSON.parse(
          (
            await promisify(execFile)(
              process.execPath,
              [
                cli,
                "--json",
                "--session",
                cliSession,
                "--cdp",
                fresh.wsEndpoint,
                ...args,
              ],
              {
                env: { ...process.env, AGENT_BROWSER_SOCKET_DIR: home },
                timeout: 45000,
                maxBuffer: 1048576,
              },
            )
          ).stdout,
        );
      try {
        assert((await call("open", "data:text/html,<h1>Managed harness</h1>")).success);
        assert((await call("snapshot")).success);
        assert((await call("screenshot", join(home, "page.png"))).success);
        assert((await stat(join(home, "page.png"))).size > 100);
      } finally {
        await call("close");
        await rm(home, { recursive: true, force: true });
      }
      const finalStarted = performance.now();
      await runtime.request({ op: "finalize", executionId });
      finalized = true;
      console.log(
        JSON.stringify({
          functional: true,
          firstBrowserMs,
          finalizationMs: performance.now() - finalStarted,
          architecture: process.arch,
        }),
      );
    }
  }
} finally {
  await closeCdp();
  if (action === "functional" && !finalized)
    await runtime.request({ op: "finalize", executionId }).catch(() => {});
  runtime.close();
}
