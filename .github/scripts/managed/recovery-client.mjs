import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { connectCdp } from "./dist/cdp.js";
import { createControllerRuntime, sessionRequest } from "./dist/index.js";

const origin = process.env.CONTROLLER_ORIGIN ?? "http://controller:8080";
const stateFile = "/controller-data/recovery-test.json";
const runtime = createControllerRuntime(origin);
const phase = process.argv[2];
try {
  if (phase === "prepare") {
    const executionId = randomUUID();
    const { wsEndpoint } = await runtime.request({ op: "register", executionId });
    const started = performance.now();
    const cdp = await connectCdp(wsEndpoint);
    try {
      const { targetId } = await cdp.call("Target.createTarget", {
        url: "about:blank",
      });
      const { sessionId } = await cdp.call("Target.attachToTarget", {
        targetId,
        flatten: true,
      });
      await cdp.call(
        "Runtime.evaluate",
        {
          expression:
            "document.title='recovery-original';globalThis.recoveryMarker='original-page'",
        },
        sessionId,
      );
      const status = await sessionRequest(wsEndpoint, "retain", 120000);
      await writeFile(
        stateFile,
        JSON.stringify({
          executionId,
          wsEndpoint,
          targetId,
          generation: status.generation,
        }),
        { mode: 0o600 },
      );
      console.log(
        JSON.stringify({
          phase,
          firstBrowserMs: performance.now() - started,
          architecture: process.arch,
        }),
      );
    } finally {
      cdp.close();
      await cdp.closed;
    }
  } else {
    const state = JSON.parse(await readFile(stateFile, "utf8"));
    if (phase === "replay") {
      await assert.rejects(
        runtime.request({ op: "register", executionId: state.executionId }),
        /browser_execution_revoked/,
      );
      console.log(JSON.stringify({ phase, fenced: true }));
    } else if (phase === "resume") {
      const before = await sessionRequest(state.wsEndpoint);
      assert.equal(before.generation, state.generation);
      assert.equal(before.state, "retained");
      const renewed = await sessionRequest(state.wsEndpoint, "renew", 120000);
      assert(renewed.expiresAt >= before.expiresAt);
      assert(renewed.expiresAt <= renewed.hardExpiresAt);
      const cdp = await connectCdp(state.wsEndpoint);
      try {
        const { targetInfos } = await cdp.call("Target.getTargets");
        assert(targetInfos.some((target) => target.targetId === state.targetId));
        const { sessionId } = await cdp.call("Target.attachToTarget", {
          targetId: state.targetId,
          flatten: true,
        });
        const result = await cdp.call(
          "Runtime.evaluate",
          { expression: "globalThis.recoveryMarker" },
          sessionId,
        );
        assert.equal(result.result.value, "original-page");
      } finally {
        cdp.close();
        await cdp.closed;
      }
      await runtime.request({ op: "finalize", executionId: state.executionId });
      console.log(JSON.stringify({ phase, samePage: true, sameGeneration: true }));
    } else throw Error("unknown_recovery_test_phase");
  }
} finally {
  runtime.close();
}
