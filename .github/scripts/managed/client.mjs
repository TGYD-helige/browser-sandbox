import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { connectCdp } from "./dist/cdp.js";
import { createControllerRuntime, sessionRequest } from "./dist/index.js";

const runtime = createControllerRuntime("http://controller:8080");
const executionId = randomUUID();
try {
  const { wsEndpoint } = await runtime.request({ op: "register", executionId });
  assert.equal((await sessionRequest(wsEndpoint)).state, "unused");
  const started = performance.now();
  const cdp = await connectCdp(wsEndpoint);
  try {
    await cdp.call("Browser.getVersion");
    console.log(
      JSON.stringify({
        firstBrowserMs: performance.now() - started,
        architecture: process.arch,
      }),
    );
  } finally {
    cdp.close();
    await cdp.closed;
  }
} finally {
  await runtime.request({ op: "finalize", executionId });
  runtime.close();
}
