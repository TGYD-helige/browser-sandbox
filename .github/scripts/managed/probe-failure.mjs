import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";

export async function rejectScreenshot(cdp, method, params, sessionId) {
  if (method !== "Page.captureScreenshot") return cdp.call(method, params, sessionId);
  // Give the client commands time to arrive while admission is held.
  await delay(250);
  const { targetInfos } = await cdp.call("Target.getTargets");
  assert(!targetInfos.some(({ url }) => url === "data:text/plain,probe-failure-cdp"));
  console.log(JSON.stringify({ event: "test_probe_cdp_target_absent" }));
  return cdp.call(method, { ...params, format: "invalid" }, sessionId);
}
