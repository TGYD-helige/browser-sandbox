import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";

const marker = "data:text/plain,probe-failure-cdp";
const queued = Promise.withResolvers();

export function observeProbeCommand(data) {
  const payload = String(data);
  if (!payload.includes(marker)) return;
  try {
    const message = JSON.parse(payload);
    if (message.method === "Target.createTarget" && message.params?.url === marker)
      queued.resolve();
  } catch {}
}

export async function rejectScreenshot(cdp, method, params, sessionId) {
  if (method !== "Page.captureScreenshot") return cdp.call(method, params, sessionId);
  const timer = new AbortController();
  try {
    assert(
      await Promise.race([
        queued.promise.then(() => true),
        delay(5000, false, { signal: timer.signal }),
      ]),
      "External probe command was not queued before renderer verification",
    );
  } finally {
    timer.abort();
  }
  const { targetInfos } = await cdp.call("Target.getTargets");
  assert(!targetInfos.some(({ url }) => url === marker));
  console.log(JSON.stringify({ event: "test_probe_cdp_target_absent" }));
  return cdp.call(method, { format: "invalid" }, sessionId);
}
