import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import { connectCdp } from "../dist/cdp.js";

const cdp = await connectCdp(process.env.BROWSER_WS_ENDPOINT);
try {
  const { targetId } = await cdp.call("Target.createTarget", {
    url: "data:text/html,<h1>browser-sandbox</h1>",
  });
  const { sessionId } = await cdp.call("Target.attachToTarget", {
    targetId,
    flatten: true,
  });
  let result;
  const deadline = Date.now() + 15_000;
  do {
    result = await cdp.call(
      "Runtime.evaluate",
      {
        expression: 'document.querySelector("h1")?.textContent',
        returnByValue: true,
      },
      sessionId,
    );
    if (result.result.value === "browser-sandbox") break;
    await delay(25);
  } while (Date.now() < deadline);
  assert.equal(result.result.value, "browser-sandbox");
  const { data } = await cdp.call("Page.captureScreenshot", {}, sessionId);
  assert(Buffer.from(data, "base64").length > 100);
  console.log("Raw CDP: navigation, page inspection, and screenshot passed.");
} finally {
  cdp.close();
}
