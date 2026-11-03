import assert from "node:assert/strict";
import { chromium } from "playwright-core";

const browser = await chromium.connectOverCDP(process.env.BROWSER_WS_ENDPOINT, {
  timeout: 60_000,
});
try {
  const context = browser.contexts()[0];
  const page = await context.newPage();
  await page.goto("data:text/html,<h1>Shared runtime</h1>");
  assert.equal(await page.locator("h1").textContent(), "Shared runtime");
  assert((await page.screenshot()).length > 100);
  console.log("Playwright CDP: navigation and screenshot passed.");
} finally {
  await browser.close();
}
