import assert from "node:assert/strict";
import { test } from "node:test";
import { selectScreenshotTab } from "./managed/screenshot-tab.mjs";

test("visual fixture selection rejects target replacement and page revival", async () => {
  for (const selected of [
    { targetId: "replacement" },
    { targetId: "original", revived: true },
  ]) {
    await assert.rejects(
      selectScreenshotTab(async (command, tabId) => {
        if (command === "eval") return { result: "hidden" };
        if (tabId) return selected;
        return { tabs: [{ tabId: "t1", targetId: "original", active: true }] };
      }),
    );
  }
});
