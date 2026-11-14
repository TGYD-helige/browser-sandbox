import assert from "node:assert/strict";

// Retained browsers can have multiple tabs; select the same live target before capture.
export async function selectScreenshotTab(call) {
  const before = (await call("eval", "document.visibilityState")).result;
  const { tabs } = await call("tab");
  const active = tabs.find((tab) => tab.active);
  assert(active, "Retained browser has no selected tab");
  const selected = await call("tab", active.tabId);
  assert.equal(selected.targetId, active.targetId);
  assert.notEqual(selected.revived, true, "Tab selection reloaded the retained page");
  const after = (await call("eval", "document.visibilityState")).result;
  console.error(
    "managed_tab_selection " +
      JSON.stringify({
        before: ["visible", "hidden"].includes(before) ? before : "unknown",
        after: ["visible", "hidden"].includes(after) ? after : "unknown",
      }),
  );
}
