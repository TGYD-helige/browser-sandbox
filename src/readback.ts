import { randomUUID } from "node:crypto";

/** Evaluate in a fresh isolated world, with bounded bytes and no redirect or service worker. */
export async function readBrowserJson(cdp, sessionId: string, url: string) {
  await cdp.call("Network.enable", {}, sessionId);
  await cdp.call("Network.setBypassServiceWorker", { bypass: true }, sessionId);
  await cdp.call("Network.setCacheDisabled", { cacheDisabled: true }, sessionId);
  try {
    const tree = await cdp.call("Page.getFrameTree", {}, sessionId);
    const generation = JSON.stringify(tree.frameTree.frame);
    const world = await cdp.call(
      "Page.createIsolatedWorld",
      {
        frameId: tree.frameTree.frame.id,
        worldName: `browser-readback-${randomUUID()}`,
      },
      sessionId,
    );
    const expression = `(async () => {
      const r = await fetch(${JSON.stringify(url)}, { method: 'GET', credentials: 'include', cache: 'no-store', redirect: 'error', signal: AbortSignal.timeout(10000) });
      if (!r.ok || r.url !== ${JSON.stringify(url)}) throw Error('read failed');
      const reader = r.body.getReader(), parts = []; let size = 0;
      try { for (;;) { const { done, value } = await reader.read(); if (done) break; size += value.length;
        if (size > 200000) throw Error('read too large'); parts.push(value); } }
      finally { await reader.cancel(); }
      const bytes = new Uint8Array(size); let offset = 0;
      for (const part of parts) { bytes.set(part, offset); offset += part.length; }
      return { text: new TextDecoder().decode(bytes), url: r.url };
    })()`;
    const result = await cdp.call(
      "Runtime.evaluate",
      {
        expression,
        contextId: world.executionContextId,
        awaitPromise: true,
        returnByValue: true,
      },
      sessionId,
    );
    const current = await cdp.call("Page.getFrameTree", {}, sessionId);
    const value = result.result?.value;
    if (
      generation !== JSON.stringify(current.frameTree.frame) ||
      result.exceptionDetails ||
      value?.url !== url ||
      typeof value?.text !== "string" ||
      Buffer.byteLength(value.text) > 200000
    )
      throw Error("browser_readback_failed");
    JSON.parse(value.text);
    return value;
  } finally {
    await cdp
      .call("Network.setBypassServiceWorker", { bypass: false }, sessionId)
      .catch(() => {});
    await cdp
      .call("Network.setCacheDisabled", { cacheDisabled: false }, sessionId)
      .catch(() => {});
  }
}
