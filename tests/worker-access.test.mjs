import assert from "node:assert/strict";
import test from "node:test";
import worker from "../adapters/cloudflare-worker/index.js";

test("Worker adapter routes only known access paths, preserves credentials and never retries", async (t) => {
  const calls = [];
  t.mock.method(globalThis, "fetch", async (request, options) => {
    calls.push({ request, options });
    return new Response("forwarded", { status: 200 });
  });
  const token = "a".repeat(64);
  const source = new Request(`https://viewer.example/execution/${token}/session`, {
    method: "POST",
    headers: { Origin: "https://viewer.example", "content-type": "application/json" },
    body: '{"action":"retain"}',
  });
  const result = await worker.fetch(source, {
    BROWSER_ORIGIN: "https://origin.example",
  });
  assert.equal(await result.text(), "forwarded");
  assert.equal(
    calls[0].request.url,
    `https://origin.example/execution/${token}/session`,
  );
  assert.equal(calls[0].request.headers.get("origin"), "https://viewer.example");
  assert.equal(await calls[0].request.text(), '{"action":"retain"}');
  assert.equal(calls[0].options.redirect, "manual");
  assert.equal(
    (
      await worker.fetch(new Request("https://viewer.example/admin"), {
        BROWSER_ORIGIN: "https://origin.example",
      })
    ).status,
    404,
  );
  assert.equal(
    (
      await worker.fetch(new Request("https://viewer.example/view"), {
        BROWSER_ORIGIN: "http://origin.example",
      })
    ).status,
    503,
  );
  assert.equal(calls.length, 1);
});

test("Worker adapter reports origin failure without replay", async (t) => {
  let calls = 0;
  t.mock.method(globalThis, "fetch", async () => {
    calls++;
    throw Error("private upstream diagnostic");
  });
  const result = await worker.fetch(new Request("https://viewer.example/view"), {
    BROWSER_ORIGIN: "https://origin.example",
  });
  assert.equal(result.status, 502);
  assert.equal(await result.text(), "Browser origin unavailable");
  assert.equal(calls, 1);
});

test("Worker forwards an awaited authentication request unchanged", async (t) => {
  let forwarded;
  t.mock.method(globalThis, "fetch", async (request) => {
    forwarded = request;
    return new Response('{"status":"submitted"}');
  });
  const path = `/execution/${"b".repeat(64)}/auth`;
  const body = JSON.stringify({
    targetId: "page",
    origin: "https://login.example",
    fields: [],
  });
  const result = await worker.fetch(
    new Request(`https://viewer.example${path}`, { method: "POST", body }),
    { BROWSER_ORIGIN: "https://origin.example" },
  );
  assert.equal(result.status, 200);
  assert.equal(forwarded.url, `https://origin.example${path}`);
  assert.equal(await forwarded.text(), body);
});
