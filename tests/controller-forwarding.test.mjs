import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer, request } from "node:http";
import { test } from "node:test";
import {
  createHostForwarding,
  forwardControllerRequest,
} from "../dist/controller-forwarding.js";
import { createNetworkHostControl } from "../dist/network-control.js";

test("concurrent forwarded host commands share the pending WebSocket handshake", async () => {
  const server = createServer();
  const host = createNetworkHostControl({
    server,
    dispatch: async (request) => ({ executionId: request.executionId }),
  });
  const forwarding = createHostForwarding();
  const life = new AbortController();
  try {
    await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
    const origin = `http://127.0.0.1:${server.address().port}`;
    const peer = { signal: life.signal, callback: async () => {} };
    const responses = await Promise.all(
      ["a", "b"].map((executionId) =>
        forwarding.request(origin, { executionId }, peer),
      ),
    );
    assert.deepEqual(responses, [{ executionId: "a" }, { executionId: "b" }]);
  } finally {
    life.abort();
    forwarding.close();
    host.close();
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
  }
});

async function proxyFixture(t, origin, observe = () => {}) {
  const server = createServer((req, res) => {
    observe(res);
    forwardControllerRequest(req, res, origin);
  });
  server.on("upgrade", (req, socket, head) =>
    forwardControllerRequest(req, socket, origin, head),
  );
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(
    () =>
      new Promise((resolve) => {
        server.closeAllConnections();
        server.close(resolve);
      }),
  );
  return `http://127.0.0.1:${server.address().port}`;
}

test("a failed WebSocket owner connection responds with retryable HTTP 503", {
  timeout: 2000,
}, async (t) => {
  const unused = createServer();
  await new Promise((resolve) => unused.listen(0, "127.0.0.1", resolve));
  const origin = `http://127.0.0.1:${unused.address().port}`;
  await new Promise((resolve) => unused.close(resolve));
  const proxy = await proxyFixture(t, origin);
  const status = await new Promise((resolve, reject) => {
    const req = request(proxy, {
      headers: { Connection: "Upgrade", Upgrade: "websocket" },
    });
    req.once("response", (res) => {
      res.resume();
      resolve(res.statusCode);
    });
    req.once("error", reject);
    req.end();
  });
  assert.equal(status, 503);
});

test("WebSocket forwarding preserves repeated raw 101 response headers", {
  timeout: 2000,
}, async (t) => {
  const owner = createServer();
  const sockets = new Set();
  owner.on("upgrade", (_req, socket) => {
    sockets.add(socket);
    socket.write(
      "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: test\r\nSet-Cookie: a=1\r\nSet-Cookie: b=2\r\n\r\n",
    );
  });
  await new Promise((resolve) => owner.listen(0, "127.0.0.1", resolve));
  t.after(
    () =>
      new Promise((resolve) => {
        for (const socket of sockets) socket.destroy();
        owner.close(resolve);
      }),
  );
  const proxy = await proxyFixture(t, `http://127.0.0.1:${owner.address().port}`);
  const headers = await new Promise((resolve, reject) => {
    const req = request(proxy, { headers: { Connection: "Upgrade", Upgrade: "test" } });
    req.once("upgrade", (res, socket) => {
      socket.destroy();
      resolve(res.rawHeaders);
    });
    req.once("error", reject);
    req.end();
  });
  const cookies = headers.flatMap((name, i) =>
    i % 2 === 0 && name.toLowerCase() === "set-cookie" ? [headers[i + 1]] : [],
  );
  assert.deepEqual(cookies, ["a=1", "b=2"]);
});

for (const auth of [false, true])
  test(`streaming forwarding has an end-to-end ${auth ? "pathname auth" : "request"} deadline`, {
    timeout: 2000,
  }, async (t) => {
    const owner = createServer((_req, res) => {
      res.writeHead(200);
      res.write("start");
      const timer = setInterval(() => res.write("still active"), 10);
      res.once("close", () => clearInterval(timer));
    });
    await new Promise((resolve) => owner.listen(0, "127.0.0.1", resolve));
    t.after(
      () =>
        new Promise((resolve) => {
          owner.closeAllConnections();
          owner.close(resolve);
        }),
    );
    let destination;
    const proxy = await proxyFixture(
      t,
      `http://127.0.0.1:${owner.address().port}`,
      (response) => {
        destination = response;
      },
    );
    t.mock.timers.enable({ apis: ["setTimeout"] });
    const path = `/execution/11111111-1111-4111-8111-111111111111/${"a".repeat(64)}/${auth ? "auth?wait=1" : "files"}`;
    const res = await new Promise((resolve, reject) => {
      const req = request(proxy + path, resolve);
      req.once("error", reject);
      req.end();
    });
    res.on("error", () => {});
    await once(res, "data");
    const closed = new Promise((resolve) => res.once("close", resolve));
    if (auth) {
      t.mock.timers.tick(180000);
      await new Promise(setImmediate);
      assert.equal(destination.destroyed, false);
      t.mock.timers.tick(440000);
    } else t.mock.timers.tick(180000);
    await closed;
    assert.equal(res.complete, false);
  });

test("non-101 WebSocket responses retain valid HTTP body framing", async (t) => {
  const owner = createServer((_req, res) =>
    res
      .writeHead(503, { "content-type": "application/json" })
      .end('{"error":"browser_controller_owner_moved"}'),
  );
  await new Promise((resolve) => owner.listen(0, "127.0.0.1", resolve));
  t.after(
    () =>
      new Promise((resolve) => {
        owner.closeAllConnections();
        owner.close(resolve);
      }),
  );
  const proxy = await proxyFixture(t, `http://127.0.0.1:${owner.address().port}`);
  const body = await new Promise((resolve, reject) => {
    const req = request(proxy, {
      headers: { Connection: "Upgrade", Upgrade: "websocket" },
    });
    req.once("error", reject);
    req.once("response", (res) => {
      let text = "";
      res.on("data", (chunk) => (text += chunk));
      res.once("error", reject);
      res.once("end", () => resolve(text));
    });
    req.end();
  });
  assert.equal(JSON.parse(body).error, "browser_controller_owner_moved");
});

test("a raw upgrade response deadline never appends a second HTTP response inside its body", {
  timeout: 2000,
}, async (t) => {
  const owner = createServer((_req, res) => {
    res.writeHead(503, { "content-type": "application/json" });
    res.write("partial-body");
  });
  await new Promise((resolve) => owner.listen(0, "127.0.0.1", resolve));
  t.after(
    () =>
      new Promise((resolve) => {
        owner.closeAllConnections();
        owner.close(resolve);
      }),
  );
  const proxy = await proxyFixture(t, `http://127.0.0.1:${owner.address().port}`);
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const res = await new Promise((resolve, reject) => {
    const req = request(
      proxy,
      { headers: { Connection: "Upgrade", Upgrade: "websocket" } },
      resolve,
    );
    req.once("error", reject);
    req.end();
  });
  let body = "";
  res.on("data", (chunk) => (body += chunk));
  res.on("error", () => {});
  await once(res, "data");
  const closed = new Promise((resolve) => res.once("close", resolve));
  t.mock.timers.tick(180000);
  await closed;
  assert.equal(body, "partial-body");
});
