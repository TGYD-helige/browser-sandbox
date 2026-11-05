import assert from "node:assert/strict";
import { once } from "node:events";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { WebSocketServer } from "ws";
import { Observation } from "../dist/observation.js";

async function fixture(t, start) {
  const server = new WebSocketServer({ host: "127.0.0.1", port: 0 });
  await once(server, "listening");
  let starts = 0;
  const lifetime = new AbortController();
  server.on("connection", (socket) => {
    socket.on("message", (raw) => {
      const request = JSON.parse(String(raw));
      let result = {};
      if (request.method === "Target.getTargets")
        result = {
          targetInfos: [
            {
              type: "page",
              targetId: "page",
              title: "Login",
              url: "https://user:secret@example.com/login?token=secret#password",
            },
          ],
        };
      if (request.method === "Target.attachToTarget") result = { sessionId: "session" };
      if (request.method === "Page.startScreencast") {
        starts++;
        if (start?.(socket, request, starts) === false) return;
      }
      socket.send(JSON.stringify({ id: request.id, result }));
    });
  });
  t.after(async () => {
    lifetime.abort();
    for (const socket of server.clients) socket.terminate();
    await new Promise((resolve) => server.close(resolve));
  });
  const observation = new Observation(
    `ws://127.0.0.1:${server.address().port}`,
    lifetime.signal,
  );
  return {
    observation,
    server,
    get starts() {
      return starts;
    },
  };
}

async function until(predicate) {
  for (let i = 0; i < 100; i++) {
    if (predicate()) return;
    await delay(5);
  }
  assert.fail("Observation did not reach the expected state");
}

test("target metadata omits URL credentials, query and fragment", async (t) => {
  const { observation } = await fixture(t);
  const targets = await observation.targets(new AbortController().signal);
  assert.equal(targets[0].url, "https://example.com/login");
});

test("concurrent watchers share initialization failure and a later watch retries", async (t) => {
  let fail;
  const f = await fixture(t, (socket, request, starts) => {
    if (starts !== 1) return;
    fail = () =>
      socket.send(JSON.stringify({ id: request.id, error: { message: "failed" } }));
    return false;
  });
  const first = f.observation.watch("page", () => {}, new AbortController().signal);
  const firstResult = first.catch((error) => error);
  await until(() => fail);
  const second = f.observation.watch("page", () => {}, new AbortController().signal);
  const secondResult = second.catch((error) => error);
  fail();
  assert.match((await firstResult).message, /browser_cdp_operation_failed/);
  assert.match((await secondResult)?.message ?? "", /browser_cdp_operation_failed/);
  await f.observation.watch("page", () => {}, new AbortController().signal);
  assert.equal(f.starts, 2);
});

test("last watcher disconnect cancels pending initialization and permits retry", async (t) => {
  const f = await fixture(t, (_socket, _request, starts) => starts !== 1);
  const viewer = new AbortController();
  const watching = f.observation.watch("page", () => {}, viewer.signal);
  const result = watching.catch((error) => error);
  await until(() => f.starts === 1);
  viewer.abort();
  await until(() => f.server.clients.size === 0);
  assert((await result) instanceof Error);
  await f.observation.watch("page", () => {}, new AbortController().signal);
  assert.equal(f.starts, 2);
});

test("one viewer disconnect leaves shared initialization alive for another", async (t) => {
  let finish;
  const f = await fixture(t, (socket, request) => {
    finish = () => socket.send(JSON.stringify({ id: request.id, result: {} }));
    return false;
  });
  const firstViewer = new AbortController();
  const secondViewer = new AbortController();
  const first = f.observation.watch("page", () => {}, firstViewer.signal);
  const firstResult = first.catch((error) => error);
  await until(() => finish);
  const second = f.observation.watch("page", () => {}, secondViewer.signal);
  firstViewer.abort();
  assert((await firstResult) instanceof Error);
  assert.equal(f.server.clients.size, 1);
  finish();
  await second;
  assert.equal(f.starts, 1);
  secondViewer.abort();
  await until(() => f.server.clients.size === 0);
});
