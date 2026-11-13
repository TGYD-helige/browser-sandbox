import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { createServer } from "node:http";
import { test } from "node:test";
import { createController } from "../dist/controller.js";
import { createControllerRuntime } from "../dist/network-control.js";
import { createBrowserSessions } from "../dist/runtime-provider.js";

test("managed sessions register without a UID, allocate lazily, proxy files, and revoke before cleanup", async (t) => {
  let created = 0,
    removed = 0;
  const worker = createServer((req, res) => res.end(req.url));
  await new Promise((resolve) => worker.listen(0, "127.0.0.1", resolve));
  t.after(() => new Promise((resolve) => worker.close(resolve)));
  const origin = `http://127.0.0.1:${worker.address().port}`;
  const controller = await createController({
    port: 0,
    backend: {
      async create(placement) {
        created++;
        return {
          ...placement,
          id: randomUUID(),
          origin,
          runtime: {
            request: async (request) =>
              request.op === "register"
                ? {
                    cdpUrl:
                      origin.replace("http", "ws") + "/execution/" + "a".repeat(64),
                    viewerToken: "b".repeat(64),
                  }
                : { stopped: true },
          },
        };
      },
      async remove() {
        removed++;
      },
    },
  });
  t.after(() => controller.stop());
  const runtime = createControllerRuntime(controller.origin);
  t.after(() => runtime.close());
  const sessions = createBrowserSessions({
    provider: { acquire: async () => runtime, release: async () => {} },
  });
  const session = await sessions.open();
  assert.equal(session.executorUid, undefined);
  assert.equal(created, 0);
  const fileUrl = session.wsEndpoint.replace("ws:", "http:") + "/files/example.txt";
  assert.equal(
    await (await fetch(fileUrl)).text(),
    "/execution/" + "a".repeat(64) + "/files/example.txt",
  );
  assert.equal(created, 1);
  await fetch(fileUrl);
  assert.equal(created, 1);
  await session.finalize();
  assert.equal(removed, 1);
  assert.equal((await fetch(fileUrl)).status, 403);
});
