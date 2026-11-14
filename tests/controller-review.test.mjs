import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { createServer } from "node:http";
import { test } from "node:test";
import { setImmediate as nextTurn } from "node:timers/promises";
import WebSocket from "ws";
import { createController } from "../dist/controller.js";
import { createControllerRuntime } from "../dist/network-control.js";

function barrier() {
  let resolve;
  const promise = new Promise((r) => {
    resolve = r;
  });
  return { promise, resolve };
}
async function fixture(t, options = {}) {
  const worker = createServer((req, res) => {
    req.on("error", () => {});
    if (options.workerResponse) return options.workerResponse(req, res);
    req.resume();
    req.on("end", () => res.end("fixture"));
  });
  await new Promise((resolve) => worker.listen(0, "127.0.0.1", resolve));
  const origin = `http://127.0.0.1:${worker.address().port}`;
  const makeWorker = (placement) => ({
    ...placement,
    id: randomUUID(),
    origin,
    runtime: {
      request: async (request) =>
        request.op === "register"
          ? {
              cdpUrl: origin.replace("http:", "ws:") + "/execution/" + "a".repeat(64),
              viewerToken: "b".repeat(64),
            }
          : { stopped: true },
      close() {},
    },
  });
  const {
    create = async (placement) => makeWorker(placement),
    remove = async () => {},
    cleanupMayFail = false,
    workerResponse: _workerResponse,
    ...config
  } = options;
  const controller = await createController({
    port: 0,
    controlPort: 0,
    backend: {
      create: (placement, signal) => create(placement, signal, makeWorker),
      remove,
    },
    ...config,
  });
  t.after(async () => {
    try {
      await controller.stop();
    } catch (error) {
      if (!cleanupMayFail) throw error;
    } finally {
      await new Promise((resolve) => worker.close(resolve));
    }
  });
  const executionId = randomUUID();
  const { wsEndpoint } = await controller.command({ op: "register", executionId });
  return {
    controller,
    executionId,
    url: wsEndpoint.replace("ws:", "http:") + "/files/test.txt",
  };
}

test("only the separate host listener accepts host control and retains its authentication hook", async (t) => {
  let authenticated = 0;
  const { controller } = await fixture(t, {
    authenticateHost: async () => {
      authenticated++;
    },
  });
  const agent = createControllerRuntime(controller.origin);
  t.after(() => agent.close());
  await assert.rejects(
    agent.request({ op: "register", executionId: randomUUID() }),
    /browser_control_/,
  );
  assert.equal(authenticated, 0);
  const host = createControllerRuntime(controller.controlOrigin);
  t.after(() => host.close());
  const endpoint = await host.request({ op: "register", executionId: randomUUID() });
  assert(endpoint.wsEndpoint.startsWith(controller.origin.replace("http:", "ws:")));
  assert.equal(authenticated, 1);
});

test("one cancelled waiter cannot cancel another browser creation waiter", async (t) => {
  const entered = barrier(),
    release = barrier(),
    secondEntered = barrier();
  let creationSignal,
    count = 0,
    requests = 0;
  let firstSocket;
  const f = await fixture(t, {
    authorizeAccess: async (_id, request) => {
      if (++requests === 1) firstSocket = request.socket;
      if (requests === 2) secondEntered.resolve();
    },
    create: async (placement, signal, makeWorker) => {
      count++;
      creationSignal = signal;
      entered.resolve();
      await release.promise;
      return makeWorker(placement);
    },
  });
  const firstLife = new AbortController();
  const first = fetch(f.url, { signal: firstLife.signal });
  const rejected = assert.rejects(first);
  await entered.promise;
  const second = fetch(f.url);
  await secondEntered.promise;
  await nextTurn();
  const disconnected = once(firstSocket, "close");
  firstLife.abort();
  firstSocket.destroy();
  await rejected;
  await disconnected;
  await nextTurn();
  try {
    assert.equal(creationSignal.aborted, false);
  } finally {
    release.resolve();
  }
  assert.equal((await second).status, 200);
  assert.equal(count, 1);
});

test("the last cancelled waiter cancels creation and a later request retries", async (t) => {
  const entered = barrier(),
    cancelled = barrier();
  let count = 0;
  const f = await fixture(t, {
    create: async (placement, signal, makeWorker) => {
      if (++count > 1) return makeWorker(placement);
      entered.resolve();
      await new Promise((_, reject) =>
        signal.addEventListener(
          "abort",
          () => {
            cancelled.resolve();
            reject(Error("browser_request_cancelled"));
          },
          { once: true },
        ),
      );
    },
  });
  const life = new AbortController();
  const first = fetch(f.url, { signal: life.signal });
  const rejected = assert.rejects(first);
  await entered.promise;
  life.abort();
  await rejected;
  await cancelled.promise;
  await nextTurn();
  assert.equal((await fetch(f.url)).status, 200);
  assert.equal(count, 2);
});

test("a creation failure is retryable after its attempt has settled", async (t) => {
  let count = 0;
  const f = await fixture(t, {
    create: async (placement, _signal, makeWorker) => {
      if (++count === 1) throw Error("browser_worker_start_failed");
      return makeWorker(placement);
    },
  });
  assert.equal((await fetch(f.url)).status, 502);
  assert.equal((await fetch(f.url)).status, 200);
  assert.equal(count, 2);
});

test("cleanup failure preserves registration failure and closes the worker channel", async (t) => {
  let closed = 0;
  const original = Error("browser_registration_failed"),
    cleanup = Error("browser_docker_unavailable");
  const f = await fixture(t, {
    cleanupMayFail: true,
    create: async (placement, _signal, makeWorker) => {
      const worker = makeWorker(placement);
      worker.runtime.request = async () => {
        throw original;
      };
      worker.runtime.close = () => {
        closed++;
      };
      return worker;
    },
    remove: async () => {
      throw cleanup;
    },
  });
  const response = await fetch(f.url);
  const body = await response.text();
  assert.equal(response.status, 502);
  assert.match(body, /browser_registration_failed/);
  assert.match(body, /browser_docker_unavailable/);
  assert.equal(closed, 1);
});

test("finalization cleanup failure preserves the original failure and closes the worker channel", async (t) => {
  let closed = 0;
  const original = Error("browser_control_disconnected"),
    cleanup = Error("browser_docker_unavailable");
  const f = await fixture(t, {
    cleanupMayFail: true,
    create: async (placement, _signal, makeWorker) => {
      const worker = makeWorker(placement);
      const request = worker.runtime.request;
      worker.runtime.request = async (input) => {
        if (input.op === "finalize") throw original;
        return request(input);
      };
      worker.runtime.close = () => {
        closed++;
      };
      return worker;
    },
    remove: async () => {
      throw cleanup;
    },
  });
  await fetch(f.url);
  await assert.rejects(
    f.controller.command({ op: "finalize", executionId: f.executionId }),
    (error) => {
      assert(error instanceof AggregateError);
      assert.equal(error.errors[0], original);
      assert.match(error.message, /browser_control_disconnected/);
      assert.match(error.message, /browser_docker_unavailable/);
      return true;
    },
  );
  assert.equal(closed, 1);
});

test("finished identity retention evicts oldest identities instead of exhausting lifetime registration", async (t) => {
  const { controller, executionId } = await fixture(t);
  await controller.command({ op: "finalize", executionId });
  let last;
  for (let index = 0; index < 65536; index++) {
    last = randomUUID();
    await controller.command({ op: "register", executionId: last });
    await controller.command({ op: "finalize", executionId: last });
  }
  await assert.rejects(
    controller.command({ op: "register", executionId: last }),
    /browser_execution_revoked/,
  );
  assert((await controller.command({ op: "register", executionId })).wsEndpoint);
});

test("invalid session JSON is rejected as a client error without allocating a worker", async (t) => {
  const { url } = await fixture(t, {
    create: async () => assert.fail("invalid session allocated worker"),
  });
  assert.equal(
    (
      await fetch(url.replace("/files/test.txt", "/session"), {
        method: "POST",
        body: "{",
      })
    ).status,
    400,
  );
});

test("files reject an oversized declared body before worker allocation", async (t) => {
  const { url } = await fixture(t, {
    create: async () => assert.fail("oversized body allocated worker"),
  });
  // The actual streamed limit is exercised separately; this header can be rejected before upload.
  const { request } = await import("node:http");
  const status = await new Promise((resolve, reject) => {
    const req = request(
      url,
      { method: "PUT", headers: { "content-length": String(16 * 1024 * 1024 + 1) } },
      (res) => {
        res.resume();
        resolve(res.statusCode);
      },
    );
    req.on("error", reject);
    req.flushHeaders();
  });
  assert.equal(status, 413);
});

test("files reject a chunked body over the shared file limit", async (t) => {
  const { url } = await fixture(t);
  const { Readable } = await import("node:stream");
  const response = await fetch(url, {
    method: "PUT",
    duplex: "half",
    body: Readable.from(
      (async function* () {
        for (let i = 0; i < 257; i++) yield Buffer.alloc(65536);
      })(),
    ),
  });
  assert.equal(response.status, 413);
});

test("an oversized file body after response headers only terminates that transfer", async (t) => {
  const { url, controller } = await fixture(t, {
    workerResponse: (req, res) => {
      req.resume();
      res.writeHead(200);
      res.write("started");
      req.on("end", () => res.end("complete"));
    },
  });
  const { request } = await import("node:http");
  await new Promise((resolve, reject) => {
    const req = request(
      url,
      { method: "GET", headers: { "transfer-encoding": "chunked" } },
      (res) => {
        res.once("data", () => req.end(Buffer.alloc(16 * 1024 * 1024 + 1)));
        res.once("aborted", resolve);
        res.once("error", () => {});
        res.once("end", () => reject(Error("oversized transfer completed")));
      },
    );
    req.on("error", () => {});
    req.write("prefix");
  });
  assert.equal((await fetch(controller.origin + "/health")).status, 204);
});

test("unconfirmed creation cleanup fences replacement and cannot finalize as an unused execution", async (t) => {
  const { WorkerCleanupFailure } = await import("../dist/lifecycle-errors.js");
  let count = 0;
  const failure = new WorkerCleanupFailure(
    Error("browser_worker_start_failed"),
    Error("browser_docker_unavailable"),
  );
  const f = await fixture(t, {
    cleanupMayFail: true,
    create: async () => {
      count++;
      throw failure;
    },
  });
  assert.equal((await fetch(f.url)).status, 502);
  assert.equal((await fetch(f.url)).status, 502);
  assert.equal(count, 1);
  await assert.rejects(
    f.controller.command({ op: "finalize", executionId: f.executionId }),
    (error) => error === failure,
  );
  assert.equal(f.controller.bindings.size, 1);
});

async function viewer(t, controller, token) {
  const client = new WebSocket(
    controller.origin.replace("http:", "ws:") + "/view/socket",
  );
  client.on("error", () => {});
  t.after(() => client.terminate());
  await once(client, "open");
  if (token) client.send(JSON.stringify({ type: "authenticate", token }));
  return client;
}

for (const [flags, limit] of [
  [{ control: true }, 1],
  [{ resize: true }, 1],
  [{}, 4],
]) {
  test(`managed viewer reserves ${limit} slot(s) per ${JSON.stringify(flags)} grant before worker readiness`, async (t) => {
    const entered = barrier(),
      release = barrier(),
      authorized = barrier();
    let count = 0;
    t.after(() => release.resolve());
    const f = await fixture(t, {
      authorizeAccess: async () => {
        if (++count === limit + 1) authorized.resolve();
      },
      create: async (placement, _signal, makeWorker) => {
        entered.resolve();
        await release.promise;
        return makeWorker(placement);
      },
    });
    const grant = await f.controller.command({
      op: "access_create",
      executionId: f.executionId,
      ...flags,
    });
    const token = new URL(grant.viewerUrl).hash.slice(1);
    const first = await viewer(t, f.controller, token);
    await entered.promise;
    for (let i = 1; i < limit; i++) await viewer(t, f.controller, token);
    const excess = await viewer(t, f.controller);
    const closed = once(excess, "close", { signal: AbortSignal.timeout(1000) });
    excess.send(JSON.stringify({ type: "authenticate", token }));
    await authorized.promise;
    const [code, reason] = await closed;
    assert.equal(code, 1008);
    assert.equal(String(reason), "browser_access_denied");
    assert.equal(first.readyState, WebSocket.OPEN);
    release.resolve();
  });
}

for (const authenticated of [false, true]) {
  test(`managed viewer bounds ${authenticated ? "authenticated" : "waiting"} sockets to 64`, async (t) => {
    const release = barrier(),
      admitted = barrier();
    t.after(() => release.resolve());
    let count = 0;
    const f = await fixture(t, {
      authorizeAccess: async () => {
        if (++count === 64) admitted.resolve();
      },
      create: async (placement, _signal, makeWorker) => {
        await release.promise;
        return makeWorker(placement);
      },
    });
    const clients = [];
    for (let i = 0; i < 64; i++) {
      let token;
      if (authenticated) {
        const grant = await f.controller.command({
          op: "access_create",
          executionId: f.executionId,
        });
        token = new URL(grant.viewerUrl).hash.slice(1);
      }
      clients.push(await viewer(t, f.controller, token));
    }
    if (authenticated) await admitted.promise;
    const excess = new WebSocket(
      f.controller.origin.replace("http:", "ws:") + "/view/socket",
    );
    excess.on("error", () => {});
    t.after(() => excess.terminate());
    const status = await new Promise((resolve) => {
      excess.once("open", () => resolve(101));
      excess.once("unexpected-response", (_req, response) => {
        response.resume();
        resolve(response.statusCode);
      });
    });
    assert.equal(status, 403);
    const closed = once(clients[0], "close");
    clients[0].terminate();
    await closed;
    await nextTurn();
    await viewer(t, f.controller);
  });
}

test("new access waits for an already-cancelled creation to settle before retrying", async (t) => {
  const entered = barrier(),
    cancelled = barrier(),
    release = barrier(),
    second = barrier();
  t.after(() => release.resolve());
  let count = 0,
    requests = 0,
    firstSocket;
  const f = await fixture(t, {
    authorizeAccess: async (_id, req) => {
      if (++requests === 1) firstSocket = req.socket;
      else second.resolve();
    },
    create: async (placement, signal, makeWorker) => {
      if (++count > 1) return makeWorker(placement);
      entered.resolve();
      await once(signal, "abort");
      cancelled.resolve();
      await release.promise;
      throw Error("browser_creation_cancelled");
    },
  });
  const first = assert.rejects(fetch(f.url));
  await entered.promise;
  firstSocket.destroy();
  await first;
  await cancelled.promise;
  const replacement = fetch(f.url);
  await second.promise;
  await nextTurn();
  release.resolve();
  assert.equal((await replacement).status, 200);
  assert.equal(count, 2);
});

for (const op of ["session", "storage_attach"]) {
  test(`${op} cancels its own wait without cancelling shared browser creation`, {
    timeout: 2000,
  }, async (t) => {
    const entered = barrier(),
      release = barrier();
    t.after(() => release.resolve());
    let creationSignal;
    const f = await fixture(t, {
      create: async (placement, signal, makeWorker) => {
        creationSignal = signal;
        entered.resolve();
        await release.promise;
        return makeWorker(placement);
      },
    });
    const browser = fetch(f.url);
    await entered.promise;
    const life = new AbortController();
    const pending = f.controller.command(
      { op, executionId: f.executionId, action: "status" },
      { signal: new AbortController().signal },
      life.signal,
    );
    const rejected = assert.rejects(pending, /browser_control_timeout/);
    life.abort();
    await rejected;
    assert.equal(creationSignal.aborted, false);
    release.resolve();
    assert.equal((await browser).status, 200);
  });
}

test("HTTP access denial is 403 on session, files, and auth without leaking hook errors", async (t) => {
  const f = await fixture(t, {
    authorizeAccess: async () => {
      throw Error("private identity details");
    },
  });
  for (const route of ["/session", "/files/test.txt", "/auth"]) {
    const response = await fetch(f.url.replace("/files/test.txt", route));
    assert.equal(response.status, 403);
    assert.deepEqual(await response.json(), { error: "browser_access_denied" });
  }
});

async function rejectedUpgrade(url) {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(url);
    socket.on("error", () => {});
    socket.once("open", () => {
      socket.terminate();
      reject(Error("Unexpected upgrade"));
    });
    socket.once("unexpected-response", async (_req, response) => {
      let body = "";
      for await (const chunk of response) body += chunk;
      resolve({ status: response.statusCode, body });
    });
  });
}

for (const [message, status, code] of [
  ["browser_worker_start_failed", 502, "browser_worker_start_failed"],
  ["browser_execution_capacity_exceeded", 503, "browser_execution_capacity_exceeded"],
  ["private Docker credentials", 502, "browser_worker_unavailable"],
]) {
  test(`HTTP and CDP upgrade preserve sanitized creation outcome ${code}`, async (t) => {
    const f = await fixture(t, {
      create: async () => {
        throw Error(message);
      },
    });
    const response = await fetch(f.url);
    assert.equal(response.status, status);
    assert.deepEqual(await response.json(), { error: code });
    const upgrade = await rejectedUpgrade(
      f.url.replace("http:", "ws:").replace("/files/test.txt", ""),
    );
    assert.equal(upgrade.status, status);
    assert.deepEqual(JSON.parse(upgrade.body), { error: code });
  });
}

test("finalization cancels storage attachment during creation", {
  timeout: 2000,
}, async (t) => {
  const entered = barrier(),
    cancelled = barrier();
  let removed = 0;
  const f = await fixture(t, {
    create: async (placement, _signal, makeWorker) => {
      const worker = makeWorker(placement);
      worker.runtime.attachStorage = async (_registration, _adapter, signal) => {
        assert(signal instanceof AbortSignal);
        entered.resolve();
        await once(signal, "abort");
        cancelled.resolve();
        throw Error("browser_control_timeout");
      };
      return worker;
    },
    remove: async () => {
      removed++;
    },
  });
  const host = createControllerRuntime(f.controller.controlOrigin);
  t.after(() => host.close());
  await host.attachStorage({ executionId: f.executionId }, {});
  const browser = fetch(f.url);
  await entered.promise;
  const finish = f.controller.command({ op: "finalize", executionId: f.executionId });
  await cancelled.promise;
  await finish;
  await browser;
  assert.equal(removed, 1);
});
