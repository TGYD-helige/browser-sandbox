import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { createServer } from "node:http";
import { test } from "node:test";
import { setImmediate as nextTurn } from "node:timers/promises";
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
