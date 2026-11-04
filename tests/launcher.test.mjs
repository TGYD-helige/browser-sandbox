import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { EventEmitter, once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import WebSocket, { WebSocketServer } from "ws";
import { createGateway } from "../dist/gateway.js";
import { createBrowserLauncher } from "../dist/launcher.js";
import {
  BrowserCleanupUnconfirmed,
  VerifiedBrowserCheckpointFailure,
} from "../dist/lifecycle-errors.js";

const deferred = () => Promise.withResolvers();

for (const quiesce of [false, true])
  test(`checkpoint failure remains visible but verified retirement permits a new generation (quiesce=${quiesce})`, async () => {
    const WS = WebSocket;
    const upstream = new WebSocketServer({ host: "127.0.0.1", port: 0 });
    await once(upstream, "listening");
    upstream.on("connection", (socket) =>
      socket.on("message", (data) => socket.send(data)),
    );
    const root = await mkdtemp(join(tmpdir(), "browser-checkpoint-retirement-"));
    const exitGate = deferred(),
      closeStarted = deferred();
    let checkpoints = 0,
      launches = 0;
    const f = fixture(() => ({
      restore: async () => {},
      save: async () => {
        if (++checkpoints === 1) throw Error("browser_cookie_save_failed");
      },
    }));
    const browsers = [],
      peers = [],
      handles = [];
    const gateway = await createGateway({
      WebSocket: WS,
      WebSocketServer: WebSocketServer,
      healthy: async () => true,
      port: 0,
      dataRoot: join(root, "data"),
      socketPath: join(root, "control.sock"),
      launch: async (entry) => {
        const n = launches++,
          pending = f.launch(entry);
        const starting = f.browserManager.getBrowserForRequest(f.req(n));
        const browser = f.browser(`ws://127.0.0.1:${upstream.address().port}`);
        browser.close = async () => {
          if (n === 0) {
            closeStarted.resolve();
            await exitGate.promise;
          }
          browser.exit();
        };
        browsers.push(browser);
        f.requests[n].resolve(browser);
        await starting;
        f.sockets[n].emit("open");
        const handle = await pending;
        handles.push(handle);
        return handle;
      },
    });
    const executionId = "11111111-1111-4111-8111-111111111111";
    try {
      const { wsEndpoint } = await gateway.command({
        op: "register",
        executionId,
        workspace: join(root, "workspace"),
      });
      const first = new WS(wsEndpoint);
      peers.push(first);
      first.on("error", () => {});
      await once(first, "open");
      const before = once(first, "message");
      first.send("before");
      assert.equal(String((await before)[0]), "before");
      const checkpoint = browsers[0].close();
      checkpoint.catch(() => {});
      await closeStarted.promise;
      // The socket can disappear before Chrome exits. It is not shutdown proof.
      f.sockets[0].emit("close");
      let settled = false;
      const retired = quiesce
        ? gateway.command({ op: "quiesce", executionId })
        : handles[0].close();
      retired.then(
        () => {
          settled = true;
        },
        () => {
          settled = true;
        },
      );
      await delay(20);
      assert.equal(settled, false);
      assert.equal(launches, 1);
      exitGate.resolve();
      await assert.rejects(checkpoint, /browser_cookie_save_failed/);
      await assert.rejects(retired, /browser_cookie/);
      // No command is replayed. A new connection may use only the verified-dead generation's profile.
      const resumed = await gateway.command({
        op: "register",
        executionId,
        workspace: join(root, "workspace"),
      });
      const second = new WS(resumed.wsEndpoint);
      peers.push(second);
      second.on("error", () => {});
      const after = new Promise((resolve, reject) => {
        second.once("message", (data) => resolve(String(data)));
        second.once("close", () =>
          reject(Error("verified retirement still blocks reconnect")),
        );
        second.once("error", reject);
      });
      second.once("open", () => second.send("after"));
      assert.equal(await after, "after");
      assert.equal(launches, 2);
      browsers[0].exit(); // late old-generation event cannot retire the new browser
      const late = once(second, "message");
      second.send("late");
      assert.equal(String((await late)[0]), "late");
      const closing = browsers[1].close();
      await closing;
      assert.deepEqual(await gateway.command({ op: "finish", executionId }), {
        stopped: true,
        credentialsScrubbed: true,
      });
      await assert.rejects(
        gateway.command({ op: "register", executionId }),
        /browser_execution_revoked/,
      );
    } finally {
      exitGate.resolve();
      for (const peer of peers) peer.terminate();
      for (const browser of browsers) browser.exit();
      await gateway.stop().catch(() => {});
      for (const socket of upstream.clients) socket.terminate();
      await new Promise((resolve) => upstream.close(resolve));
      await rm(root, { recursive: true, force: true });
    }
  });
function fixture(
  cookieStore = () => ({ restore: async () => {}, save: async () => {} }),
) {
  const sockets = [],
    requests = [];
  class Socket extends EventEmitter {
    constructor(url) {
      super();
      this.url = url;
      sockets.push(this);
    }
    terminate() {
      this.closed = true;
      queueMicrotask(() => this.emit("close"));
    }
  }
  class Manager {
    constructor(_config, hooks) {
      this.hooks = hooks;
    }
    async getBrowserForRequest(req) {
      const gate = deferred();
      requests.push(gate);
      const browser = await gate.promise;
      await this.hooks.browser({ browser, req });
      return browser;
    }
  }
  const runtime = createBrowserLauncher({
    BrowserManager: Manager,
    Hooks: class {},
    WebSocket: Socket,
    base: "http://localhost",
    token: "test",
    privacyArgs: [],
    cookieStore,
  });
  const req = (index) => ({ parsed: sockets[index].url });
  function browser(endpoint = "ws://localhost/browser") {
    const child = new EventEmitter();
    child.exitCode = null;
    child.signalCode = null;
    child.kill = () => {
      throw Error("unexpected fallback kill");
    };
    return {
      browser: {
        target: () => ({
          createCDPSession: async () => ({
            send: async () => {},
            detach: async () => {},
          }),
        }),
      },
      process: () => child,
      wsEndpoint: () => endpoint,
      close: async () => {},
      exit() {
        child.exitCode = 0;
        child.emit("exit", 0);
      },
    };
  }
  const start = (signal) =>
    runtime.launch({
      executionId: "same-workspace",
      profileDir: "/workspace/browser-data",
      fingerprint: "1",
      signal,
    });
  return { ...runtime, sockets, requests, req, browser, start };
}

test("a failed startup cannot claim retirement when process termination throws", async () => {
  const f = fixture(),
    abort = new AbortController();
  const pending = f.start(abort.signal);
  pending.catch(() => {});
  const starting = f.browserManager.getBrowserForRequest(f.req(0));
  const browser = f.browser();
  f.requests[0].resolve(browser);
  await starting;
  try {
    abort.abort();
    await assert.rejects(
      pending,
      (error) => error instanceof BrowserCleanupUnconfirmed,
    );
    assert.equal(browser.process().exitCode, null);
  } finally {
    browser.exit();
  }
});

test("SIGKILL without an exit event has a bounded unconfirmed outcome", {
  timeout: 2000,
}, async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const f = fixture();
  const pending = f.start(new AbortController().signal);
  const starting = f.browserManager.getBrowserForRequest(f.req(0));
  const browser = f.browser();
  const signals = [];
  browser.process().kill = (signal) => {
    signals.push(signal);
    return true;
  };
  f.requests[0].resolve(browser);
  await starting;
  f.sockets[0].emit("open");
  const handle = await pending;
  const closed = handle.close();
  closed.catch(() => {});
  try {
    await new Promise((resolve) => setImmediate(resolve));
    t.mock.timers.tick(5000);
    await new Promise((resolve) => setImmediate(resolve));
    assert.deepEqual(signals, ["SIGKILL"]);
    t.mock.timers.tick(5000);
    await assert.rejects(closed, (error) => error instanceof BrowserCleanupUnconfirmed);
    assert.equal(browser.process().exitCode, null);
    assert.equal(browser.process().signalCode, null);
    await assert.rejects(
      handle.close(),
      (error) => error instanceof BrowserCleanupUnconfirmed,
    );
  } finally {
    browser.exit();
    t.mock.timers.reset();
  }
});

for (const cause of ["timeout", "crash"])
  test(`launcher reports ${cause} without an attached MCP client`, async () => {
    const f = fixture();
    const pending = f.start(new AbortController().signal);
    const starting = f.browserManager.getBrowserForRequest(f.req(0));
    const browser = f.browser();
    f.requests[0].resolve(browser);
    await starting;
    f.sockets[0].emit("open");
    const session = await pending;
    if (cause === "timeout") f.sockets[0].emit("close");
    else browser.exit();
    await Promise.race([
      session.closed,
      delay(1000).then(() => {
        throw Error("browser death was not reported");
      }),
    ]);
    browser.exit();
    await session.close();
  });

test("cancelled client waits for the real server launch and process exit before reconnecting", async () => {
  const f = fixture(),
    abort = new AbortController();
  let settled = false;
  const first = f.start(abort.signal).catch((error) => {
    settled = true;
    return error;
  });
  const serverStart = f.browserManager.getBrowserForRequest(f.req(0));
  abort.abort();
  await delay(20);
  assert.equal(settled, false, "socket close must not settle a pending server launch");
  const oldBrowser = f.browser();
  f.requests[0].resolve(oldBrowser);
  await serverStart;
  await delay(20);
  assert.equal(settled, false, "the profile remains held until Chrome exits");
  oldBrowser.exit();
  assert.match((await first).message, /browser_session_failed/);

  const next = f.start(new AbortController().signal);
  assert.notEqual(
    f.req(0).parsed.searchParams.get("trackingId"),
    f.req(1).parsed.searchParams.get("trackingId"),
  );
  assert.equal(
    JSON.parse(f.req(0).parsed.searchParams.get("launch")).userDataDir,
    JSON.parse(f.req(1).parsed.searchParams.get("launch")).userDataDir,
  );
  assert.throws(
    () => f.browserManager.getBrowserForRequest(f.req(0)),
    /browser_launch_cancelled/,
  );
  const nextServer = f.browserManager.getBrowserForRequest(f.req(1));
  const newBrowser = f.browser();
  f.requests[1].resolve(newBrowser);
  await nextServer;
  f.sockets[1].emit("open");
  const session = await next;
  const closing = session.close();
  newBrowser.exit();
  await closing;
});

test("a cancelled queued request cannot start later or attach to a new launch", async () => {
  const f = fixture(),
    abort = new AbortController();
  const first = assert.rejects(f.start(abort.signal), /browser_session_failed/);
  abort.abort();
  await first;
  const nextAbort = new AbortController();
  const next = assert.rejects(f.start(nextAbort.signal), /browser_session_failed/);
  assert.throws(
    () => f.browserManager.getBrowserForRequest(f.req(0)),
    /browser_launch_cancelled/,
  );
  assert.equal(f.requests.length, 0);
  nextAbort.abort();
  await next;
});

test("server launch failure settles cancellation without waiting for a browser that never started", async () => {
  const f = fixture(),
    abort = new AbortController();
  const first = assert.rejects(f.start(abort.signal), /browser_session_failed/);
  const serverStart = f.browserManager.getBrowserForRequest(f.req(0));
  const serverFailed = assert.rejects(serverStart, /launch failed/);
  abort.abort();
  f.requests[0].reject(Error("launch failed"));
  await Promise.all([first, serverFailed]);
});

test("gateway reconnect cannot reuse the profile while cancelled server startup is still running", async () => {
  const upstream = new WebSocketServer({ host: "127.0.0.1", port: 0 });
  await once(upstream, "listening");
  upstream.on("connection", (socket) =>
    socket.on("message", (data) => socket.send(data)),
  );
  const endpoint = `ws://127.0.0.1:${upstream.address().port}`;
  const root = await mkdtemp(join(tmpdir(), "browser-launch-order-"));
  const f = fixture(),
    oldBrowser = f.browser(endpoint),
    newBrowser = f.browser(endpoint);
  const gateway = await createGateway({
    WebSocket,
    WebSocketServer: WebSocketServer,
    launch: f.launch,
    healthy: async () => true,
    port: 0,
    dataRoot: join(root, "data"),
    socketPath: join(root, "control.sock"),
  });
  const waitFor = async (predicate) => {
    const until = Date.now() + 3000;
    while (!predicate()) {
      assert.ok(Date.now() < until, "timed out");
      await delay(10);
    }
  };
  let first, second;
  try {
    const { wsEndpoint } = await gateway.command({
      op: "register",
      executionId: "11111111-1111-4111-8111-111111111111",
      workspace: join(root, "first"),
    });
    first = new WebSocket(wsEndpoint);
    await once(first, "open");
    await waitFor(() => f.sockets.length === 1);
    const oldStarting = f.browserManager.getBrowserForRequest(f.req(0));
    first.close();
    await once(first, "close");
    await waitFor(() => f.sockets[0].closed);
    second = new WebSocket(wsEndpoint);
    await once(second, "open");
    second.send("hi");
    await delay(30);
    assert.equal(f.sockets.length, 1);
    f.requests[0].resolve(oldBrowser);
    await oldStarting;
    await delay(30);
    assert.equal(f.sockets.length, 1, "reconnect must still wait for process exit");
    oldBrowser.exit();
    await waitFor(() => f.sockets.length === 2);
    const nextStarting = f.browserManager.getBrowserForRequest(f.req(1));
    f.requests[1].resolve(newBrowser);
    await nextStarting;
    const echoed = once(second, "message");
    f.sockets[1].emit("open");
    assert.equal(String((await echoed)[0]), "hi");
  } finally {
    oldBrowser.exit();
    newBrowser.exit();
    first?.terminate();
    second?.terminate();
    for (const pending of f.requests) pending.resolve(oldBrowser);
    await gateway.stop();
    await new Promise((resolve) => upstream.close(resolve));
    await rm(root, { recursive: true, force: true });
  }
});

for (const forceKill of [false, true])
  test(`waits for the captured child after Browserless clears its wrapper (fallback=${forceKill})`, async () => {
    const f = fixture(),
      abort = new AbortController();
    const child = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], {
      stdio: "ignore",
    });
    await once(child, "spawn");
    const exited = once(child, "exit");
    let wrappedChild = child,
      settled = false;
    const browser = {
      process: () => wrappedChild,
      wsEndpoint: () => "ws://localhost/browser",
      close: async () => {},
    };
    const first = f.start(abort.signal).catch((error) => {
      settled = true;
      return error;
    });
    const serverStart = f.browserManager.getBrowserForRequest(f.req(0));
    // Router registers its completion before launch cancellation starts awaiting the same Promise.
    const routerComplete = serverStart.then(() => {
      wrappedChild = null;
    });
    try {
      abort.abort();
      await delay(20);
      f.requests[0].resolve(browser);
      await routerComplete;
      await delay(30);
      assert.equal(browser.process(), null);
      assert.equal(child.exitCode, null);
      assert.equal(child.signalCode, null);
      assert.equal(
        settled,
        false,
        "a cleared Browserless wrapper is not process-exit evidence",
      );
      if (!forceKill) child.kill("SIGTERM");
      await exited;
      assert.match((await first).message, /browser_session_failed/);
      assert.equal(child.signalCode, forceKill ? "SIGKILL" : "SIGTERM");
    } finally {
      f.requests[0].resolve(browser);
      if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
      await exited;
      await first;
    }
  });

test("cookie restore precedes admission and persistent cookies are saved before Chrome closes", async () => {
  const events = [];
  const f = fixture(() => ({
    restore: async () => {
      events.push("restore");
    },
    save: async () => {
      events.push("save");
    },
  }));
  const starting = f.start(new AbortController().signal);
  const serverStart = f.browserManager.getBrowserForRequest(f.req(0));
  const browser = f.browser();
  browser.close = async () => {
    events.push("chrome-close");
    browser.exit();
  };
  f.requests[0].resolve(browser);
  await serverStart;
  f.sockets[0].emit("open");
  const session = await starting;
  assert.deepEqual(events, ["restore"]);
  await browser.close();
  await browser.close();
  await session.close();
  assert.deepEqual(events, ["restore", "save", "chrome-close"]);
});

test("cookie save failures still close Chrome and fail lifecycle confirmation", async () => {
  const f = fixture(() => ({
    restore: async () => {},
    save: async () => {
      throw Error("browser_cookie_save_failed");
    },
  }));
  const starting = f.start(new AbortController().signal);
  const serverStart = f.browserManager.getBrowserForRequest(f.req(0));
  const browser = f.browser();
  browser.close = async () => browser.exit();
  f.requests[0].resolve(browser);
  await serverStart;
  f.sockets[0].emit("open");
  const session = await starting;
  await assert.rejects(browser.close(), /browser_cookie_save_failed/);
  assert.equal(browser.process().exitCode, 0);
  await assert.rejects(session.close(), /browser_cookie_save_failed/);
});

test("runtime shutdown owns signals so Puppeteer cannot close Chrome before the Cookie checkpoint", async () => {
  const f = fixture(),
    abort = new AbortController();
  const starting = assert.rejects(f.start(abort.signal), /browser_session_failed/);
  const options = JSON.parse(f.req(0).parsed.searchParams.get("launch"));
  for (const signal of ["handleSIGINT", "handleSIGTERM", "handleSIGHUP"])
    assert.equal(options[signal], false);
  abort.abort();
  await starting;
});

for (const duringSave of [false, true])
  test(`crashed Chrome retires without a cookie checkpoint rejection (duringSave=${duringSave})`, async () => {
    let saves = 0,
      closes = 0,
      browser;
    const f = fixture(() => ({
      restore: async () => {},
      save: async () => {
        saves++;
        browser.exit();
        throw Error("browser_cookie_save_failed");
      },
    }));
    const starting = f.start(new AbortController().signal);
    const serverStart = f.browserManager.getBrowserForRequest(f.req(0));
    browser = f.browser();
    browser.close = async () => {
      closes++;
    };
    f.requests[0].resolve(browser);
    await serverStart;
    f.sockets[0].emit("open");
    const session = await starting;
    if (!duringSave) browser.exit();
    // Model Browserless's event callback that ignores the returned promise.
    browser.close();
    await delay(20);
    await session.close();
    assert.equal(saves, duringSave ? 1 : 0);
    assert.equal(closes, 1);
  });

test("unawaited live-browser checkpoint failures stay observable without terminating the service", async () => {
  const f = fixture(() => ({
    restore: async () => {},
    save: async () => {
      throw Error("browser_cookie_save_failed");
    },
  }));
  const starting = f.start(new AbortController().signal);
  const serverStart = f.browserManager.getBrowserForRequest(f.req(0));
  const browser = f.browser();
  browser.close = async () => browser.exit();
  f.requests[0].resolve(browser);
  await serverStart;
  f.sockets[0].emit("open");
  const session = await starting;
  browser.close();
  await delay(20);
  await assert.rejects(session.close(), /browser_cookie_save_failed/);
});

for (const timing of ["before-rejection", "after-rejection"]) {
  for (const op of ["quiesce", "finish", "stop"])
    test(`handshake checkpoint failure survives ${op} (${timing})`, {
      timeout: 10000,
    }, async () => {
      const WS = WebSocket;
      const upstream = new WebSocketServer({ host: "127.0.0.1", port: 0 });
      await once(upstream, "listening");
      upstream.on("connection", (socket) =>
        socket.on("message", (data) => socket.send(data)),
      );
      const root = await mkdtemp(join(tmpdir(), "browser-handshake-checkpoint-"));
      const exitGate = deferred(),
        handshakeFailed = deferred(),
        closeStarted = deferred();
      let launches = 0,
        checkpoints = 0,
        launchFailure,
        stopped = false;
      const browsers = [],
        peers = [];
      const f = fixture(() => ({
        restore: async () => {},
        save: async () => {
          if (++checkpoints === 1) throw Error("browser_cookie_save_failed");
        },
      }));
      const gateway = await createGateway({
        WebSocket: WS,
        WebSocketServer: WebSocketServer,
        healthy: async () => true,
        port: 0,
        dataRoot: join(root, "data"),
        socketPath: join(root, "control.sock"),
        launch: async (entry) => {
          const n = launches++,
            pending = f.launch(entry);
          pending.catch((error) => {
            launchFailure = error;
          });
          const starting = f.browserManager.getBrowserForRequest(f.req(n));
          const browser = f.browser(`ws://127.0.0.1:${upstream.address().port}`);
          browser.close = async () => {
            if (n === 0) {
              closeStarted.resolve();
              await exitGate.promise;
            }
            browser.exit();
          };
          browsers.push(browser);
          f.requests[n].resolve(browser);
          await starting;
          if (n === 0) {
            browser.close().catch(() => {});
            await closeStarted.promise;
            // Browserless closes before its initial websocket handshake completes.
            f.sockets[n].emit("error", Error("handshake failed"));
            handshakeFailed.resolve();
          } else f.sockets[n].emit("open");
          return pending;
        },
      });
      const executionId = "11111111-1111-4111-8111-111111111111";
      const shutdown = () => {
        stopped = true;
        return gateway.stop();
      };
      try {
        const registration = {
          op: "register",
          executionId,
          workspace: join(root, "workspace"),
        };
        const { wsEndpoint } = await gateway.command(registration);
        const first = new WS(wsEndpoint);
        peers.push(first);
        first.on("error", () => {});
        const disconnected = new Promise((resolve) => first.once("close", resolve));
        await handshakeFailed.promise;
        const lifecycle = () =>
          op === "stop" ? shutdown() : gateway.command({ op, executionId });
        let confirmation;
        if (timing === "before-rejection") {
          let settled = false;
          confirmation = lifecycle();
          confirmation.then(
            () => {
              settled = true;
            },
            () => {
              settled = true;
            },
          );
          await delay(20);
          assert.equal(
            settled,
            false,
            "no lifecycle acknowledgement before captured process exit",
          );
        }
        exitGate.resolve();
        await disconnected;
        if (!confirmation) confirmation = lifecycle();
        await assert.rejects(
          confirmation,
          op === "stop" ? /browser_lifecycle_failed/ : /browser_cookie_save_failed/,
        );
        assert.ok(launchFailure instanceof VerifiedBrowserCheckpointFailure);
        if (op === "quiesce") {
          const resumed = await gateway.command(registration);
          const second = new WS(resumed.wsEndpoint);
          peers.push(second);
          second.on("error", () => {});
          const echo = new Promise((resolve, reject) => {
            second.once("message", (data) => resolve(String(data)));
            second.once("close", () =>
              reject(Error("verified exit did not permit recovery")),
            );
            second.once("error", reject);
          });
          second.once("open", () => second.send("recovered"));
          assert.equal(await echo, "recovered");
          assert.equal(launches, 2);
          browsers[0].exit();
          const late = once(second, "message");
          second.send("still-current");
          assert.equal(String((await late)[0]), "still-current");
          await browsers[1].close();
          assert.deepEqual(await gateway.command({ op: "finish", executionId }), {
            stopped: true,
            credentialsScrubbed: true,
          });
        } else if (op === "finish") {
          await assert.rejects(
            gateway.command(registration),
            /browser_execution_revoked/,
          );
          await assert.rejects(shutdown(), /browser_lifecycle_failed/);
        }
      } finally {
        exitGate.resolve();
        for (const browser of browsers) browser.exit();
        for (const peer of peers) peer.terminate();
        if (!stopped) await shutdown().catch(() => {});
        for (const socket of upstream.clients) socket.terminate();
        await new Promise((resolve) => upstream.close(resolve));
        await rm(root, { recursive: true, force: true });
      }
    });
}

for (const clean of [true, false])
  test(`persistent profiles fence Puppeteer async disposal until native exit (clean=${clean})`, async () => {
    const f = fixture(),
      abort = new AbortController();
    const pending = f.launch({
      executionId: "profile-run",
      profileDir: "/profile",
      fingerprint: "1",
      sessionBoundary: true,
      signal: abort.signal,
    });
    const starting = f.browserManager.getBrowserForRequest(f.req(0));
    const browser = f.browser();
    let disposed = 0;
    browser.browser = {
      close: async () => {
        disposed++;
        assert.notEqual(browser.process().exitCode, null);
      },
    };
    browser.close = () => browser.browser.close();
    f.requests[0].resolve(browser);
    await starting;
    f.sockets[0].emit("open");
    const handle = await pending,
      closing = handle.close();
    closing.catch(() => {});
    while (f.sockets.length < 2) await delay(1);
    const nativeRequest = f.sockets[1];
    nativeRequest.send = (source) => {
      assert.equal(JSON.parse(source).method, "Browser.close");
      queueMicrotask(() => nativeRequest.emit("message", '{"id":1,"result":{}}'));
    };
    nativeRequest.emit("open");
    const disposing = browser.browser.close();
    disposing.catch(() => {});
    await delay(10);
    assert.equal(
      disposed,
      0,
      "CDP disconnect must not escalate into an early process kill",
    );
    if (!clean) browser.process().signalCode = "SIGKILL";
    browser.exit();
    if (clean) await closing;
    else await assert.rejects(closing, /browser_profile_flush_unconfirmed_sigkill/);
    await disposing;
    assert.equal(disposed, 1);
  });

test("download configuration stays attached until its browser generation has stopped", async () => {
  const f = fixture();
  let detached = 0;
  const pending = f.launch({
    executionId: "download",
    profileDir: "/profile",
    fingerprint: "1",
    filesDir: "/files",
    signal: new AbortController().signal,
  });
  const starting = f.browserManager.getBrowserForRequest(f.req(0));
  const browser = f.browser();
  browser.browser.target = () => ({
    createCDPSession: async () => ({
      async send(method, params) {
        assert.equal(method, "Browser.setDownloadBehavior");
        assert.equal(params.downloadPath, "/files");
      },
      async detach() {
        detached++;
      },
    }),
  });
  f.requests[0].resolve(browser);
  let session;
  try {
    await starting;
    f.sockets[0].emit("open");
    session = await pending;
    assert.equal(detached, 0, "detaching resets Chrome's download configuration");
  } finally {
    browser.exit();
    await session?.close();
  }
  assert.equal(detached, 1);
});
