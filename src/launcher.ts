import { randomBytes } from "node:crypto";
import { once } from "node:events";
import { setTimeout as delay } from "node:timers/promises";
import { createCookieStore } from "./cookies.js";
import {
  BrowserCleanupUnconfirmed,
  VerifiedBrowserCheckpointFailure,
} from "./lifecycle-errors.js";

async function requestNativeClose(WebSocket, endpoint) {
  return new Promise<void>((resolve) => {
    const socket = new WebSocket(endpoint);
    let settled = false;
    const finish = () => {
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        socket.terminate();
        resolve();
      }
    };
    const timer = setTimeout(finish, 5000);
    socket.on("error", finish);
    socket.on("close", finish);
    socket.on("open", () =>
      socket.send(JSON.stringify({ id: 1, method: "Browser.close" })),
    );
    socket.on("message", finish);
  });
}

export function createBrowserLauncher({
  BrowserManager,
  Hooks,
  config,
  fileSystem,
  WebSocket,
  base,
  token,
  privacyArgs,
  cookieStore = createCookieStore,
}) {
  const launches = new Map();
  class ExecutionHooks extends Hooks {
    async browser({ browser, req }) {
      const entry = launches.get(req.parsed.searchParams.get("trackingId"));
      if (!entry) {
        browser.process()?.kill("SIGKILL");
        throw Error("browser_execution_missing");
      }
      entry.browser = browser;
      // Track the child before any asynchronous setup can fail.
      entry.child = browser.process();
      entry.child?.once("exit", () => {
        entry.exited.resolve();
        entry.disconnected.resolve();
      });
      if (entry.filesDir) {
        // Chrome resets this setting when its owning CDP session detaches.
        entry.downloadSession = await browser.browser.target().createCDPSession();
        await entry.downloadSession.send("Browser.setDownloadBehavior", {
          behavior: "allow",
          downloadPath: entry.filesDir,
        });
      }
      const flushNative = () =>
        (entry.flushing ??= (async () => {
          if (
            entry.child &&
            entry.child.exitCode === null &&
            entry.child.signalCode === null
          ) {
            await requestNativeClose(WebSocket, browser.wsEndpoint());
            await Promise.race([entry.exited.promise, delay(15000)]);
          }
        })());
      if (entry.sessionBoundary) {
        // Browserless clears its wrapper on CDP disconnect. Puppeteer's async disposer
        // may then bypass that wrapper and kill Chrome before databases finish flushing.
        const native = browser.browser;
        if (typeof native?.close !== "function")
          throw Error("browser_profile_launcher_upgrade_required");
        const nativeClose = native.close.bind(native);
        native.close = () =>
          (entry.nativeClosing ??= (async () => {
            await flushNative();
            return nativeClose();
          })());
      }

      await entry.cookies.restore(browser.wsEndpoint());
      const close = browser.close.bind(browser);
      browser.close = () => {
        if (!entry.stopping) {
          const exited = () =>
            entry.child &&
            (entry.child.exitCode !== null || entry.child.signalCode !== null);
          entry.stopping = (async () => {
            try {
              const endpoint = browser.wsEndpoint();
              if (endpoint && !exited()) await entry.cookies.save(endpoint);
            } catch (error) {
              // A dead Chrome cannot checkpoint cookies. Its verified exit still
              // permits retirement; a live-browser checkpoint failure remains an error.
              if (!exited()) {
                entry.checkpointFailure = error;
                throw error;
              }
            } finally {
              if (entry.sessionBoundary && !exited()) {
                // Ask Chrome itself to flush profile databases before Puppeteer cleans up its process.
                await flushNative();
              }
              await close();
            }
          })();
          // Browserless also closes from events without awaiting the promise.
          // Observe rejection while preserving it for explicit lifecycle callers.
          entry.stopping.catch(() => {});
        }
        return entry.stopping;
      };
    }
  }
  class ExecutionBrowserManager extends BrowserManager {
    constructor(...args: any[]) {
      super(...args);
    }
    getBrowserForRequest(req, ...args) {
      const entry = launches.get(req.parsed.searchParams.get("trackingId"));
      if (!entry || entry.cancelled) throw Error("browser_launch_cancelled");
      // Register before yielding: socket cancellation must await the server's launch, not its handshake.
      return (entry.starting = super.getBrowserForRequest(req, ...args));
    }
  }
  const hooks = new ExecutionHooks();
  const browserManager = new ExecutionBrowserManager(...[config, hooks, fileSystem]);
  return {
    hooks,
    browserManager,
    async launch({
      executionId,
      profileDir,
      journalDir,
      fingerprint,
      signal,
      sessionBoundary,
      filesDir = undefined,
      allowedDomains = [],
    }) {
      const trackingId = randomBytes(16).toString("hex");
      const entry: any = {
        sessionBoundary,
        filesDir,
        exited: Promise.withResolvers(),
        cookies: cookieStore(WebSocket, {
          executionId,
          profileDir,
          journalDir,
          sessionBoundary,
        }),
        disconnected: Promise.withResolvers(),
      };
      launches.set(trackingId, entry);
      const url = new URL("/chromium", base);
      url.protocol = "ws:";
      url.searchParams.set("token", token);
      url.searchParams.set("trackingId", trackingId);
      url.searchParams.set(
        "launch",
        JSON.stringify({
          headless: true,
          userDataDir: profileDir,
          // The runtime must checkpoint profiles before Puppeteer's process signal handlers close Chrome.
          handleSIGINT: false,
          handleSIGTERM: false,
          handleSIGHUP: false,
          args: [
            `--browser-execution-id=${executionId}`,
            `--fingerprint=${fingerprint}`,
            ...privacyArgs,
            ...(allowedDomains.length
              ? [
                  `--host-resolver-rules=MAP * ~NOTFOUND, ${allowedDomains.map((domain) => `EXCLUDE ${domain}`).join(", ")}`,
                ]
              : []),
          ],
        }),
      );
      const held = new WebSocket(url, { handshakeTimeout: 1800000 });
      held.on("error", entry.disconnected.resolve);
      held.on("close", entry.disconnected.resolve);
      const abort = () => {
        entry.cancelled = true;
        // Disconnecting Browserless first can kill Chrome before native databases flush.
        if (sessionBoundary && entry.ready) {
          entry.browser
            .close()
            .catch(() => {})
            .finally(() => held.terminate());
        } else held.terminate();
      };
      const close = () =>
        (entry.closing ??= (async () => {
          signal.removeEventListener("abort", abort);
          abort();
          // Failed launches include Browserless cleanup; late queued requests cannot pass the manager guard.
          await entry.starting?.catch(() => null);
          const child = entry.child;
          if (child && child.exitCode === null && child.signalCode === null) {
            const exited = once(child, "exit");
            // Browserless owns normal shutdown; retain the existing bounded process-exit fallback.
            await Promise.race([exited, delay(sessionBoundary ? 15000 : 5000)]);
            if (child.exitCode === null && child.signalCode === null) {
              entry.forcedStop = true;
              child.kill("SIGKILL");
            }
            await exited;
          }
          launches.delete(trackingId);
          try {
            await entry.stopping;
            if (
              sessionBoundary &&
              (entry.forcedStop || child?.signalCode || (child && child.exitCode !== 0))
            )
              throw new VerifiedBrowserCheckpointFailure(
                Error(
                  "browser_profile_flush_unconfirmed_" +
                    (entry.forcedStop
                      ? "forced_kill"
                      : (child?.signalCode?.toLowerCase() ?? "nonzero_exit")),
                ),
              );
          } catch (error) {
            // A failed save and a proven stop are separate outcomes. Preserve the
            // failure for lifecycle callers while permitting safe profile reuse.
            if (
              error === entry.checkpointFailure &&
              child &&
              (child.exitCode !== null || child.signalCode !== null)
            ) {
              throw new VerifiedBrowserCheckpointFailure(error);
            }
            throw error;
          }
        })()
          .catch((error) => {
            if (error instanceof VerifiedBrowserCheckpointFailure) throw error;
            throw new BrowserCleanupUnconfirmed(error);
          })
          .finally(async () => {
            await entry.downloadSession?.detach().catch(() => {});
            entry.downloadSession = null;
          }));
      signal.addEventListener("abort", abort, { once: true });
      if (signal.aborted) abort();
      try {
        await new Promise<void>((resolve, reject) => {
          held.once("open", resolve);
          held.once("error", reject);
          held.once("close", () => reject(Error("browser_session_closed")));
        });
        if (entry.cancelled || !entry.browser?.wsEndpoint())
          throw Error("browser_session_missing");
        entry.ready = true;
        return {
          endpoint: entry.browser.wsEndpoint(),
          close,
          closed: entry.disconnected.promise,
        };
      } catch {
        await close();
        throw Error("browser_session_failed");
      }
    },
  };
}
