import { randomBytes } from "node:crypto";
import { createRequire } from "node:module";
import { release } from "node:os";
import { createGateway } from "./gateway.js";
import { createBrowserLauncher } from "./launcher.js";
import { createBrowserLimiter } from "./limiter.js";
import { type ControlPeer, createNetworkHostControl } from "./network-control.js";
import {
  installBrowserNetworkPolicy,
  installWorkerNetworkPolicy,
  storageDestinations,
} from "./network-policy.js";
import { BROWSER_PRIVACY_ARGS } from "./privacy.js";
import { verifyTarget } from "./probe.js";
import { createWorkerStorage } from "./worker-storage.js";

if (process.env.PORT !== "3001") throw Error("browser_internal_port_invalid");
if (process.getuid() !== 0) throw Error("browser_provider_identity_required");
installBrowserNetworkPolicy();
if (process.argv[2] === "worker")
  installWorkerNetworkPolicy(undefined, undefined, await storageDestinations());
const [major, minor] = release().split(".").map(Number);
if (major < 4 || (major === 4 && minor < 8)) throw Error("browser_kernel_unsupported");
// Set before loading Browserless: the secret never enters execution environments.
process.env.TOKEN = randomBytes(32).toString("hex");
const {
  Browserless,
  BrowserManager,
  Config,
  FileSystem,
  Hooks,
  Limiter,
  Metrics,
  Monitoring,
  WebHooks,
} = await import(process.env.APP_DIR + "/build/exports.js");
const require = createRequire(import.meta.url);
const WebSocket = require("ws"),
  WebSocketServer = WebSocket.WebSocketServer;
const config = new Config(),
  fileSystem = new FileSystem(config);
const base = `http://127.0.0.1:${process.env.PORT}`;
const { hooks, browserManager, launch } = createBrowserLauncher({
  BrowserManager,
  Hooks,
  config,
  fileSystem,
  WebSocket,
  base,
  token: process.env.TOKEN,
  privacyArgs: BROWSER_PRIVACY_ARGS,
  extensionsDir: process.env.BROWSER_EXTENSIONS_DIR,
});
const metrics = new Metrics(),
  monitoring = new Monitoring(config),
  webhooks = new WebHooks(config);
const limiter = createBrowserLimiter(
  Limiter,
  config,
  metrics,
  monitoring,
  webhooks,
  hooks,
);
const backend = new Browserless({
  config,
  fileSystem,
  hooks,
  browserManager,
  limiter,
  metrics,
  monitoring,
  webhooks,
});
await backend.start();
const healthUrl = new URL("/active", base);
healthUrl.searchParams.set("token", process.env.TOKEN);
let verified = false;
const workerExecutionId =
  process.argv[2] === "worker" ? process.env.BROWSER_EXECUTION_ID : undefined;
let controllerPeer: ControlPeer;
let controllerAutomation = false;
const gateway = await createGateway({
  workerExecutionId,
  WebSocket,
  WebSocketServer,
  socketPath: process.env.BROWSER_CONTROL_SOCKET ?? "/run/browser-sandbox/control.sock",
  healthy: async () =>
    verified && (await fetch(healthUrl, { signal: AbortSignal.timeout(2000) })).ok,
  launch,
  maxBrowsers: Number(process.env.CONCURRENT ?? 2),
  maxSessionMs: Number(process.env.TIMEOUT ?? 1800000),
  startupTimeoutMs: Number(process.env.BROWSER_STARTUP_TIMEOUT ?? 45000),
  access: workerExecutionId
    ? {
        port: 8081,
        host: "0.0.0.0",
        publicOrigin: process.env.BROWSER_WORKER_ORIGIN,
        automation: {
          available: () => controllerAutomation,
          pause: async (executionId, signal) => {
            await controllerPeer.callback("pause", { executionId }, signal);
          },
          resume: async (executionId, signal) => {
            await controllerPeer.callback("resume", { executionId }, signal);
          },
        },
      }
    : process.env.BROWSER_ACCESS_PORT
      ? {
          port: Number(process.env.BROWSER_ACCESS_PORT),
          host: process.env.BROWSER_ACCESS_HOST ?? "127.0.0.1",
          publicOrigin: process.env.BROWSER_ACCESS_ORIGIN,
          viewerOrigins: process.env.BROWSER_VIEWER_ORIGINS?.split(","),
        }
      : undefined,
});
try {
  await verifyTarget(gateway, WebSocket);
  verified = true;
} catch (error) {
  console.error(
    "Browser startup probe failed:",
    String(error.message).replace(/[a-f0-9]{32,}/g, "[redacted]"),
  );
  await gateway.stop();
  await backend.stop();
  throw Error("browser_sandbox_probe_failed");
}
const workerStorage = createWorkerStorage(
  process.env.BROWSER_CONTROL_SOCKET ?? "/run/browser-sandbox/control.sock",
);
const networkControl = workerExecutionId
  ? createNetworkHostControl({
      server: gateway.workerServer,
      dispatch: async (request, peer, signal) => {
        if (request.executionId !== workerExecutionId)
          throw Error("browser_identity_mismatch");
        if (!controllerPeer || controllerPeer.signal.aborted || request.automation)
          controllerPeer = peer;
        if (request.op === "register")
          controllerAutomation = request.automation === true;
        const storageResult = await workerStorage(request, peer, signal);
        if (storageResult) return storageResult;
        return gateway.workerCommand(request, signal);
      },
    })
  : undefined;
for (const signal of ["SIGINT", "SIGTERM"])
  process.once(signal, async () => {
    try {
      networkControl?.close();
      await gateway.stop();
      await backend.stop();
      process.exit(0);
    } catch {
      process.exit(1);
    }
  });
