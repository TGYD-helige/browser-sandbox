import { chmod, mkdir, rm } from "node:fs/promises";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { join } from "node:path";
import { attachCdpProxy } from "./cdp-proxy.js";
import { createExecutions } from "./executions.js";
import { createHostControl } from "./host-control.js";
import { createHttpHandler } from "./http-routes.js";
import { peerUid } from "./peer-identity.js";

/** Host lifecycle and execution-scoped transports over a private Linux network. */
export async function createGateway({
  WebSocket,
  WebSocketServer,
  launch,
  healthy,
  socketPath,
  port = 9222,
  dataRoot = "/browser-data",
  maxBrowsers = 2,
  resolvePeerUid = peerUid,
  profileStore = undefined,
  profileArchive = undefined,
  controlTimeoutMs = 30000,
  profileFinalizeTimeoutMs = 180000,
  startupTimeoutMs = 45000,
  maxSessionMs = 1800000,
}) {
  const server = createServer();
  const executions = await createExecutions({
    launch,
    dataRoot,
    maxBrowsers,
    getPort: () => (server.address() as AddressInfo).port,
    profileStore,
    profileArchive,
    startupTimeoutMs,
    maxSessionMs,
  });
  server.on("request", createHttpHandler({ executions, resolvePeerUid, healthy }));
  const ws = attachCdpProxy({
    server,
    WebSocket,
    WebSocketServer,
    executions,
    resolvePeerUid,
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, "127.0.0.1", resolve);
  });
  await mkdir(join(socketPath, ".."), { recursive: true, mode: 0o700 });
  await chmod(join(socketPath, ".."), 0o755);
  await rm(socketPath, { force: true });
  const control = await createHostControl({
    socketPath,
    executions,
    controlTimeoutMs,
    profileFinalizeTimeoutMs,
  });
  return {
    command: executions.command,
    port: (server.address() as AddressInfo).port,
    async stop() {
      executions.beginShutdown();
      await control.drain();
      let failure: unknown;
      try {
        await executions.stop();
      } catch (error) {
        failure = error;
      }
      ws.close();
      await Promise.all([
        new Promise<void>((resolve) => server.close(() => resolve())),
        control.stop(),
      ]);
      if (failure) throw failure;
    },
  };
}
