import { chmod, mkdir, rm } from "node:fs/promises";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { join } from "node:path";
import { type AccessOptions, createAccessServer } from "./access-server.js";
import { createAuthRequests } from "./auth-requests.js";
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
  access = undefined as AccessOptions | undefined,
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
  const fileTransfers = { active: 0 };
  const authentication = createAuthRequests(
    executions,
    access?.automation,
    access?.allowInsecureAuthLoopback,
  );
  server.on(
    "request",
    createHttpHandler({
      executions,
      resolvePeerUid,
      healthy,
      fileTransfers,
      authentication,
    }),
  );
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
  let remote: Awaited<ReturnType<typeof createAccessServer>>;
  try {
    remote = access
      ? await createAccessServer({
          executions,
          WebSocket,
          WebSocketServer,
          options: access,
          fileTransfers,
          authentication,
        })
      : undefined;
  } catch (error) {
    ws.close();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await executions.stop();
    throw error;
  }
  const command = (
    request,
    host = false,
    fromSocket = false,
    signal: AbortSignal = undefined,
  ) => {
    if (typeof request.op === "string" && request.op.startsWith("access_")) {
      if (!host) return Promise.reject(Error("browser_host_authority_required"));
      if (!remote) return Promise.reject(Error("browser_access_disabled"));
      return Promise.resolve().then(() => {
        signal?.throwIfAborted();
        return remote.command(request);
      });
    }
    return executions.command(request, host, fromSocket, signal);
  };
  const control = await createHostControl({
    socketPath,
    executions,
    controlTimeoutMs,
    profileFinalizeTimeoutMs,
    dispatch: (request, signal) =>
      request.op.startsWith("access_")
        ? command(request, true, true, signal)
        : executions.dispatch(request, signal),
  });
  return {
    command,
    port: (server.address() as AddressInfo).port,
    accessPort: remote?.port,
    async stop() {
      executions.beginShutdown();
      await remote?.stop();
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
