import { chmod, rm } from "node:fs/promises";
import type { Socket } from "node:net";
import { createServer as socketServer } from "node:net";
import { StringDecoder } from "node:string_decoder";
import { attachArchiveChannel } from "./archive-channel.js";

export async function createHostControl({
  socketPath,
  executions,
  controlTimeoutMs,
  profileFinalizeTimeoutMs,
}) {
  const connections = new Set<Socket>();
  // Preserve lifecycle order per execution without holding other executions behind storage I/O.
  const createControl = (host) =>
    socketServer((socket) => {
      const requestLife = new AbortController();
      connections.add(socket);
      socket.once("close", () => {
        connections.delete(socket);
        requestLife.abort();
      });
      socket.setTimeout(controlTimeoutMs, () => socket.destroy());
      socket.on("error", () => {});
      let input = "",
        handled = false;
      const decoder = new StringDecoder("utf8");
      let storageHandler: ReturnType<typeof attachArchiveChannel>;
      socket.once("end", () => requestLife.abort());
      socket.on("data", (chunk) => {
        const decoded = decoder.write(chunk);
        if (storageHandler) return storageHandler(decoded);
        if (handled) return;
        input += decoded;
        if (input.length > (host ? 262144 : 4096)) return socket.destroy();
        if (!input.includes("\n")) return;
        handled = true;
        let request: any;
        try {
          request = JSON.parse(input.slice(0, input.indexOf("\n")));
          if (
            !request ||
            typeof request !== "object" ||
            Array.isArray(request) ||
            typeof request.op !== "string"
          )
            throw Error("browser_request_invalid");
        } catch {
          socket.end(JSON.stringify({ error: "browser_request_invalid" }) + "\n");
          return;
        }
        const budget =
          host && ["profile_finalize", "finalize"].includes(request?.op)
            ? profileFinalizeTimeoutMs
            : controlTimeoutMs;
        socket.setTimeout(budget, () => socket.destroy());
        const deadline = setTimeout(() => {
          requestLife.abort();
          socket.destroy();
        }, budget);
        socket.once("close", () => clearTimeout(deadline));
        const work = Promise.resolve().then(() => {
          if (!host) throw Error("browser_host_authority_required");
          return executions.dispatch(request, requestLife.signal);
        });
        work.then(
          (result) => {
            if (
              host &&
              request.op === "storage_attach" &&
              !requestLife.signal.aborted
            ) {
              clearTimeout(deadline);
              socket.setTimeout(0);
              if (result.status === "absent") {
                socket.end(JSON.stringify({ ready: true, absent: true }) + "\n");
                return;
              }
              if (result.status === "temporary") {
                socket.end(JSON.stringify({ ready: true, temporary: true }) + "\n");
                return;
              }
              storageHandler = attachArchiveChannel(
                executions.get(request.executionId),
                socket,
              );
              socket.write(JSON.stringify({ ready: true }) + "\n");
              const rest = input.slice(input.indexOf("\n") + 1);
              if (rest) storageHandler(rest);
            } else socket.end(JSON.stringify({ result }) + "\n");
          },
          (error) =>
            socket.end(
              JSON.stringify({
                error: /^browser_[a-z_]+$/.test(error.message)
                  ? error.message
                  : "browser_lifecycle_failed",
              }) + "\n",
            ),
        );
      });
    });
  const control = createControl(false),
    authControl = createControl(true);
  const authPath = socketPath + ".auth";
  await rm(authPath, { force: true });
  const priorMask = process.umask(0o077);
  try {
    await new Promise<void>((resolve, reject) => {
      authControl.once("error", reject);
      authControl.listen(authPath, resolve);
    });
  } finally {
    process.umask(priorMask);
  }
  await chmod(authPath, 0o600);
  await new Promise<void>((resolve, reject) => {
    control.once("error", reject);
    control.listen(socketPath, resolve);
  });
  // Set permissions while we own the socket; the container drops CAP_FOWNER.
  await chmod(socketPath, 0o600);
  return {
    async drain() {
      await executions.drain();
    },
    async stop() {
      for (const socket of connections) socket.destroy();
      await Promise.all(
        [control, authControl].map(
          (server) => new Promise<void>((resolve) => server.close(() => resolve())),
        ),
      );
      await rm(socketPath, { force: true });
      await rm(authPath, { force: true });
    },
  };
}
