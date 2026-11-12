import { transferFile } from "./files.js";
export function createHttpHandler({
  executions,
  resolvePeerUid,
  healthy,
  resolveAccess = undefined,
  fileTransfers = { active: 0 },
  authentication = undefined,
  onSessionStop = undefined,
}) {
  async function authorize(capability, socket) {
    const entry = resolveAccess
      ? resolveAccess(capability)
      : executions.resolve(capability);
    if (
      !entry ||
      (!resolveAccess &&
        entry.uid >= 100000 &&
        (await resolvePeerUid(socket)) !== entry.uid) ||
      entry.revoked ||
      executions.closing
    )
      return null;
    return entry;
  }
  return async (req, res) => {
    try {
      const authRoute = /^\/execution\/([a-f0-9]{64})\/auth$/.exec(req.url ?? "");
      if (authRoute) {
        const entry = await authorize(authRoute[1], req.socket);
        if (!entry || !authentication) {
          res.writeHead(403);
          res.end();
          return;
        }
        if (req.method !== "POST") {
          res.writeHead(405);
          res.end();
          return;
        }
        const abort = new AbortController();
        const disconnected = () => {
          if (!res.writableEnded) abort.abort();
        };
        res.once("close", disconnected);
        const timer = setTimeout(() => {
          abort.abort();
          req.destroy();
        }, 620_000);
        const readTimer = setTimeout(() => {
          abort.abort();
          req.destroy();
        }, 10_000);
        try {
          const chunks: Buffer[] = [];
          let size = 0;
          for await (const chunk of req) {
            size += chunk.length;
            if (size > 8192) {
              res.writeHead(413);
              res.end();
              return;
            }
            chunks.push(chunk);
          }
          const input = Buffer.concat(chunks).toString("utf8");
          clearTimeout(readTimer);
          const result = await authentication.request(
            entry,
            JSON.parse(input),
            abort.signal,
          );
          res.writeHead(200, {
            "content-type": "application/json",
            "cache-control": "no-store",
          });
          res.end(JSON.stringify(result));
        } finally {
          clearTimeout(timer);
          clearTimeout(readTimer);
          res.off("close", disconnected);
        }
        return;
      }
      const sessionRoute = /^\/execution\/([a-f0-9]{64})\/session$/.exec(req.url ?? "");
      if (sessionRoute) {
        const entry = await authorize(sessionRoute[1], req.socket);
        if (!entry) {
          res.writeHead(403);
          res.end();
          return;
        }
        if (!["GET", "POST"].includes(req.method)) {
          res.writeHead(405);
          res.end();
          return;
        }
        const starting = entry.starting;
        const abort = new AbortController();
        const timer = setTimeout(() => {
          abort.abort();
          req.destroy();
          res.destroy();
        }, 30_000);
        const disconnected = () => {
          if (!res.writableEnded) abort.abort();
        };
        res.once("close", disconnected);
        try {
          let request = { action: "status", durationMs: undefined };
          if (req.method === "POST") {
            let input = "";
            for await (const chunk of req) {
              input += chunk;
              if (Buffer.byteLength(input) > 8192) {
                res.writeHead(413);
                res.end();
                return;
              }
            }
            request = JSON.parse(input);
          }
          if (request.action === "stop") onSessionStop?.(req, res);
          const result = await executions.dispatch(
            {
              op: "session",
              ...(request.action === "stop" ? { expectedStarting: starting } : {}),
              executionId: entry.executionId,
              action: request.action,
              durationMs: request.durationMs,
            },
            abort.signal,
          );
          res.writeHead(200, {
            "content-type": "application/json",
            "cache-control": "no-store",
          });
          res.end(JSON.stringify(result));
        } finally {
          clearTimeout(timer);
          res.off("close", disconnected);
        }
        return;
      }
      const fileRoute = /^\/execution\/([a-f0-9]{64})\/files\/([^/?]+)$/.exec(
        req.url ?? "",
      );
      if (fileRoute) {
        const entry = await authorize(fileRoute[1], req.socket);
        if (!entry) {
          res.writeHead(403);
          res.end();
          return;
        }
        if (fileTransfers.active >= 16) {
          res.writeHead(429);
          res.end();
          return;
        }
        fileTransfers.active++;
        const cancel = () => {
          req.destroy();
          res.destroy();
        };
        entry.lifetime.signal.addEventListener("abort", cancel, { once: true });
        const transfer = transferFile(
          entry.filesDir,
          entry.uid,
          fileRoute[2],
          req,
          res,
        );
        entry.fileTransfers.add(transfer);
        try {
          await transfer;
        } finally {
          fileTransfers.active--;
          entry.fileTransfers.delete(transfer);
          entry.lifetime.signal.removeEventListener("abort", cancel);
        }
        return;
      }
      if (
        req.url === "/health" &&
        !executions.closing &&
        (await healthy().catch(() => false))
      ) {
        res.writeHead(204);
        res.end();
      } else {
        res.writeHead(503);
        res.end();
      }
    } catch (error) {
      if (!res.headersSent) res.writeHead(400, { "content-type": "application/json" });
      res.end(
        JSON.stringify({
          error: /^browser_[a-z_]+$/.test(error.message)
            ? error.message
            : "browser_request_invalid",
        }),
      );
    }
  };
}
