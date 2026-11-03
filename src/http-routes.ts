import { transferFile } from "./files.js";
export function createHttpHandler({ executions, resolvePeerUid, healthy }) {
  async function authorize(capability, socket) {
    const entry = executions.resolve(capability);
    if (
      !entry ||
      executions.closing ||
      (entry.uid >= 100000 && (await resolvePeerUid(socket)) !== entry.uid) ||
      entry.revoked ||
      executions.closing
    )
      return null;
    return entry;
  }
  return async (req, res) => {
    try {
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
        const abort = new AbortController();
        const timer = setTimeout(() => {
          abort.abort();
          req.destroy();
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
          const result = await executions.dispatch(
            {
              op: "session",
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
        const cancel = () => req.destroy();
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
