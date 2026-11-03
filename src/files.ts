import { constants } from "node:fs";
import { open } from "node:fs/promises";
import type { IncomingMessage, ServerResponse } from "node:http";
import { join } from "node:path";
import { finished, pipeline } from "node:stream/promises";

export const MAX_FILE_BYTES = 16 * 1024 * 1024;

/** Single-level filenames and pinned descriptors keep browser-owned links out. */
export async function transferFile(
  directory: string,
  uid: number,
  name: string,
  request: IncomingMessage,
  response: ServerResponse,
) {
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(name)) {
    response.writeHead(400);
    response.end();
    return;
  }
  if (!["PUT", "GET"].includes(request.method)) {
    response.writeHead(405);
    response.end();
    return;
  }
  const deadline = setTimeout(() => {
    request.destroy();
    response.destroy();
  }, 30_000);
  let file: Awaited<ReturnType<typeof open>>;
  try {
    file = await open(
      join(directory, name),
      constants.O_NOFOLLOW |
        constants.O_NONBLOCK |
        (request.method === "PUT"
          ? constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY
          : constants.O_RDONLY),
      0o600,
    );
    const stat = await file.stat();
    if (!stat.isFile() || stat.nlink !== 1 || stat.size > MAX_FILE_BYTES)
      throw Error("browser_file_invalid");
    if (request.method === "PUT") {
      let size = 0;
      for await (const chunk of request) {
        size += chunk.length;
        if (size > MAX_FILE_BYTES) throw Error("browser_file_too_large");
        await file.writeFile(chunk);
      }
      if (process.getuid?.() === 0) await file.chown(uid, uid);
      response.writeHead(201, { "content-type": "application/json" });
      const sent = finished(response, { cleanup: true });
      response.end(
        JSON.stringify({ name, path: join(directory, name), byteSize: size }),
      );
      await sent;
    } else {
      response.strictContentLength = true;
      response.writeHead(200, {
        "content-type": "application/octet-stream",
        "content-length": stat.size,
      });
      await pipeline(
        file.createReadStream({ autoClose: false, end: Math.max(0, stat.size - 1) }),
        response,
      );
    }
  } catch (error) {
    if (!response.headersSent && !response.destroyed) {
      response.writeHead(
        error.code === "ENOENT" ? 404 : error.code === "EEXIST" ? 409 : 400,
      );
      response.end();
    } else response.destroy();
  } finally {
    clearTimeout(deadline);
    await file?.close();
  }
}
