import { createHash, randomBytes } from "node:crypto";
import { existsSync } from "node:fs";
import { mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";

function cdp(WebSocket, endpoint, method, params = {}) {
  return new Promise<{ cookies: any[] }>((resolve, reject) => {
    const socket = new WebSocket(endpoint);
    let settled = false;
    const finish = (error, result = undefined) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      socket.terminate();
      error
        ? reject(Error("browser_cookie_protocol_failed_" + error))
        : resolve(result);
    };
    const timer = setTimeout(() => finish("timeout"), 5000);
    socket.on("error", () => finish("socket"));
    socket.on("close", () => finish("closed"));
    socket.on("open", () => socket.send(JSON.stringify({ id: 1, method, params })));
    socket.on("message", (data) => {
      try {
        const reply = JSON.parse(String(data));
        if (reply.id === 1) finish(reply.error ? "cdp" : null, reply.result);
      } catch {
        finish("json");
      }
    });
  });
}

// Chrome can exit before its delayed Cookie DB commit. Journal persistent cookies across managed closes.
export function createCookieStore(
  WebSocket,
  {
    executionId,
    profileDir,
    journalDir,
    dataRoot = "/browser-data",
    sessionBoundary = false,
  },
) {
  const directory =
    journalDir ??
    (profileDir === join(dataRoot, executionId, "profile")
      ? join(dataRoot, executionId)
      : join(
          dataRoot,
          ".cookies",
          createHash("sha256").update(profileDir).digest("hex"),
        ));
  const path = join(directory, "cookies.json");
  const profileExisted = existsSync(profileDir);
  const persistent = (cookies) => {
    const result = cookies.filter(
      (cookie) => !cookie.session && cookie.expires > Date.now() / 1000,
    );
    if (sessionBoundary && result.some((cookie) => cookie.partitionKeyOpaque))
      throw Error("browser_cookie_partition_unsupported");
    return result.filter((cookie) => !cookie.partitionKeyOpaque);
  };
  return {
    async restore(endpoint) {
      if (!profileExisted) {
        await rm(path, { force: true });
        return;
      }
      let source: string;
      try {
        source = await readFile(path, "utf8");
      } catch (error) {
        if (error.code !== "ENOENT") throw Error("browser_cookie_restore_failed");
        if (!sessionBoundary) return;
        // A new Chrome process is a new browser session, including after an unclean exit.
        const { cookies } = await cdp(WebSocket, endpoint, "Storage.getCookies");
        await cdp(WebSocket, endpoint, "Storage.clearCookies");
        await cdp(WebSocket, endpoint, "Storage.setCookies", {
          cookies: persistent(cookies),
        });
        return;
      }
      try {
        const cookies = persistent(JSON.parse(source));
        // Replace the prior disk state as well: deleted cookies must not reappear.
        await cdp(WebSocket, endpoint, "Storage.clearCookies");
        await cdp(WebSocket, endpoint, "Storage.setCookies", { cookies });
        await rm(path);
      } catch {
        throw Error("browser_cookie_restore_failed");
      }
    },
    async save(endpoint) {
      const temporary = join(
        directory,
        `cookies-${randomBytes(16).toString("hex")}.tmp`,
      );
      let stage = "protocol";
      try {
        const { cookies } = await cdp(WebSocket, endpoint, "Storage.getCookies");
        // These directories are runtime-owned; browser UID 20001 cannot redirect root writes.
        const snapshot = JSON.stringify(persistent(cookies));
        stage = "mkdir";
        await mkdir(directory, { recursive: true, mode: 0o700 });
        stage = "write";
        await writeFile(temporary, snapshot, { mode: 0o600, flag: "wx" });
        stage = "rename";
        await rename(temporary, path);
      } catch (error) {
        throw Error(
          error.message === "browser_cookie_partition_unsupported"
            ? error.message
            : /^browser_cookie_protocol_failed_[a-z]+$/.test(error.message)
              ? "browser_cookie_save_failed_" +
                error.message.slice("browser_cookie_protocol_failed_".length)
              : "browser_cookie_save_failed_" +
                stage +
                "_" +
                (/^[A-Z_0-9]+$/.test(error.code ?? "")
                  ? error.code.toLowerCase()
                  : "error"),
        );
      } finally {
        await rm(temporary, { force: true });
      }
    },
  };
}
