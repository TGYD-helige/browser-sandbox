import { randomUUID } from "node:crypto";
import { EXTENSION_STARTUP_ERRORS } from "./lifecycle-errors.js";

/** Prove the target can run a sandboxed renderer before admitting executions. */
export async function verifyTarget(gateway, WebSocket) {
  const executionId = randomUUID();
  let socket: any;
  try {
    const { wsEndpoint } = await gateway.command({
      op: "register",
      executionId,
    });
    socket = new WebSocket(wsEndpoint);
    const pending = new Map();
    let id = 0;
    socket.on("message", (data) => {
      const reply = JSON.parse(String(data)),
        entry = pending.get(reply.id);
      if (entry) {
        pending.delete(reply.id);
        reply.error
          ? entry.reject(Error("browser_probe_failed"))
          : entry.resolve(reply.result);
      }
    });
    socket.on("close", (code, reason) => {
      const message =
        code === 1011 && EXTENSION_STARTUP_ERRORS.has(String(reason))
          ? String(reason)
          : "browser_probe_disconnected";
      for (const entry of pending.values()) entry.reject(Error(message));
      pending.clear();
    });
    await new Promise<void>((resolve, reject) => {
      socket.once("open", resolve);
      socket.once("error", reject);
    });
    const call = (method, params = {}, sessionId = undefined) =>
      new Promise<any>((resolve, reject) => {
        const key = ++id,
          timer = setTimeout(() => reject(Error("browser_probe_timeout")), 30000);
        pending.set(key, {
          resolve: (value) => {
            clearTimeout(timer);
            resolve(value);
          },
          reject: (error) => {
            clearTimeout(timer);
            reject(error);
          },
        });
        socket.send(
          JSON.stringify({
            id: key,
            method,
            params,
            ...(sessionId ? { sessionId } : {}),
          }),
        );
      });
    const { targetId } = await call("Target.createTarget", {
      url: "about:blank",
    });
    const { sessionId } = await call("Target.attachToTarget", {
      targetId,
      flatten: true,
    });
    const { data } = await call("Page.captureScreenshot", {}, sessionId);
    if (typeof data !== "string" || data.length < 100)
      throw Error("browser_renderer_probe_failed");
  } finally {
    socket?.terminate();
    await gateway.command({ op: "finish", executionId });
  }
}
