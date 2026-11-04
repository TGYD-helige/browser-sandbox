import WebSocket from "ws";

/** A bounded private CDP connection, independent of any harness package. */
export async function connectCdp(endpoint: string, signal?: AbortSignal) {
  const socket = new WebSocket(endpoint, {
    maxPayload: 32 * 1024 * 1024,
    handshakeTimeout: 30_000,
  });
  let sequence = 0;
  let ready = false;
  const pending = new Map<
    number,
    {
      resolve(value: any): void;
      reject(error: Error): void;
      timer: NodeJS.Timeout;
    }
  >();
  const close = () => socket.terminate();
  signal?.addEventListener("abort", close, { once: true });
  socket.on("error", () => {});
  socket.on("close", () => {
    signal?.removeEventListener("abort", close);
    for (const call of pending.values()) {
      clearTimeout(call.timer);
      call.reject(Error("browser_cdp_closed"));
    }
    pending.clear();
  });
  socket.on("message", (raw) => {
    let response: any;
    try {
      response = JSON.parse(raw.toString());
    } catch {
      return close();
    }
    const call = pending.get(response.id);
    if (!call) return;
    ready = true;
    pending.delete(response.id);
    clearTimeout(call.timer);
    response.error
      ? call.reject(Error("browser_cdp_operation_failed"))
      : call.resolve(response.result);
  });
  await new Promise<void>((resolve, reject) => {
    socket.once("open", resolve);
    socket.once("error", () => reject(Error("browser_cdp_connect_failed")));
    socket.once("close", () => reject(Error("browser_cdp_closed")));
    if (signal?.aborted) close();
  });
  return {
    close,
    call(
      method: string,
      params: Record<string, unknown> = {},
      sessionId?: string,
    ): Promise<any> {
      if (socket.readyState !== WebSocket.OPEN)
        return Promise.reject(Error("browser_cdp_closed"));
      return new Promise((resolve, reject) => {
        const id = ++sequence;
        // Allow the gateway's 45s cold startup before the ordinary 15s command budget.
        const timer = setTimeout(
          () => {
            pending.delete(id);
            reject(Error("browser_cdp_timeout"));
          },
          ready ? 15_000 : 60_000,
        );
        pending.set(id, { resolve, reject, timer });
        socket.send(
          JSON.stringify({
            id,
            method,
            params,
            ...(sessionId ? { sessionId } : {}),
          }),
        );
      });
    },
  };
}
