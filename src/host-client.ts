import { connect } from "node:net";
import { StringDecoder } from "node:string_decoder";

export interface Registration {
  executionId: string;
  executorUid: number;
  profileId?: string;
  storage?: boolean;
  allowedDomains?: string[];
}

export interface Endpoint {
  protocol: "browser-sandbox.host/v1";
  executorUid: number;
  wsEndpoint: string | null;
  profileId?: string;
}

/** Trusted host only. The socket's filesystem permissions are the authority. */
export function hostRequest<T = Record<string, unknown>>(
  socketPath: string,
  request: Record<string, unknown>,
  { signal, timeoutMs = 180_000 }: { signal?: AbortSignal; timeoutMs?: number } = {},
): Promise<T> {
  return new Promise((resolve, reject) => {
    const socket = connect(socketPath + ".auth");
    let input = "";
    const decoder = new StringDecoder("utf8");
    const finish = (error?: Error, result?: T) => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", abort);
      socket.destroy();
      error ? reject(error) : resolve(result as T);
    };
    const abort = () => finish(Error("browser_request_cancelled"));
    const timer = setTimeout(() => finish(Error("browser_control_timeout")), timeoutMs);
    signal?.addEventListener("abort", abort, { once: true });
    socket.on("error", () => finish(Error("browser_control_unavailable")));
    socket.on("end", () => {
      if (!input.includes("\n")) finish(Error("browser_control_incomplete"));
    });
    socket.on("connect", () => {
      if (signal?.aborted) return abort();
      socket.write(JSON.stringify(request) + "\n");
    });
    socket.on("data", (chunk) => {
      input += decoder.write(chunk);
      if (input.length > 32 * 1024 * 1024)
        return finish(Error("browser_control_response_too_large"));
      if (!input.includes("\n")) return;
      try {
        const frame = JSON.parse(input.slice(0, input.indexOf("\n")));
        finish(frame.error ? Error(frame.error) : undefined, frame.result);
      } catch {
        finish(Error("browser_control_response_invalid"));
      }
    });
    if (signal?.aborted) abort();
  });
}

export interface StorageAdapter {
  load(signal: AbortSignal): Promise<Record<string, unknown>>;
  save(signal: AbortSignal): Promise<Record<string, unknown>>;
  commit(
    result: Record<string, unknown>,
    signal: AbortSignal,
  ): Promise<{ confirmedSha: string }>;
}

/** Keep storage authorization in the host; no platform SDK enters the runtime. */
export async function attachStorage(
  socketPath: string,
  registration: Registration,
  adapter: StorageAdapter,
) {
  const socket = connect(socketPath + ".auth");
  const life = new AbortController();
  let input = "";
  const decoder = new StringDecoder("utf8");
  let ready = false;
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      socket.destroy();
      reject(Error("browser_storage_attach_timeout"));
    }, 30_000);
    socket.once("close", () => {
      life.abort();
      clearTimeout(timer);
      if (!ready) reject(Error("browser_storage_disconnected"));
    });
    socket.on("error", () => {
      if (!ready) reject(Error("browser_storage_unavailable"));
    });
    socket.on("connect", () =>
      socket.write(
        JSON.stringify({
          op: "storage_attach",
          ...registration,
          storage: true,
        }) + "\n",
      ),
    );
    socket.on("data", (chunk) => {
      input += decoder.write(chunk);
      if (input.length > 262_144) return socket.destroy();
      while (input.includes("\n")) {
        const end = input.indexOf("\n");
        let frame: any;
        try {
          frame = JSON.parse(input.slice(0, end));
        } catch {
          socket.destroy();
          return;
        }
        input = input.slice(end + 1);
        if (frame.error) {
          clearTimeout(timer);
          reject(Error(frame.error));
          socket.destroy();
          return;
        }
        if (frame.ready) {
          ready = true;
          clearTimeout(timer);
          resolve();
          continue;
        }
        const signal = AbortSignal.any([life.signal, AbortSignal.timeout(30_000)]);
        const work =
          frame.op === "load"
            ? () => adapter.load(signal)
            : frame.op === "save"
              ? () => adapter.save(signal)
              : frame.op === "commit"
                ? () => adapter.commit(frame.payload, signal)
                : () => {
                    throw Error("browser_storage_operation_invalid");
                  };
        Promise.resolve()
          .then(work)
          .then(
            (result) => {
              if (!socket.destroyed)
                socket.write(
                  JSON.stringify({ requestId: frame.requestId, result }) + "\n",
                );
            },
            () => {
              if (!socket.destroyed)
                socket.write(
                  JSON.stringify({
                    requestId: frame.requestId,
                    error: "browser_profile_storage_unavailable",
                  }) + "\n",
                );
            },
          );
      }
    });
  });
  return {
    close() {
      life.abort();
      socket.destroy();
    },
  };
}
