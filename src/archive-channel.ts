import { randomBytes } from "node:crypto";
import { StringDecoder } from "node:string_decoder";

// The existing root-only control socket also carries optional host storage requests.
export function attachArchiveChannel(entry, socket) {
  const pending = new Map(),
    decoder = new StringDecoder("utf8");
  let input = "";
  const channel = {
    request(op, signal, payload = undefined) {
      if (socket.destroyed || signal?.aborted)
        return Promise.reject(Error("browser_profile_storage_unavailable"));
      return new Promise<any>((resolve, reject) => {
        const requestId = randomBytes(16).toString("hex");
        const finish = (error, result = undefined) => {
          clearTimeout(timer);
          signal?.removeEventListener("abort", abort);
          pending.delete(requestId);
          error
            ? reject(Error("browser_profile_storage_unavailable"))
            : resolve(result);
        };
        const abort = () => finish(true),
          timer = setTimeout(abort, 30000);
        pending.set(requestId, finish);
        signal?.addEventListener("abort", abort, { once: true });
        socket.write(JSON.stringify({ requestId, op, payload }) + "\n");
      });
    },
  };
  entry.archiveChannels ??= new Set();
  entry.archiveChannels.add(channel);
  socket.once("close", () => {
    entry.archiveChannels.delete(channel);
    for (const finish of [...pending.values()]) finish(true);
  });
  return (chunk) => {
    input += decoder.write(chunk);
    if (input.length > 262144) return socket.destroy();
    while (input.includes("\n")) {
      const end = input.indexOf("\n"),
        line = input.slice(0, end);
      input = input.slice(end + 1);
      let frame: any;
      try {
        frame = JSON.parse(line);
      } catch {
        return socket.destroy();
      }
      pending.get(frame?.requestId)?.(frame.error, frame.result);
    }
  };
}
export async function requestArchive(entry, op, signal, payload = undefined) {
  for (const channel of [...(entry.archiveChannels ?? [])].reverse()) {
    if (!entry.archiveChannels.has(channel)) continue;
    try {
      const result = await channel.request(op, signal, payload);
      if (!result || typeof result !== "object" || Array.isArray(result))
        throw Error("browser_profile_storage_unavailable");
      return result;
    } catch (error) {
      if (op === "commit") throw error;
      // Only retry transport loss before receiving a grant, never website operations or an upload.
      if (signal?.aborted || entry.archiveChannels.has(channel)) throw error;
    }
  }
  throw Error("browser_profile_storage_unavailable");
}
