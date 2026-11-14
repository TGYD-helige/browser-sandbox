import { attachStorage, type Registration } from "./host-client.js";
import { WORKER_UID } from "./managed-backend.js";
import type { ControlPeer } from "./network-control.js";

/** One worker owns one Unix storage bridge, including pending attachment and revocation. */
export function createWorkerStorage(socketPath: string) {
  let current: {
    peer: ControlPeer;
    channel?: Awaited<ReturnType<typeof attachStorage>>;
    closed: boolean;
    pending: boolean;
    close(): void;
  };
  return async (
    request: Registration & { op: string },
    peer: ControlPeer,
    signal?: AbortSignal,
  ): Promise<{ attached: true } | { detached: true } | undefined> => {
    if (request.op === "storage_detach") {
      if (current && current.peer !== peer)
        throw Error("browser_host_authority_required");
      current?.close();
      return { detached: true };
    }
    if (request.op !== "storage_attach") return;
    signal?.throwIfAborted();
    if (peer.signal.aborted) throw Error("browser_storage_disconnected");
    if (current) throw Error("browser_storage_already_attached");
    const setup = new AbortController();
    const entry: typeof current = {
      peer,
      closed: false,
      pending: true,
      close() {
        if (entry.closed) return;
        entry.closed = true;
        setup.abort();
        entry.channel?.close();
        peer.signal.removeEventListener("abort", entry.close);
        signal?.removeEventListener("abort", entry.close);
        if (!entry.pending && current === entry) current = undefined;
      },
    };
    current = entry;
    peer.signal.addEventListener("abort", entry.close, { once: true });
    signal?.addEventListener("abort", entry.close, { once: true });
    try {
      if (peer.signal.aborted || signal?.aborted)
        throw Error("browser_storage_disconnected");
      const { executionId } = request;
      const registration = { ...request, executorUid: WORKER_UID, existingOnly: true };
      const channel = await attachStorage(
        socketPath,
        registration,
        {
          load: (signal) => peer.callback("load", { executionId }, signal),
          save: (signal) => peer.callback("save", { executionId }, signal),
          commit: (payload, signal) =>
            peer.callback("commit", { executionId, payload }, signal),
        },
        setup.signal,
      );
      if (entry.closed) {
        channel.close();
        throw Error("browser_storage_disconnected");
      }
      entry.channel = channel;
      return { attached: true };
    } catch (error) {
      entry.close();
      throw error;
    } finally {
      signal?.removeEventListener("abort", entry.close);
      entry.pending = false;
      if (entry.closed && current === entry) current = undefined;
    }
  };
}
