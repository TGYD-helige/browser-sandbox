import type { ControlPeer } from "./network-control.js";
import type { SessionStatus } from "./session.js";
import type { ControlState } from "./takeover.js";

export const WORKER_PROTOCOL = "browser-sandbox.worker/v2";
export interface WorkerStatus {
  protocol: typeof WORKER_PROTOCOL;
  session: SessionStatus;
  takeover: ControlState;
  pendingAuth: number;
}

export type IdleProtection = "unknown" | "auth" | "takeover" | "clients" | "retention";

/** One conservative classification drives controller reporting and the worker's atomic recheck. */
export function workerIdleProtection(
  value: unknown,
  now = Date.now(),
  allowLegacy = false,
): IdleProtection | undefined {
  const status = value as WorkerStatus;
  if (
    !status ||
    (status.protocol !== WORKER_PROTOCOL &&
      !(allowLegacy && (status.protocol as string) === "browser-sandbox.worker/v1"))
  )
    return "unknown";
  const session = status.session;
  if (
    !session ||
    !["unused", "live", "retained", "expired", "stopped", "lost"].includes(
      session.state,
    ) ||
    !Number.isSafeInteger(session.connectedClients) ||
    session.connectedClients < 0 ||
    !Number.isSafeInteger(status.pendingAuth) ||
    status.pendingAuth < 0 ||
    !["automation", "stopping", "human", "uncertain"].includes(status.takeover)
  )
    return "unknown";
  if (status.pendingAuth) return "auth";
  if (status.takeover !== "automation") return "takeover";
  if (session.connectedClients) return "clients";
  if (session.state === "retained") {
    if (!Number.isSafeInteger(session.expiresAt)) return "unknown";
    if (session.expiresAt > now) return "retention";
  }
}

export function workerIdle(value: unknown, now = Date.now()): boolean {
  return workerIdleProtection(value, now) === undefined;
}

/** Epoch zero preserves single-controller clients; seeing a fenced owner permanently disables it. */
export function createWorkerFence(
  dispatch: (request: any, peer: ControlPeer, signal: AbortSignal) => Promise<any>,
) {
  let highest = 0;
  let ownership = new AbortController();
  let peers = new WeakMap<ControlPeer, ControlPeer>();
  return async (request: any, peer: ControlPeer, signal: AbortSignal) => {
    const epoch = request.epoch ?? 0;
    if (!Number.isSafeInteger(epoch) || epoch < 0)
      throw Error("browser_controller_epoch_invalid");
    if (epoch < highest) throw Error("browser_controller_fenced");
    if (epoch > highest) {
      highest = epoch;
      ownership.abort();
      ownership = new AbortController();
      peers = new WeakMap();
    }
    const check = () => {
      if (epoch !== highest) throw Error("browser_controller_fenced");
    };
    const ownerSignal = AbortSignal.any([peer.signal, ownership.signal]);
    let fencedPeer = peers.get(peer);
    if (!fencedPeer) {
      fencedPeer = {
        signal: ownerSignal,
        callback: async (op, payload, callbackSignal) => {
          check();
          const result = await peer.callback(
            op,
            payload,
            AbortSignal.any([ownerSignal, ...(callbackSignal ? [callbackSignal] : [])]),
          );
          check();
          return result;
        },
      };
      peers.set(peer, fencedPeer);
    }
    const result = await dispatch(
      request,
      fencedPeer,
      AbortSignal.any([signal, ownerSignal]),
    );
    check();
    return result;
  };
}
