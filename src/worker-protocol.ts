import type { SessionStatus } from "./session.js";
import type { ControlState } from "./takeover.js";

export const WORKER_PROTOCOL = "browser-sandbox.worker/v1";
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
): IdleProtection | undefined {
  const status = value as WorkerStatus;
  if (!status || status.protocol !== WORKER_PROTOCOL) return "unknown";
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
