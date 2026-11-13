import type { SessionStatus } from "./session.js";
import type { ControlState } from "./takeover.js";

export const WORKER_PROTOCOL = "browser-sandbox.worker/v1";
export interface WorkerStatus {
  protocol: typeof WORKER_PROTOCOL;
  session: SessionStatus;
  takeover: ControlState;
  pendingAuth: number;
}

/** Shared by controller preflight and the worker's atomic idle-finalize check. Unknown state keeps data. */
export function workerIdle(value: unknown, now = Date.now()): boolean {
  const status = value as WorkerStatus;
  if (!status || status.protocol !== WORKER_PROTOCOL) return false;
  const session = status.session;
  if (
    !session ||
    !["unused", "live", "retained", "expired", "stopped", "lost"].includes(
      session.state,
    ) ||
    !Number.isSafeInteger(session.connectedClients) ||
    session.connectedClients !== 0 ||
    status.takeover !== "automation" ||
    status.pendingAuth !== 0
  )
    return false;
  if (session.state === "retained")
    return Number.isSafeInteger(session.expiresAt) && session.expiresAt <= now;
  return true;
}
