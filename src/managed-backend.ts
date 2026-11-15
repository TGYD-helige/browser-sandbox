import type { AutomationCoordinator } from "./automation-control.js";
import type { BrowserRuntime, Placement } from "./runtime-provider.js";

export const WORKER_ENVIRONMENT = [
  "TIMEOUT",
  "TZ",
  "BROWSER_STARTUP_TIMEOUT",
  "LANG",
] as const;

/** Exact backend incarnation. Private addresses never leave the controller. */
export interface ManagedWorker {
  id: string;
  executionId: string;
  incarnation: string;
  origin: string;
  runtime: BrowserRuntime & { close?(): void };
}

/** Optional platform adapter; create cleans partial resources or throws WorkerCleanupFailure when cleanup is unconfirmed. */
export interface ManagedBackend {
  /** Preserve published workers on controller exit; the backend owns their lifetime backstop. */
  readonly preserveWorkersOnShutdown?: boolean;
  create(
    placement: Placement & {
      incarnation: string;
      allowedDomains?: string[];
      automation?: AutomationCoordinator;
    },
    signal: AbortSignal,
  ): Promise<ManagedWorker>;
  remove(worker: ManagedWorker, signal: AbortSignal): Promise<void>;
}
