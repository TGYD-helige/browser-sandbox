import type { AutomationCoordinator } from "./automation-control.js";
import type { BrowserRuntime, Placement } from "./runtime-provider.js";

export const WORKER_ENVIRONMENT = [
  "TIMEOUT",
  "TZ",
  "BROWSER_STARTUP_TIMEOUT",
  "LANG",
  "BROWSER_STORAGE_EGRESS",
  "BROWSER_PROFILE_STORE_ORIGIN",
] as const;
export const WORKER_UID = 100001;

/** Exact backend incarnation. Private addresses never leave the controller. */
export interface ManagedWorker {
  id: string;
  executionId: string;
  incarnation: string;
  origin: string;
  profileId?: string;
  runtime: BrowserRuntime & {
    close?(): void;
    setAutomation?(automation: AutomationCoordinator): void;
  };
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
  /** A failed discovery is unknown ownership; include stopped resources awaiting cleanup. */
  list?(signal: AbortSignal): Promise<ManagedWorker[]>;
  remove(worker: ManagedWorker, signal: AbortSignal): Promise<void>;
}
