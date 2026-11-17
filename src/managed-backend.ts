import type { AutomationCoordinator } from "./automation-control.js";
import type { ExecutionRecord } from "./execution-journal.js";
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
  /** Controller epoch persisted on a shared-backend resource. */
  epoch?: number;
  /** Immutable allocation epoch, retained through failed cleanup. */
  creationEpoch?: number;
  /** An owned cleanup resource with no surviving browser process or Profile disk. */
  infrastructureOnly?: boolean;
  runtime: BrowserRuntime & {
    close?(): void;
    setEpoch?(epoch: number): void;
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
  fence?(
    worker: ManagedWorker,
    epoch: number,
    signal: AbortSignal,
    stopped?: boolean,
  ): Promise<void>;
  remove(worker: ManagedWorker, signal: AbortSignal): Promise<void>;
}

export interface ExecutionOwner {
  replica: string;
  origin: string;
  epoch: number;
}

/** Shared ownership contract; platform arbitration and persistence stay in its adapter. */
export interface ExecutionOwnership {
  lookup(id: string): ExecutionOwner | undefined;
  resolve(id: string, create?: boolean): Promise<ExecutionOwner | undefined>;
  assertOwned(id: string): Promise<number>;
  epoch(id: string): number | undefined;
  owns(id: string): boolean;
  onLost(callback: (id: string) => void): void;
  findCapability(token: string): string | undefined;
  indexLegacy(record: ExecutionRecord): Promise<void>;
  acquireProfile(id: string, profileId: string): Promise<void>;
  complete(id: string): Promise<void>;
  refresh(): Promise<string[]>;
  watch(): Promise<void>;
  release(): Promise<void>;
  close(): Promise<void>;
  journal: {
    read(id: string): Promise<ExecutionRecord | undefined>;
    has(id: string): boolean;
    ids(): string[];
    finishedIds(): string[];
    create(id: string, data: Record<string, unknown>): Promise<ExecutionRecord>;
    update(
      id: string,
      revision: number,
      data: Record<string, unknown>,
    ): Promise<ExecutionRecord>;
  };
}
