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
  /** An owned cleanup resource with no surviving browser process or Profile disk. */
  infrastructureOnly?: boolean;
  runtime: BrowserRuntime & {
    close?(): void;
    setEpoch?(epoch: number, ownerUid?: string): void;
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
  list?(signal: AbortSignal, executionId?: string): Promise<ManagedWorker[]>;
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
  uid: string;
  finished?: boolean;
  created?: boolean;
}

/** Shared ownership contract; platform arbitration and persistence stay in its adapter. */
export interface ExecutionOwnership {
  lookup(id: string): ExecutionOwner | undefined;
  /** Cached unfinished executions; refresh() relists them from the platform. */
  active(): string[];
  needsRelist(): boolean;
  resolve(
    id: string,
    create?: boolean,
    signal?: AbortSignal,
  ): Promise<ExecutionOwner | undefined>;
  assertOwned(id: string): Promise<{ epoch: number; uid: string }>;
  epoch(id: string): number | undefined;
  owns(id: string): boolean;
  onLost(callback: (id: string) => void): void;
  findCapability(token: string): string | undefined;
  indexLegacy(record: ExecutionRecord): Promise<void>;
  acquireProfile(id: string, profileId: string): Promise<void>;
  /** Run volume maintenance only while the durable Profile Lease is free. */
  withUnleasedProfile<T>(
    profileId: string,
    work: () => Promise<T>,
    signal?: AbortSignal,
  ): Promise<T | undefined>;
  discardRegistration(
    id: string,
    owner: ExecutionOwner,
    incarnation?: string,
  ): Promise<void>;
  complete(id: string): Promise<boolean>;
  refresh(): Promise<string[]>;
  watch(): Promise<void>;
  release(): Promise<void>;
  close(): Promise<void>;
  journal: {
    read(id: string): Promise<ExecutionRecord | undefined>;
    create(id: string, data: Record<string, unknown>): Promise<ExecutionRecord>;
    update(
      id: string,
      revision: number,
      data: Record<string, unknown>,
    ): Promise<ExecutionRecord>;
  };
}
