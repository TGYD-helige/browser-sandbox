import { randomUUID } from "node:crypto";
import {
  attachStorage,
  hostRequest,
  type Registration,
  type StorageAdapter,
} from "./host-client.js";
import { validExecutorUid } from "./peer-identity.js";

/** One runtime reachable through its trusted host-control channel. */
export interface BrowserRuntime {
  /** Managed runtimes do not use kernel executor UIDs. */
  identity?: "managed";
  request<T = Record<string, unknown>>(
    request: Record<string, unknown>,
    options?: { signal?: AbortSignal; timeoutMs?: number },
  ): Promise<T>;
  attachStorage(
    registration: Registration,
    adapter: StorageAdapter,
  ): ReturnType<typeof attachStorage>;
}

export interface Placement {
  executionId: string;
  /** Isolation key: a tenant-isolating provider never places different tenants in one runtime. */
  tenantId?: string;
  profileId?: string;
}

/** Decides which runtime owns an execution; multi-instance and per-tenant deployments implement this. */
export interface RuntimeProvider {
  identity?: "managed";
  /** Repeated calls for one executionId must return the same runtime. */
  acquire(placement: Placement, signal?: AbortSignal): Promise<BrowserRuntime>;
  /** Called after the runtime confirmed the execution finished, or when registration was refused. */
  release(placement: Placement): Promise<void>;
}

export function createSocketRuntime(socketPath: string): BrowserRuntime {
  return {
    request: (request, options) => hostRequest(socketPath, request, options),
    attachStorage: (registration, adapter) =>
      attachStorage(socketPath, registration, adapter),
  };
}

/** Every execution shares one runtime container and kernel, so tenant placement is refused. */
export function createLocalRuntimeProvider(socketPath: string): RuntimeProvider {
  const runtime = createSocketRuntime(socketPath);
  return {
    async acquire(placement) {
      if (placement.tenantId !== undefined)
        throw Error("browser_tenant_isolation_unavailable");
      return runtime;
    },
    async release() {},
  };
}

export interface BrowserSessionOptions {
  tenantId?: string;
  profileId?: string;
  storage?: StorageAdapter;
  allowedDomains?: string[];
  signal?: AbortSignal;
}

export interface BrowserSessionHandle {
  executionId: string;
  /** Run the harness process as this Linux UID; the endpoint rejects other peers. */
  executorUid?: number;
  wsEndpoint: string;
  /** Trusted host-control channel for session and access requests about this execution. */
  runtime: BrowserRuntime;
  /** Stop the harness processes first; the UID is reused only after confirmed finalization. */
  finalize(signal?: AbortSignal): Promise<Record<string, unknown>>;
}

/** Give every harness session its own execution identity and executor UID. */
export function createBrowserSessions({
  provider,
  firstUid = 100001,
  uidCount = 10000,
}: {
  provider: RuntimeProvider;
  firstUid?: number;
  uidCount?: number;
}) {
  if (
    !Number.isSafeInteger(uidCount) ||
    uidCount < 1 ||
    !validExecutorUid(firstUid) ||
    !validExecutorUid(firstUid + uidCount - 1)
  )
    throw Error("browser_executor_identity_invalid");
  // shortcut: UIDs are tracked in this process, so give each host process its own UID range per runtime.
  const used = new Set<number>();
  const takeUid = () => {
    for (let uid = firstUid; uid < firstUid + uidCount; uid++)
      if (!used.has(uid)) {
        used.add(uid);
        return uid;
      }
    throw Error("browser_executor_uids_exhausted");
  };
  return {
    async open(options: BrowserSessionOptions = {}): Promise<BrowserSessionHandle> {
      const { tenantId, profileId, allowedDomains, signal } = options;
      const placement: Placement = { executionId: randomUUID(), tenantId, profileId };
      const registration: Registration = {
        executionId: placement.executionId,
        executorUid: provider.identity === "managed" ? undefined : takeUid(),
        profileId,
        allowedDomains,
      };
      let runtime: BrowserRuntime | undefined;
      let storage: Awaited<ReturnType<typeof attachStorage>> | undefined;
      let finishing: Promise<Record<string, unknown>> | undefined;
      const finish = async (finalizeSignal?: AbortSignal) => {
        const result = await runtime.request(
          { op: "finalize", executionId: placement.executionId },
          { signal: finalizeSignal },
        );
        storage?.close();
        await provider.release(placement);
        used.delete(registration.executorUid);
        return result;
      };
      try {
        runtime = await provider.acquire(placement, signal);
        if (runtime.identity === "managed") {
          used.delete(registration.executorUid);
          registration.executorUid = undefined;
        } else if (registration.executorUid === undefined)
          registration.executorUid = takeUid();
        signal?.throwIfAborted();
        if (options.storage)
          storage = await runtime.attachStorage(registration, options.storage);
        const { wsEndpoint } = await runtime.request<{ wsEndpoint: string }>(
          {
            op: "register",
            ...registration,
            ...(runtime.identity === "managed" && tenantId !== undefined
              ? { tenantId }
              : {}),
            storage: storage ? true : undefined,
          },
          { signal },
        );
        return {
          executionId: placement.executionId,
          executorUid: registration.executorUid,
          wsEndpoint,
          runtime,
          finalize(finalizeSignal) {
            finishing ??= finish(finalizeSignal).catch((error) => {
              finishing = undefined;
              throw error;
            });
            return finishing;
          },
        };
      } catch (error) {
        // The host needs the identity to retry release or finalization through the provider.
        const unconfirmed = (code: string, cleanupError: unknown) =>
          Object.assign(new AggregateError([error, cleanupError], code), {
            executionId: placement.executionId,
            executorUid: registration.executorUid,
          });
        if (!runtime) {
          used.delete(registration.executorUid);
          throw error;
        }
        // Another host process or an earlier run holds this UID, so keep it out of this pool.
        if (error?.message === "browser_executor_uid_in_use") {
          storage?.close();
          await provider.release(placement).catch((releaseError) => {
            throw unconfirmed(error.message, releaseError);
          });
          throw error;
        }
        // A registration may exist even when its reply was lost; keep the UID until finalization confirms.
        await finish().catch((cleanupError) => {
          storage?.close();
          throw unconfirmed("browser_session_cleanup_unconfirmed", cleanupError);
        });
        throw error;
      }
    },
  };
}
