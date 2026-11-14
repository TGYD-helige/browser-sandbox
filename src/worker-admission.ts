export interface WorkerSlot {
  /** Aborts creation only; the controller retains the slot until owned cleanup completes. */
  signal: AbortSignal;
  ready(): void;
  /** Creation has stopped, but unconfirmed infrastructure still owns capacity. */
  retain(): void;
  release(): void;
}

/** Capacity belongs to the controller; backend adapters do not schedule workers. */
export function createWorkerAdmission({
  maxWorkers,
  maxPending,
  maxQueued,
  creationTimeoutMs,
}: {
  maxWorkers: number;
  maxPending: number;
  maxQueued: number;
  creationTimeoutMs: number;
}) {
  if (
    ![maxWorkers, maxPending, creationTimeoutMs].every(
      (value) => Number.isSafeInteger(value) && value > 0,
    ) ||
    !Number.isSafeInteger(maxQueued) ||
    maxQueued < 0 ||
    creationTimeoutMs > 2_147_483_647
  )
    throw Error("browser_worker_capacity_invalid");
  let workers = 0;
  let pending = 0;
  const queue: Array<() => void> = [];
  const available = () => workers < maxWorkers && pending < maxPending;
  const drain = () => {
    while (available() && queue.length) queue.shift()();
  };
  return {
    status: () => ({ workers, pending, queued: queue.length }),
    // Restored workers are already ready; no creation deadline applies.
    restore(): WorkerSlot {
      workers++;
      let released = false;
      return {
        signal: new AbortController().signal,
        ready() {},
        retain() {},
        release() {
          if (released) return;
          released = true;
          workers--;
          drain();
        },
      };
    },
    acquire(signal?: AbortSignal): Promise<WorkerSlot> {
      if (signal?.aborted) return Promise.reject(signal.reason);
      if (!available() && queue.length >= maxQueued)
        return Promise.reject(Error("browser_worker_queue_full"));
      return new Promise((resolve, reject) => {
        const abort = new AbortController();
        let started = false;
        let creating = true;
        let released = false;
        const detach = () => {
          clearTimeout(timer);
          signal?.removeEventListener("abort", cancel);
        };
        const cancel = () => {
          abort.abort(
            signal?.aborted ? signal.reason : Error("browser_worker_creation_timeout"),
          );
          detach();
          if (started) return;
          const index = queue.indexOf(start);
          if (index >= 0) queue.splice(index, 1);
          reject(abort.signal.reason);
        };
        const timer = setTimeout(cancel, creationTimeoutMs);
        const start = () => {
          started = true;
          workers++;
          pending++;
          const retain = () => {
            if (!creating || released) return;
            creating = false;
            pending--;
            detach();
            drain();
          };
          resolve({
            signal: abort.signal,
            ready() {
              abort.signal.throwIfAborted();
              retain();
            },
            retain,
            release() {
              if (released) return;
              released = true;
              workers--;
              if (creating) pending--;
              detach();
              drain();
            },
          });
        };
        signal?.addEventListener("abort", cancel, { once: true });
        if (available()) start();
        else queue.push(start);
      });
    },
  };
}
