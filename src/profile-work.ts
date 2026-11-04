/** Bound disk/network preparation separately from Browserless browser admission. */
export function createProfileWork(limit: number, maxCleanups: number) {
  let active = 0;
  type Pending = { start(): void; cancel(): void };
  const waiting: Pending[] = [],
    cleanups: Pending[] = [];
  async function run<T>(
    work: () => Promise<T>,
    signal?: AbortSignal,
    cleanup = false,
  ): Promise<T> {
    signal?.throwIfAborted();
    await new Promise<void>((resolve, reject) => {
      if (active < limit) {
        active++;
        resolve();
        return;
      }
      const queue = cleanup ? cleanups : waiting;
      if (queue.length >= (cleanup ? maxCleanups : limit * 2)) {
        reject(Error("browser_profile_work_queue_full"));
        return;
      }
      const pending = {
        start() {
          signal?.removeEventListener("abort", pending.cancel);
          active++;
          resolve();
        },
        cancel() {
          const index = queue.indexOf(pending);
          if (index < 0) return;
          queue.splice(index, 1);
          signal?.removeEventListener("abort", pending.cancel);
          reject(Error("browser_profile_work_cancelled"));
        },
      };
      queue.push(pending);
      signal?.addEventListener("abort", pending.cancel, { once: true });
      if (signal?.aborted) pending.cancel();
    });
    try {
      signal?.throwIfAborted();
      return await work();
    } finally {
      active--;
      (cleanups.shift() ?? waiting.shift())?.start();
    }
  }
  return {
    run,
    // Ownership recovery must remain admissible when ordinary preparation is full.
    cleanup<T>(work: () => Promise<T>, signal?: AbortSignal) {
      return run(work, signal, true);
    },
  };
}
