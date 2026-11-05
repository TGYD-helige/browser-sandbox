import type { Execution } from "./executions.js";

export interface AutomationCoordinator {
  pause(executionId: string, signal: AbortSignal): Promise<void>;
  resume(executionId: string, signal: AbortSignal): Promise<void>;
}

export async function waitForControl<T>(
  work: Promise<T>,
  signal: AbortSignal,
): Promise<T> {
  let cancel: () => void;
  try {
    const result = await Promise.race([
      work,
      new Promise<never>((_, reject) => {
        cancel = () => reject(Error("browser_control_timeout"));
        signal.addEventListener("abort", cancel, { once: true });
        if (signal.aborted) cancel();
      }),
    ]);
    signal.throwIfAborted();
    return result;
  } finally {
    signal.removeEventListener("abort", cancel);
  }
}

/** A transition includes the host callback; a late resume cannot overlap a new pause. */
export function createAutomationControl(automation?: AutomationCoordinator) {
  const transitions = new WeakMap<Execution, Promise<unknown>>();
  const confirmations = new WeakMap<Execution, string>();
  const generation = (entry: Execution) =>
    entry.session.status(entry.peers.size).generation;
  const confirmed = (entry: Execution) =>
    confirmations.has(entry) && confirmations.get(entry) === generation(entry);
  return {
    confirm(entry: Execution) {
      confirmations.set(entry, generation(entry));
    },
    canPause: (entry: Execution) =>
      !transitions.has(entry) && (!!automation || confirmed(entry)),
    async pause(entry: Execution, signal: AbortSignal) {
      if (automation) await automation.pause(entry.executionId, signal);
      else {
        const stopped = confirmed(entry);
        confirmations.delete(entry);
        if (!stopped) throw Error("browser_automation_stop_required");
      }
      signal.throwIfAborted();
    },
    async resume(entry: Execution, signal: AbortSignal) {
      if (!automation) return false;
      await automation.resume(entry.executionId, signal);
      signal.throwIfAborted();
      return true;
    },
    run<T>(
      entry: Execution,
      signal: AbortSignal,
      operation: () => Promise<T>,
    ): Promise<T> {
      signal.throwIfAborted();
      if (transitions.has(entry)) throw Error("browser_control_busy");
      const work = Promise.resolve().then(() => {
        signal.throwIfAborted();
        return operation();
      });
      transitions.set(entry, work);
      const cleanup = () => {
        if (transitions.get(entry) === work) transitions.delete(entry);
      };
      // Keep the lock until the underlying callback settles, even if the caller times out.
      work.finally(cleanup).catch(() => {});
      return waitForControl(work, signal);
    },
  };
}
