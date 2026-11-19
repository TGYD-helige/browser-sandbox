/** Serialize work for each key without retaining settled operations. */
export function createSerialWork() {
  const pending = new Map<string, Promise<unknown>>();
  return <T>(key: string, action: () => Promise<T>): Promise<T> => {
    const work = (pending.get(key) ?? Promise.resolve()).then(action);
    const settled = work
      .catch(() => {})
      .finally(() => {
        if (pending.get(key) === settled) pending.delete(key);
      });
    pending.set(key, settled);
    return work;
  };
}
