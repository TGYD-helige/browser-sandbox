export function logWorkerLifecycle(
  event: string,
  fields: Record<string, string | number | null> = {},
  failed = false,
) {
  console[failed ? "error" : "log"](JSON.stringify({ event, ...fields }));
}
