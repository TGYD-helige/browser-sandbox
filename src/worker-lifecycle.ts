export function logWorkerLifecycle(
  event: string,
  fields: Record<string, string | number | null> = {},
  failed = false,
) {
  console[failed ? "error" : "log"](JSON.stringify({ event, ...fields }));
}

export function exitWorkerWithFailure(
  event: string,
  fields: Record<string, string | number | null> = {},
): Promise<never> {
  return new Promise(() => {
    const exit = () => process.exit(1);
    // Let stderr drain when possible without letting a stalled log reader keep the worker alive.
    setTimeout(exit, 500);
    process.stderr.once("error", exit);
    process.stderr.write(`${JSON.stringify({ event, ...fields })}\n`, exit);
  });
}
