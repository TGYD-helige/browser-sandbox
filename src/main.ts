import { logWorkerLifecycle } from "./worker-lifecycle.js";
import { WORKER_PROTOCOL } from "./worker-protocol.js";

const role = process.argv[2] ?? "local";
if (role === "controller") await import("./controller-main.js");
else if (role === "local") await import("./local-main.js");
else if (role === "worker") {
  const started = performance.now();
  logWorkerLifecycle("worker_starting");
  try {
    if (
      process.env.BROWSER_WORKER_PROTOCOL &&
      process.env.BROWSER_WORKER_PROTOCOL !== WORKER_PROTOCOL
    )
      throw Error("browser_worker_protocol_unsupported");
    if (!/^[a-f0-9-]{36}$/.test(process.env.BROWSER_EXECUTION_ID ?? ""))
      throw Error("browser_identity_invalid");
    process.env.CONCURRENT = "1";
    process.env.QUEUED = "0";
    await import("./local-main.js");
    logWorkerLifecycle("worker_ready", { durationMs: performance.now() - started });
  } catch (error) {
    const code =
      error instanceof Error && /^browser_[a-z_]{1,100}$/.test(error.message)
        ? error.message
        : "browser_worker_startup_failed";
    logWorkerLifecycle(
      "worker_start_failed",
      { code, durationMs: performance.now() - started },
      true,
    );
    process.exit(1);
  }
} else throw Error("browser_role_invalid");
