import { WORKER_PROTOCOL } from "./worker-protocol.js";

const role = process.argv[2] ?? "local";
if (role === "controller") await import("./controller-main.js");
else if (role === "worker" || role === "local") {
  if (role === "worker") {
    if (
      process.env.BROWSER_WORKER_PROTOCOL &&
      process.env.BROWSER_WORKER_PROTOCOL !== WORKER_PROTOCOL
    )
      throw Error("browser_worker_protocol_unsupported");
    if (!/^[a-f0-9-]{36}$/.test(process.env.BROWSER_EXECUTION_ID ?? ""))
      throw Error("browser_identity_invalid");
    process.env.CONCURRENT = "1";
    process.env.QUEUED = "0";
  }
  await import("./local-main.js");
} else throw Error("browser_role_invalid");
