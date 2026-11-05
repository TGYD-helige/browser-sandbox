import { randomUUID } from "node:crypto";
import { hostRequest } from "../dist/index.js";

// Trusted host only. Stop your agent's browser workers before confirming takeover.
const socket =
  process.env.BROWSER_CONTROL_SOCKET ?? "/run/browser-sandbox/control.sock";
const [operation = "create", executionId = randomUUID()] = process.argv.slice(2);
if (operation === "create") {
  await hostRequest(socket, { op: "register", executionId, executorUid: 100001 });
  const access = await hostRequest(socket, {
    op: "access_create",
    executionId,
    control: true,
    resize: true,
  });
  // These URLs are credentials: display only to the authorized local operator.
  console.log(JSON.stringify(access, null, 2));
} else if (operation === "retain") {
  await hostRequest(socket, { op: "session", executionId, action: "retain" });
} else if (operation === "confirm-stopped") {
  await hostRequest(socket, {
    op: "access_confirm_stopped",
    executionId,
    automationStopped: true,
  });
  console.log("Stop confirmed. The authorized viewer can now request control.");
} else if (operation === "finish") {
  await hostRequest(socket, { op: "finalize", executionId });
} else throw Error("Choose create, retain, confirm-stopped, or finish");
