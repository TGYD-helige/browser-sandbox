import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { hostRequest } from "../dist/index.js";

// Run as the trusted host inside the browser's network namespace.
const socket =
  process.env.BROWSER_CONTROL_SOCKET ?? "/run/browser-sandbox/control.sock";
const executionId = randomUUID();
const executorUid = 100001;
const registration = {
  op: "register",
  executionId,
  executorUid,
  ...(process.env.PROFILE_ID ? { profileId: process.env.PROFILE_ID } : {}),
};
const { wsEndpoint } = await hostRequest(socket, registration);
try {
  const client = new URL(process.argv[2] ?? "./raw-cdp.mjs", import.meta.url);
  const child = spawn(process.execPath, [client.pathname], {
    uid: executorUid,
    gid: executorUid,
    env: { PATH: process.env.PATH, BROWSER_WS_ENDPOINT: wsEndpoint },
    stdio: "inherit",
  });
  const [code] = await once(child, "exit");
  if (code !== 0) throw Error("Browser consumer failed");
} finally {
  await hostRequest(socket, { op: "finalize", executionId });
}
