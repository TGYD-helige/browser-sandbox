#!/usr/bin/env node
import { type SessionAction, sessionRequest } from "./session.js";

const [action = "status", duration] = process.argv.slice(2);
try {
  if (!["status", "retain", "renew", "release", "stop"].includes(action))
    throw Error("browser_session_action_invalid");
  const endpoint = process.env.BROWSER_WS_ENDPOINT;
  if (!endpoint) throw Error("browser_session_endpoint_missing");
  console.log(
    JSON.stringify(
      await sessionRequest(
        endpoint,
        action as SessionAction,
        duration === undefined ? undefined : Number(duration),
      ),
    ),
  );
} catch (error) {
  console.error(
    /^browser_[a-z_]+$/.test(error.message) ? error.message : "browser_session_failed",
  );
  process.exitCode = 1;
}
