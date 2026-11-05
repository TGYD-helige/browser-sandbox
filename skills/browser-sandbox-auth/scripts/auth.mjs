import { realpath } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const statuses = new Set([
  "submitted",
  "declined",
  "cancelled",
  "expired",
  "page_changed",
  "origin_changed",
  "locator_invalid",
  "submission_failed",
  "unavailable",
]);
const keysOnly = (value, keys) =>
  value &&
  typeof value === "object" &&
  !Array.isArray(value) &&
  Object.keys(value).every((key) => keys.includes(key));

/** Portable transport shared with the SDK; authority and form validation stay in the runtime. */
export async function requestAuth(endpoint, request, signal) {
  if (
    !keysOnly(request, [
      "targetId",
      "origin",
      "fields",
      "options",
      "submit",
      "timeoutMs",
    ]) ||
    !Array.isArray(request.fields) ||
    request.fields.length > 8 ||
    request.fields.some(
      (field) => !keysOnly(field, ["id", "label", "type", "selector", "required"]),
    ) ||
    (request.options !== undefined &&
      (!Array.isArray(request.options) ||
        request.options.length > 8 ||
        request.options.some(
          (option) => !keysOnly(option, ["id", "label", "selector"]),
        ))) ||
    (request.submit !== undefined && !keysOnly(request.submit, ["selector"]))
  )
    throw Error("browser_auth_request_invalid");
  const timeoutMs = request.timeoutMs ?? 300_000;
  if (!Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 600_000)
    throw Error("browser_auth_request_invalid");
  const body = JSON.stringify(request);
  if (Buffer.byteLength(body) > 8192) throw Error("browser_auth_request_invalid");
  let url;
  try {
    url = new URL(endpoint);
  } catch {
    throw Error("browser_auth_endpoint_invalid");
  }
  if (
    !["ws:", "wss:"].includes(url.protocol) ||
    !/^\/execution\/[a-f0-9]{64}$/.test(url.pathname) ||
    url.search ||
    url.hash ||
    url.username ||
    url.password
  )
    throw Error("browser_auth_endpoint_invalid");
  url.protocol = url.protocol === "wss:" ? "https:" : "http:";
  url.pathname += "/auth";
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body,
    redirect: "error",
    signal: AbortSignal.any([
      AbortSignal.timeout(timeoutMs + 20_000),
      ...(signal ? [signal] : []),
    ]),
  });
  let size = 0;
  const chunks = [];
  for await (const chunk of response.body ?? []) {
    size += chunk.byteLength;
    if (size > 8192) throw Error("browser_auth_response_invalid");
    chunks.push(chunk);
  }
  let value;
  try {
    value = JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    throw Error("browser_auth_response_invalid");
  }
  if (!response.ok)
    throw Error(
      /^browser_[a-z_]+$/.test(value?.error)
        ? value.error
        : "browser_auth_request_failed",
    );
  if (
    !value ||
    typeof value.requestId !== "string" ||
    !statuses.has(value.status) ||
    typeof value.controlReleased !== "boolean" ||
    typeof value.resumed !== "boolean"
  )
    throw Error("browser_auth_response_invalid");
  return {
    requestId: value.requestId,
    status: value.status,
    controlReleased: value.controlReleased,
    resumed: value.resumed,
    ...(typeof value.selectedOption === "string"
      ? { selectedOption: value.selectedOption }
      : {}),
  };
}

async function main() {
  if (process.argv.length !== 2) throw Error("browser_auth_usage_invalid");
  let size = 0;
  const chunks = [];
  for await (const chunk of process.stdin) {
    size += chunk.byteLength;
    if (size > 8192) throw Error("browser_auth_request_invalid");
    chunks.push(chunk);
  }
  let request;
  try {
    request = JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    throw Error("browser_auth_request_invalid");
  }
  const result = await requestAuth(process.env.BROWSER_WS_ENDPOINT, request);
  process.stdout.write(`${JSON.stringify(result)}\n`);
}

if (
  process.argv[1] &&
  (await realpath(process.argv[1]).catch(() => null)) === fileURLToPath(import.meta.url)
) {
  try {
    await main();
  } catch (error) {
    const code = /^browser_[a-z_]+$/.test(error?.message)
      ? error.message
      : "browser_auth_request_failed";
    process.stderr.write(`${JSON.stringify({ error: code })}\n`);
    process.exitCode = 1;
  }
}
