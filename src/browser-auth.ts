import { randomUUID } from "node:crypto";
import { requestAuth } from "./auth-client.mjs";
import { connectCdp } from "./cdp.js";

export type BrowserAuthStatus =
  | "submitted"
  | "declined"
  | "cancelled"
  | "expired"
  | "page_changed"
  | "origin_changed"
  | "locator_invalid"
  | "submission_failed"
  | "unavailable";
export interface BrowserAuthField {
  id: string;
  label: string;
  type: "text" | "email" | "password" | "code";
  selector: string;
  required?: boolean;
}
export interface BrowserAuthOption {
  id: string;
  label: string;
  selector: string;
}
export interface BrowserAuthRequest {
  targetId: string;
  origin: string;
  fields: BrowserAuthField[];
  options?: BrowserAuthOption[];
  submit?: { selector: string };
  timeoutMs?: number;
}
export interface BrowserAuthResult {
  requestId: string;
  status: BrowserAuthStatus;
  selectedOption?: string;
  controlReleased: boolean;
  resumed: boolean;
}
export interface BrowserAuthPrompt {
  requestId: string;
  origin: string;
  targetId: string;
  fields: Omit<BrowserAuthField, "selector">[];
  options?: Omit<BrowserAuthOption, "selector">[];
  expiresAt: number;
  phase: "waiting" | "submitting";
}
export interface BrowserAuthResponse {
  action: "submit" | "cancel";
  values?: Record<string, string>;
  selectedOption?: string;
}

const validText = (value: unknown, max: number) =>
  typeof value === "string" && value.length > 0 && value.length <= max;
export function validateAuthRequest(
  value: any,
  allowHttpLoopback = false,
): BrowserAuthRequest {
  if (
    !value ||
    typeof value !== "object" ||
    Array.isArray(value) ||
    !validText(value.targetId, 128) ||
    !Array.isArray(value.fields) ||
    value.fields.length > 8
  )
    throw Error("browser_auth_request_invalid");
  let origin: URL;
  try {
    origin = new URL(value.origin);
  } catch {
    throw Error("browser_auth_origin_invalid");
  }
  if (
    origin.origin !== value.origin ||
    (origin.protocol !== "https:" &&
      !(
        allowHttpLoopback &&
        origin.protocol === "http:" &&
        ["localhost", "127.0.0.1", "[::1]"].includes(origin.hostname)
      ))
  )
    throw Error("browser_auth_origin_invalid");
  const ids = new Set<string>();
  const fields = value.fields.map((field: any) => {
    if (
      !field ||
      typeof field.id !== "string" ||
      !/^[A-Za-z][A-Za-z0-9_-]{0,63}$/.test(field.id) ||
      ids.has(field.id) ||
      !validText(field.label, 80) ||
      !validText(field.selector, 512) ||
      !["text", "email", "password", "code"].includes(field.type) ||
      (field.required !== undefined && typeof field.required !== "boolean") ||
      Object.keys(field).some(
        (key) => !["id", "label", "type", "selector", "required"].includes(key),
      )
    )
      throw Error("browser_auth_request_invalid");
    ids.add(field.id);
    return {
      id: field.id,
      label: field.label,
      type: field.type,
      selector: field.selector,
      required: field.required !== false,
    };
  });
  let options: BrowserAuthOption[];
  if (value.options !== undefined) {
    if (
      !Array.isArray(value.options) ||
      !value.options.length ||
      value.options.length > 8 ||
      fields.length ||
      value.submit
    )
      throw Error("browser_auth_request_invalid");
    options = value.options.map((option: any) => {
      if (
        !option ||
        typeof option.id !== "string" ||
        !/^[A-Za-z][A-Za-z0-9_-]{0,63}$/.test(option.id) ||
        ids.has(option.id) ||
        !validText(option.label, 80) ||
        !validText(option.selector, 512) ||
        Object.keys(option).some((key) => !["id", "label", "selector"].includes(key))
      )
        throw Error("browser_auth_request_invalid");
      ids.add(option.id);
      return { id: option.id, label: option.label, selector: option.selector };
    });
  }
  if (!fields.length && !options) throw Error("browser_auth_request_invalid");
  if (
    value.submit !== undefined &&
    (!value.submit ||
      !validText(value.submit.selector, 512) ||
      Object.keys(value.submit).some((key) => key !== "selector"))
  )
    throw Error("browser_auth_request_invalid");
  if (
    value.timeoutMs !== undefined &&
    (!Number.isInteger(value.timeoutMs) ||
      value.timeoutMs < 1 ||
      value.timeoutMs > 600_000)
  )
    throw Error("browser_auth_request_invalid");
  if (
    Object.keys(value).some(
      (key) =>
        !["targetId", "origin", "fields", "options", "submit", "timeoutMs"].includes(
          key,
        ),
    )
  )
    throw Error("browser_auth_request_invalid");
  return {
    targetId: value.targetId,
    origin: origin.origin,
    fields,
    options,
    submit: value.submit ? { selector: value.submit.selector } : undefined,
    timeoutMs: value.timeoutMs ?? 300_000,
  };
}

/** A private isolated world keeps credential values out of request results and diagnostics. */
export async function prepareBrowserAuth(
  endpoint: string,
  request: BrowserAuthRequest,
  signal: AbortSignal,
  invalidate: (status: BrowserAuthStatus) => void,
) {
  const cdp = await connectCdp(endpoint, signal);
  let waiting = true;
  try {
    const { targetInfos } = await cdp.call("Target.getTargets");
    const target = targetInfos.find(
      (item: any) => item.type === "page" && item.targetId === request.targetId,
    );
    if (!target || new URL(target.url).origin !== request.origin)
      throw Error("browser_auth_origin_changed");
    const { sessionId } = await cdp.call("Target.attachToTarget", {
      targetId: request.targetId,
      flatten: true,
    });
    const {
      frameTree: { frame },
    } = await cdp.call("Page.getFrameTree", {}, sessionId);
    if (new URL(frame.url).origin !== request.origin)
      throw Error("browser_auth_origin_changed");
    const { executionContextId } = await cdp.call(
      "Page.createIsolatedWorld",
      { frameId: frame.id, worldName: `browser-auth-${randomUUID()}` },
      sessionId,
    );
    async function call(fn: string, args: any[] = []) {
      const result = await cdp.call(
        "Runtime.callFunctionOn",
        {
          executionContextId,
          functionDeclaration: fn,
          arguments: args.map((value) => ({ value })),
          returnByValue: true,
        },
        sessionId,
      );
      if (result.exceptionDetails) throw Error("browser_auth_page_changed");
      return result.result?.value;
    }
    const prepared = await call(
      `function(spec) {
      const visible = (node) => node instanceof HTMLElement && node.isConnected && node.getClientRects().length > 0 && getComputedStyle(node).visibility !== 'hidden' && getComputedStyle(node).display !== 'none';
      const find = (selector) => { let nodes; try { nodes = document.querySelectorAll(selector); } catch { return null; } return nodes.length === 1 && visible(nodes[0]) ? nodes[0] : null; };
      const fields = {};
      for (const field of spec.fields) {
        const node = find(field.selector);
        const accepted = field.type === 'password' ? ['password'] : field.type === 'email' ? ['text','email'] : ['text','tel','number','email'];
        if (!(node instanceof HTMLInputElement) || node.disabled || node.readOnly || !accepted.includes(node.type)) return false;
        fields[field.id] = node;
      }
      const choices = {};
      for (const option of spec.options ?? []) { const node = find(option.selector); if (!node || node.disabled || !node.matches('button,a,input[type=button],input[type=submit],[role=button]')) return false; choices[option.id] = node; }
      const submit = spec.submit ? find(spec.submit.selector) : null;
      if (spec.submit && (!submit || !submit.matches('button,input[type=submit],[role=button]'))) return false;
      globalThis.__browserAuth = { fields, choices, submit, visible };
      return true;
    }`,
      [request],
    );
    if (!prepared) throw Error("browser_auth_locator_invalid");
    cdp.subscribe((event) => {
      if (!waiting) return;
      if (
        event.method === "Target.detachedFromTarget" &&
        event.params.sessionId === sessionId
      )
        invalidate("page_changed");
      if (
        event.sessionId === sessionId &&
        event.method === "Page.frameNavigated" &&
        !event.params.frame.parentId &&
        event.params.frame.loaderId !== frame.loaderId
      ) {
        let status: BrowserAuthStatus = "page_changed";
        try {
          if (new URL(event.params.frame.url).origin !== request.origin)
            status = "origin_changed";
        } catch {}
        invalidate(status);
      }
    });
    cdp.closed.then(() => {
      if (waiting && !signal.aborted) invalidate("page_changed");
    });
    await cdp.call("Page.enable", {}, sessionId);
    return {
      async submit(response: BrowserAuthResponse) {
        waiting = false;
        const current = (await cdp.call("Page.getFrameTree", {}, sessionId)).frameTree
          .frame;
        if (new URL(current.url).origin !== request.origin)
          return "origin_changed" as const;
        if (current.loaderId !== frame.loaderId) return "page_changed" as const;
        if (request.options) {
          const selected = request.options.find(
            (option) => option.id === response.selectedOption,
          );
          if (!selected) throw Error("browser_auth_response_invalid");
        } else {
          const values = response.values;
          if (
            !values ||
            typeof values !== "object" ||
            Array.isArray(values) ||
            Object.keys(values).some(
              (id) => !request.fields.some((field) => field.id === id),
            ) ||
            request.fields.some(
              (field) =>
                (field.required &&
                  (!Object.hasOwn(values, field.id) || !values[field.id])) ||
                (Object.hasOwn(values, field.id) &&
                  (typeof values[field.id] !== "string" ||
                    values[field.id].length > 2000)),
            ) ||
            Buffer.byteLength(JSON.stringify(values)) > 8192
          )
            throw Error("browser_auth_response_invalid");
          const filled = await call(
            `function(spec, values, expectedOrigin) {
            if (location.origin !== expectedOrigin) return false;
            const auth = globalThis.__browserAuth;
            for (const field of spec.fields) {
              const node = auth.fields[field.id];
              if (!auth.visible(node) || node.disabled || node.readOnly || node.ownerDocument !== document || location.origin !== expectedOrigin) return false;
              if (!Object.hasOwn(values, field.id)) continue;
              Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set.call(node, values[field.id]);
              node.dispatchEvent(new Event('input', { bubbles: true, composed: true }));
              node.dispatchEvent(new Event('change', { bubbles: true }));
            }
            return true;
          }`,
            [request, values, request.origin],
          );
          if (!filled) return "page_changed" as const;
        }
        if (request.submit || request.options) {
          const position = await call(
            `function(option, expectedOrigin) {
            if (location.origin !== expectedOrigin) return null;
            const auth = globalThis.__browserAuth;
            const node = option ? auth.choices[option] : auth.submit;
            if (!auth.visible(node) || node.disabled || node.ownerDocument !== document) return null;
            node.scrollIntoView({ block: 'center', inline: 'center' });
            const rect = node.getBoundingClientRect();
            const x = rect.left + rect.width / 2, y = rect.top + rect.height / 2;
            if (x < 0 || y < 0 || x >= innerWidth || y >= innerHeight || !node.contains(document.elementFromPoint(x, y))) return null;
            return { x, y };
          }`,
            [response.selectedOption ?? null, request.origin],
          );
          if (!position) return "submission_failed" as const;
          for (const type of ["mousePressed", "mouseReleased"])
            await cdp.call(
              "Input.dispatchMouseEvent",
              { type, ...position, button: "left", clickCount: 1 },
              sessionId,
            );
        }
        return "submitted" as const;
      },
      async close() {
        waiting = false;
        cdp.close();
        await cdp.closed;
      },
    };
  } catch (error) {
    waiting = false;
    cdp.close();
    await cdp.closed;
    throw error;
  }
}

/** Await human submission; the result deliberately contains no entered field values. */
export async function browserAuthRequest(
  endpoint: string,
  request: BrowserAuthRequest,
  signal?: AbortSignal,
): Promise<BrowserAuthResult> {
  return requestAuth(endpoint, validateAuthRequest(request, true), signal);
}
