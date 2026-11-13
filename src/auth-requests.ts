import { randomUUID } from "node:crypto";
import type { createAutomationControl } from "./automation-control.js";
import {
  type BrowserAuthPrompt,
  type BrowserAuthResponse,
  type BrowserAuthResult,
  type BrowserAuthStatus,
  prepareBrowserAuth,
  validateAuthRequest,
} from "./browser-auth.js";

/** Human waits live outside execution mutation queues so finish and cancellation remain available. */
export function createAuthRequests(
  executions: any,
  automationControl: ReturnType<typeof createAutomationControl>,
  allowHttpLoopback = false,
) {
  const pending = new Map<
    any,
    { prompt?: BrowserAuthPrompt; respond(response: BrowserAuthResponse): void }
  >();
  return {
    prompt: (entry: any) => pending.get(entry)?.prompt,
    respond(entry: any, requestId: string, response: BrowserAuthResponse) {
      const request = pending.get(entry);
      if (
        !request?.prompt ||
        request.prompt.requestId !== requestId ||
        request.prompt.phase !== "waiting"
      )
        throw Error("browser_auth_request_stale");
      if (
        !response ||
        !["submit", "cancel"].includes(response.action) ||
        Object.keys(response).some(
          (key) => !["action", "values", "selectedOption"].includes(key),
        )
      )
        throw Error("browser_auth_response_invalid");
      if (response.action === "submit") {
        const { fields, options } = request.prompt;
        if (options) {
          if (
            response.values !== undefined ||
            !options.some((option) => option.id === response.selectedOption)
          )
            throw Error("browser_auth_response_invalid");
        } else {
          const values = response.values;
          if (
            response.selectedOption !== undefined ||
            !values ||
            typeof values !== "object" ||
            Array.isArray(values) ||
            Object.keys(values).some(
              (id) => !fields.some((field) => field.id === id),
            ) ||
            fields.some(
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
        }
      }
      request.prompt.phase = "submitting";
      request.respond(response);
    },
    async request(
      entry: any,
      input: unknown,
      caller: AbortSignal,
    ): Promise<BrowserAuthResult> {
      const spec = validateAuthRequest(input, allowHttpLoopback);
      if (pending.has(entry)) throw Error("browser_auth_request_busy");
      if (!entry.starting || entry.revoked || executions.closing)
        throw Error("browser_page_unavailable");
      const requestId = randomUUID();
      let answer: (response: BrowserAuthResponse) => void;
      const response = new Promise<BrowserAuthResponse>((resolve) => {
        answer = resolve;
      });
      const record = { prompt: undefined as BrowserAuthPrompt, respond: answer };
      pending.set(entry, record);
      entry.authPending = true;
      const lifetime = new AbortController();
      const signal = AbortSignal.any([
        caller,
        entry.abort.signal,
        entry.lifetime.signal,
        lifetime.signal,
      ]);
      const starting = entry.starting;
      const original = entry.session.status(entry.peers.size);
      const ownedRetention = !entry.session.retained;
      let retainedUntil: number | undefined;
      const stillOwnsRetention = () =>
        ownedRetention &&
        retainedUntil !== undefined &&
        entry.session.status(entry.peers.size).expiresAt === retainedUntil;
      const expiresAt = Math.min(
        Date.now() + spec.timeoutMs,
        original.hardExpiresAt ?? Date.now(),
        original.expiresAt ?? Infinity,
      );
      const timer = setTimeout(
        () => lifetime.abort(),
        Math.max(1, expiresAt - Date.now()),
      );
      const identity = {
        actorId: requestId,
        leaseId: requestId,
        epoch: undefined as string,
      };
      let privateInput: Awaited<ReturnType<typeof prepareBrowserAuth>>;
      let acquired = false;
      let controlReleased = false;
      let resumed = false;
      let status: BrowserAuthStatus = "unavailable";
      let selectedOption: string;
      const control = (op: string, extra = {}, opSignal = signal) =>
        executions.dispatch(
          {
            op: "takeover",
            executionId: entry.executionId,
            action: { op, ...identity, ...extra },
          },
          opSignal,
        );
      let invalidated: (status: BrowserAuthStatus) => void;
      const invalidation = new Promise<BrowserAuthStatus>((resolve) => {
        invalidated = resolve;
      });
      const abort = () =>
        invalidated(
          caller.aborted || entry.lifetime.signal.aborted || entry.abort.signal.aborted
            ? "cancelled"
            : "expired",
        );
      signal.addEventListener("abort", abort, { once: true });
      try {
        signal.throwIfAborted();
        if (expiresAt <= Date.now()) throw Error("browser_auth_expired");
        if (entry.takeover && entry.takeover.state !== "automation")
          throw Error("browser_control_busy");
        if (ownedRetention) {
          const retained = await executions.dispatch(
            {
              op: "session",
              executionId: entry.executionId,
              action: "retain",
              durationMs: Math.min(1_800_000, expiresAt - Date.now() + 30_000),
            },
            signal,
          );
          retainedUntil = retained.expiresAt;
        }
        const lease = await automationControl.run(entry, signal, async () => {
          await automationControl.pause(entry, signal);
          return control("acquire", {
            automationStopped: true,
            targetId: spec.targetId,
          });
        });
        identity.epoch = lease.epoch;
        acquired = true;
        const browser = await starting;
        signal.throwIfAborted();
        privateInput = await prepareBrowserAuth(
          browser.endpoint,
          spec,
          signal,
          invalidated,
        );
        record.prompt = {
          requestId,
          origin: spec.origin,
          targetId: spec.targetId,
          fields: spec.fields.map(({ selector: _selector, ...field }) => field),
          options: spec.options?.map(({ selector: _selector, ...option }) => option),
          expiresAt,
          phase: "waiting",
        };
        const outcome = await Promise.race([response, invalidation]);
        record.prompt = undefined;
        if (typeof outcome === "string") status = outcome;
        else if (outcome.action === "cancel") status = "declined";
        else {
          selectedOption = outcome.selectedOption;
          status = await privateInput.submit(outcome);
        }
      } catch (error) {
        const code = (error as Error).message;
        status = signal.aborted
          ? caller.aborted ||
            entry.lifetime.signal.aborted ||
            entry.abort.signal.aborted
            ? "cancelled"
            : "expired"
          : code === "browser_auth_locator_invalid"
            ? "locator_invalid"
            : code === "browser_auth_origin_changed"
              ? "origin_changed"
              : code === "browser_auth_page_changed"
                ? "page_changed"
                : acquired
                  ? "submission_failed"
                  : "unavailable";
      } finally {
        record.prompt = undefined;
        clearTimeout(timer);
        signal.removeEventListener("abort", abort);
        await privateInput?.close();
        if (
          acquired &&
          !entry.revoked &&
          entry.starting === starting &&
          !executions.closing &&
          !entry.abort.signal.aborted
        ) {
          const cleanup = AbortSignal.timeout(10_000);
          try {
            await automationControl.run(entry, cleanup, async () => {
              await control("completeRequest", {}, cleanup);
              controlReleased = true;
              if (stillOwnsRetention() && entry.session.retained)
                await executions.dispatch(
                  {
                    op: "session",
                    executionId: entry.executionId,
                    action: "renew",
                    durationMs: 30_000,
                  },
                  cleanup,
                );
              if (!caller.aborted)
                resumed = await automationControl.resume(entry, cleanup);
            });
          } catch {
            /* Release and host resumption have independent outcomes. */
          }
        }
        if (
          !acquired &&
          stillOwnsRetention() &&
          entry.session.retained &&
          !entry.revoked &&
          (!entry.takeover || entry.takeover.state === "automation")
        ) {
          try {
            await executions.dispatch(
              {
                op: "session",
                executionId: entry.executionId,
                action: "renew",
                durationMs: 30_000,
              },
              AbortSignal.timeout(5000),
            );
          } catch {}
        }
        pending.delete(entry);
        entry.authPending = false;
      }
      return { requestId, status, selectedOption, controlReleased, resumed };
    },
  };
}
