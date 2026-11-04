import { randomUUID } from "node:crypto";
import { connectCdp } from "./cdp.js";
import { readBrowserJson } from "./readback.js";

export type ControlState = "automation" | "stopping" | "human" | "uncertain";

/** The gateway owns the fence; host authentication owns who may request a lease. */
export class Takeover {
  state: ControlState = "automation";
  private epoch = randomUUID();
  private actorId = "";
  private leaseId = "";
  private expiresAt = 0;
  private origin?: string;
  private targetId?: string;
  private lastRelease?: {
    actorId: string;
    leaseId: string;
    epoch: string;
    result: any;
  };
  private frame?: { revision: string; generation: string; capturedAt: number };
  constructor(
    private endpoint: string,
    private stopAutomation: () => Promise<void>,
    private connect = connectCdp,
  ) {}

  async command(request: any, signal?: AbortSignal) {
    if (request.op === "readback") {
      if (this.state !== "automation") throw Error("browser_control_busy");
      this.state = "stopping";
      let stopped = false;
      try {
        await this.stopAutomation();
        const page = await this.stopTargets(signal);
        stopped = true;
        const url = new URL(request.url);
        if (
          url.protocol !== "https:" ||
          url.username ||
          url.password ||
          url.origin !== page.origin
        )
          throw Error("browser_readback_origin_mismatch");
        const cdp = await this.connect(this.endpoint, signal);
        try {
          const { sessionId } = await cdp.call("Target.attachToTarget", {
            targetId: page.targetId,
            flatten: true,
          });
          try {
            return {
              ...(await readBrowserJson(cdp, sessionId, url.href)),
              targetId: page.targetId,
              capturedAt: Date.now(),
            };
          } finally {
            await cdp.call("Target.detachFromTarget", { sessionId });
          }
        } finally {
          cdp.close();
        }
      } finally {
        this.state = stopped ? "automation" : "uncertain";
      }
    }
    if (request.op === "status") {
      if (this.state === "human" && Date.now() >= this.expiresAt)
        this.state = "uncertain";
      return {
        state: this.state,
        expiresAt: this.expiresAt || undefined,
        leasedByYou: request.actorId === this.actorId,
        ...(request.actorId === this.actorId
          ? { epoch: this.epoch, leaseId: this.leaseId }
          : {}),
        ...(await this.page(signal)),
      };
    }
    if (request.op === "acquire") {
      if (
        typeof request.actorId !== "string" ||
        !request.actorId ||
        typeof request.leaseId !== "string" ||
        !request.leaseId ||
        request.automationStopped !== true
      )
        throw Error("browser_takeover_request_invalid");
      const same = request.actorId === this.actorId && request.leaseId === this.leaseId;
      if (this.state === "human" && same && Date.now() < this.expiresAt)
        return {
          state: this.state,
          epoch: this.epoch,
          expiresAt: this.expiresAt,
          ...(await this.page(signal)),
        };
      if (
        this.state !== "automation" &&
        !(
          this.state !== "stopping" &&
          (Date.now() >= this.expiresAt ||
            (request.replaceExisting === true && request.actorId === this.actorId))
        )
      )
        throw Error("browser_control_busy");
      this.actorId = request.actorId;
      this.leaseId = request.leaseId;
      this.epoch = randomUUID();
      this.state = "stopping";
      try {
        await this.stopAutomation();
        const page = await this.stopTargets(signal);
        signal?.throwIfAborted();
        this.origin = page.origin;
        this.targetId = page.targetId;
        this.expiresAt = Date.now() + 10 * 60_000;
        this.frame = undefined;
        this.state = "human";
        return {
          state: this.state,
          epoch: this.epoch,
          expiresAt: this.expiresAt,
          ...page,
        };
      } catch {
        this.state = "uncertain";
        throw Error("browser_stop_uncertain");
      }
    }
    if (
      request.op === "release" &&
      this.state === "automation" &&
      this.lastRelease &&
      request.actorId === this.lastRelease.actorId &&
      request.leaseId === this.lastRelease.leaseId &&
      request.epoch === this.lastRelease.epoch
    )
      return this.lastRelease.result;
    if (
      (this.state !== "human" &&
        !(request.op === "release" && this.state === "uncertain")) ||
      request.actorId !== this.actorId ||
      request.leaseId !== this.leaseId ||
      request.epoch !== this.epoch
    )
      throw Error("browser_control_stale");
    if (request.op === "release") {
      this.state = "stopping";
      try {
        const page = await this.page(signal);
        if (
          page.origin !== this.origin ||
          request.origin !== page.origin ||
          page.targetId !== this.targetId
        )
          throw Error("browser_origin_mismatch");
        const epoch = this.epoch;
        this.epoch = randomUUID();
        this.state = "automation";
        this.frame = undefined;
        const result = { state: this.state, epoch: this.epoch, ...page };
        this.lastRelease = {
          actorId: this.actorId,
          leaseId: this.leaseId,
          epoch,
          result,
        };
        return result;
      } catch (error) {
        this.state = "uncertain";
        throw error;
      }
    }
    if (Date.now() >= this.expiresAt) {
      this.state = "uncertain";
      throw Error("browser_control_expired");
    }
    if (request.op === "renew") {
      const page = await this.page(signal);
      if (page.origin !== this.origin || page.targetId !== this.targetId)
        throw Error("browser_origin_mismatch");
      this.expiresAt = Date.now() + 10 * 60_000;
      return {
        state: this.state,
        epoch: this.epoch,
        expiresAt: this.expiresAt,
        ...page,
      };
    }
    const cdp = await this.connect(this.endpoint, signal);
    try {
      const { targetInfos } = await cdp.call("Target.getTargets");
      const target = targetInfos.find(
        (item: any) =>
          item.type === "page" && item.targetId === (request.targetId ?? this.targetId),
      );
      if (!target || !/^https?:/.test(target.url))
        throw Error("browser_page_unavailable");
      const origin = new URL(target.url).origin;
      if (
        request.op !== "frame" &&
        (request.targetId !== target.targetId || request.origin !== origin)
      )
        throw Error("browser_target_changed");
      const { sessionId } = await cdp.call("Target.attachToTarget", {
        targetId: target.targetId,
        flatten: true,
      });
      const generation = async () =>
        JSON.stringify(
          (await cdp.call("Page.getFrameTree", {}, sessionId)).frameTree.frame,
        );
      const before = await generation();
      if (request.op === "frame") {
        const { data } = await cdp.call(
          "Page.captureScreenshot",
          { format: "jpeg", quality: 70 },
          sessionId,
        );
        if (before !== (await generation())) throw Error("browser_frame_stale");
        this.frame = {
          revision: randomUUID(),
          generation: before,
          capturedAt: Date.now(),
        };
        const { cssVisualViewport } = await cdp.call(
          "Page.getLayoutMetrics",
          {},
          sessionId,
        );
        return {
          data,
          frameRevision: this.frame.revision,
          targetId: target.targetId,
          origin,
          viewportWidth: cssVisualViewport.clientWidth,
          viewportHeight: cssVisualViewport.clientHeight,
        };
      }
      if (
        !this.frame ||
        request.frameRevision !== this.frame.revision ||
        this.frame.generation !== before ||
        Date.now() - this.frame.capturedAt > 30_000
      )
        throw Error("browser_frame_stale");
      this.frame = undefined;
      if (
        request.op === "pointer" &&
        Number.isFinite(request.x) &&
        Number.isFinite(request.y)
      ) {
        const { cssVisualViewport: viewport } = await cdp.call(
          "Page.getLayoutMetrics",
          {},
          sessionId,
        );
        if (
          request.x < 0 ||
          request.y < 0 ||
          request.x >= viewport.clientWidth ||
          request.y >= viewport.clientHeight
        )
          throw Error("browser_pointer_invalid");
        for (const type of ["mousePressed", "mouseReleased"])
          await cdp.call(
            "Input.dispatchMouseEvent",
            { type, x: request.x, y: request.y, button: "left", clickCount: 1 },
            sessionId,
          );
      } else if (
        request.op === "text" &&
        typeof request.text === "string" &&
        request.text.length <= 2000
      ) {
        await cdp.call("Input.insertText", { text: request.text }, sessionId);
      } else if (
        request.op === "key" &&
        ["Enter", "Tab", "Backspace", "Escape", "PageUp", "PageDown", "Back"].includes(
          request.key,
        )
      ) {
        if (request.key === "Back") {
          const history = await cdp.call("Page.getNavigationHistory", {}, sessionId);
          if (history.currentIndex > 0)
            await cdp.call(
              "Page.navigateToHistoryEntry",
              { entryId: history.entries[history.currentIndex - 1].id },
              sessionId,
            );
          return {};
        }
        for (const type of ["keyDown", "keyUp"])
          await cdp.call(
            "Input.dispatchKeyEvent",
            {
              type,
              key: request.key,
              windowsVirtualKeyCode: {
                Enter: 13,
                Tab: 9,
                Backspace: 8,
                Escape: 27,
                PageUp: 33,
                PageDown: 34,
              }[request.key],
            },
            sessionId,
          );
      } else throw Error("browser_human_operation_invalid");
      return {};
    } finally {
      cdp.close();
    }
  }
  private async page(signal?: AbortSignal) {
    const cdp = await this.connect(this.endpoint, signal);
    try {
      const { targetInfos } = await cdp.call("Target.getTargets");
      const target = targetInfos.find(
        (item: any) =>
          item.type === "page" &&
          (!this.targetId || item.targetId === this.targetId) &&
          /^https?:/.test(item.url),
      );
      if (!target || !/^https?:/.test(target.url))
        throw Error("browser_page_unavailable");
      return { targetId: target.targetId, origin: new URL(target.url).origin };
    } finally {
      cdp.close();
    }
  }
  private async stopTargets(signal?: AbortSignal) {
    const cdp = await this.connect(this.endpoint, signal);
    try {
      const { targetInfos } = await cdp.call("Target.getTargets");
      for (const target of targetInfos.filter((item: any) => item.type === "page")) {
        const { sessionId } = await cdp.call("Target.attachToTarget", {
          targetId: target.targetId,
          flatten: true,
        });
        try {
          await cdp.call("Runtime.terminateExecution", {}, sessionId);
          await cdp.call("Page.stopLoading", {}, sessionId);
        } finally {
          await cdp.call("Target.detachFromTarget", { sessionId });
        }
      }
      return await this.page(signal);
    } finally {
      cdp.close();
    }
  }
}
