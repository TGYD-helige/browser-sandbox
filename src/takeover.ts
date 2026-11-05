import { createHash, randomUUID } from "node:crypto";
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

  get leaseExpiresAt(): number | undefined {
    return this.state === "human" || this.state === "uncertain"
      ? this.expiresAt || undefined
      : undefined;
  }

  invalidateFrame() {
    this.frame = undefined;
  }

  heldBy(actorId: string, leaseId: string, epoch?: string) {
    if (this.state === "human" && Date.now() >= this.expiresAt)
      this.state = "uncertain";
    return (
      this.state === "human" &&
      this.actorId === actorId &&
      this.leaseId === leaseId &&
      this.epoch === epoch
    );
  }

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
        this.targetId = request.targetId ?? this.targetId;
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
        !(
          ["release", "completeRequest"].includes(request.op) &&
          this.state === "uncertain"
        )) ||
      request.actorId !== this.actorId ||
      request.leaseId !== this.leaseId ||
      request.epoch !== this.epoch
    )
      throw Error("browser_control_stale");
    if (request.op === "completeRequest") {
      // The trusted request owner has closed its private input channel before releasing.
      this.epoch = randomUUID();
      this.state = "automation";
      this.frame = undefined;
      return { state: this.state };
    }
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
    if (request.op === "confirmTarget") {
      const cdp = await this.connect(this.endpoint, signal);
      try {
        const { targetInfos } = await cdp.call("Target.getTargets");
        const target = targetInfos.find(
          (item: any) => item.type === "page" && item.targetId === request.targetId,
        );
        if (
          !target ||
          !/^https?:/.test(target.url) ||
          new URL(target.url).origin !== request.origin
        )
          throw Error("browser_target_changed");
        const { sessionId } = await cdp.call("Target.attachToTarget", {
          targetId: target.targetId,
          flatten: true,
        });
        try {
          const generation = JSON.stringify(
            (await cdp.call("Page.getFrameTree", {}, sessionId)).frameTree.frame,
          );
          if (
            !this.frame ||
            request.frameRevision !== this.frame.revision ||
            this.frame.generation !== generation ||
            Date.now() - this.frame.capturedAt > 30_000
          )
            throw Error("browser_frame_stale");
          this.targetId = target.targetId;
          this.origin = request.origin;
          this.frame = undefined;
          return { state: this.state, targetId: this.targetId, origin: this.origin };
        } finally {
          await cdp.call("Target.detachFromTarget", { sessionId });
        }
      } finally {
        cdp.close();
      }
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
          documentId: createHash("sha256").update(before).digest("hex"),
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
        ["pointer", "scroll"].includes(request.op) &&
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
        if (request.op === "scroll") {
          if (
            !Number.isFinite(request.deltaX) ||
            !Number.isFinite(request.deltaY) ||
            Math.abs(request.deltaX) > 2000 ||
            Math.abs(request.deltaY) > 2000
          )
            throw Error("browser_pointer_invalid");
          await cdp.call(
            "Input.dispatchMouseEvent",
            {
              type: "mouseWheel",
              x: request.x,
              y: request.y,
              deltaX: request.deltaX,
              deltaY: request.deltaY,
            },
            sessionId,
          );
        } else
          for (const type of ["mousePressed", "mouseReleased"])
            await cdp.call(
              "Input.dispatchMouseEvent",
              { type, x: request.x, y: request.y, button: "left", clickCount: 1 },
              sessionId,
            );
      } else if (request.op === "navigate") {
        if (typeof request.url !== "string" || request.url.length > 2048)
          throw Error("browser_navigation_invalid");
        const url = new URL(request.url);
        if (!["http:", "https:"].includes(url.protocol) || url.username || url.password)
          throw Error("browser_navigation_invalid");
        await cdp.call("Page.navigate", { url: url.href }, sessionId);
      } else if (request.op === "reload") {
        await cdp.call("Page.reload", {}, sessionId);
      } else if (
        request.op === "text" &&
        typeof request.text === "string" &&
        request.text.length <= 2000
      ) {
        await cdp.call("Input.insertText", { text: request.text }, sessionId);
      } else if (
        request.op === "key" &&
        [
          "Enter",
          "Tab",
          "Backspace",
          "Escape",
          "PageUp",
          "PageDown",
          "Back",
          "Forward",
          "Delete",
          "ArrowLeft",
          "ArrowRight",
          "ArrowUp",
          "ArrowDown",
          "Home",
          "End",
          "SelectAll",
        ].includes(request.key)
      ) {
        if (request.modifiers !== undefined && ![0, 8].includes(request.modifiers))
          throw Error("browser_human_operation_invalid");
        if (["Back", "Forward"].includes(request.key)) {
          const history = await cdp.call("Page.getNavigationHistory", {}, sessionId);
          const index = history.currentIndex + (request.key === "Back" ? -1 : 1);
          if (index >= 0 && index < history.entries.length)
            await cdp.call(
              "Page.navigateToHistoryEntry",
              { entryId: history.entries[index].id },
              sessionId,
            );
          return {};
        }
        for (const type of ["keyDown", "keyUp"])
          await cdp.call(
            "Input.dispatchKeyEvent",
            {
              type,
              key: request.key === "SelectAll" ? "a" : request.key,
              modifiers: request.key === "SelectAll" ? 2 : (request.modifiers ?? 0),
              windowsVirtualKeyCode: {
                Enter: 13,
                Tab: 9,
                Backspace: 8,
                Escape: 27,
                PageUp: 33,
                PageDown: 34,
                Delete: 46,
                ArrowLeft: 37,
                ArrowRight: 39,
                ArrowUp: 38,
                ArrowDown: 40,
                Home: 36,
                End: 35,
                SelectAll: 65,
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
