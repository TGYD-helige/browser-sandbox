import { connectCdp } from "./cdp.js";

export const viewablePage = (target: any) =>
  target.type === "page" &&
  (/^https?:/.test(target.url) || target.url === "about:blank");

/** One capture per target; observation never starts or retains a browser. */
export class Observation {
  private streams = new Map<
    string,
    { listeners: Set<(frame: any) => void>; latest?: any; close(): void }
  >();
  constructor(
    private endpoint: string,
    private signal: AbortSignal,
  ) {}

  async targets(signal: AbortSignal) {
    const cdp = await connectCdp(this.endpoint, signal);
    try {
      const { targetInfos } = await cdp.call("Target.getTargets");
      return targetInfos
        .filter(viewablePage)
        .slice(0, 64)
        .map(({ targetId, title, url }) => {
          const displayUrl = new URL(url);
          displayUrl.username = "";
          displayUrl.password = "";
          return {
            targetId,
            title: String(title ?? "").slice(0, 512),
            url: displayUrl.href.slice(0, 2048),
          };
        });
    } finally {
      cdp.close();
    }
  }

  async watch(targetId: string, listener: (frame: any) => void, signal: AbortSignal) {
    const life = AbortSignal.any([this.signal, signal]);
    life.throwIfAborted();
    let stream = this.streams.get(targetId);
    if (!stream) {
      if (this.streams.size >= 4) throw Error("browser_view_capacity_exceeded");
      const listeners = new Set<(frame: any) => void>();
      const abort = new AbortController();
      const combined = AbortSignal.any([this.signal, abort.signal]);
      // Reserve before awaiting so simultaneous viewers share the same capture.
      stream = { listeners, close: () => abort.abort() };
      this.streams.set(targetId, stream);
      const owned = stream;
      try {
        const cdp = await connectCdp(this.endpoint, combined);
        cdp.closed.finally(() => {
          if (this.streams.get(targetId) === owned) this.streams.delete(targetId);
          for (const emit of listeners) emit({ error: "browser_view_lost" });
          listeners.clear();
        });
        const targets = await cdp.call("Target.getTargets");
        if (
          !targets.targetInfos.some(
            (target: any) => target.targetId === targetId && viewablePage(target),
          )
        )
          throw Error("browser_page_unavailable");
        const { sessionId } = await cdp.call("Target.attachToTarget", {
          targetId,
          flatten: true,
        });
        cdp.subscribe((event) => {
          if (event.sessionId !== sessionId) return;
          if (
            event.method === "Page.frameNavigated" &&
            !event.params.frame.parentId &&
            !viewablePage({ type: "page", url: event.params.frame.url })
          )
            return cdp.close();
          if (event.method !== "Page.screencastFrame") return;
          const { data, metadata, sessionId: frameId } = event.params;
          if (typeof data !== "string" || data.length > 8 * 1024 * 1024)
            return cdp.close();
          owned.latest = {
            data,
            targetId,
            viewportWidth: metadata.deviceWidth,
            viewportHeight: metadata.deviceHeight,
          };
          for (const emit of listeners) emit(owned.latest);
          cdp
            .call("Page.screencastFrameAck", { sessionId: frameId }, sessionId)
            .catch(() => cdp.close());
        });
        await cdp.call("Page.enable", {}, sessionId);
        await cdp.call(
          "Page.startScreencast",
          {
            format: "jpeg",
            quality: 70,
            maxWidth: 1920,
            maxHeight: 1080,
            everyNthFrame: 1,
          },
          sessionId,
        );
      } catch (error) {
        owned.close();
        if (this.streams.get(targetId) === owned) this.streams.delete(targetId);
        for (const emit of listeners) emit({ error: "browser_view_lost" });
        throw error;
      }
    }
    if (life.aborted && !stream.listeners.size) stream.close();
    life.throwIfAborted();
    stream.listeners.add(listener);
    if (stream.latest) listener(stream.latest);
    const current = stream;
    const stop = () => {
      current.listeners.delete(listener);
      if (!current.listeners.size) {
        current.close();
        if (this.streams.get(targetId) === current) this.streams.delete(targetId);
      }
    };
    life.addEventListener("abort", stop, { once: true });
    if (life.aborted) stop();
  }
}
