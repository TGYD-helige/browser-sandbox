import { connectCdp } from "./cdp.js";
import { viewablePage } from "./observation.js";

/** Emulation is session-scoped: keep the authorized owner's connection until release. */
export class BrowserViewport {
  closed = false;
  private life = new AbortController();
  private connecting?: ReturnType<typeof connectCdp>;
  private sessions = new Map<string, string>();
  private stop = () => this.close();
  constructor(
    private endpoint: string,
    private signal: AbortSignal,
  ) {
    signal.addEventListener("abort", this.stop, { once: true });
    if (signal.aborted) this.close();
  }
  close() {
    if (this.closed) return;
    this.closed = true;
    this.signal.removeEventListener("abort", this.stop);
    this.life.abort();
    this.sessions.clear();
  }
  async resize(targetId: string, width: number, height: number, signal: AbortSignal) {
    if (
      typeof targetId !== "string" ||
      !Number.isInteger(width) ||
      !Number.isInteger(height) ||
      width < 1 ||
      height < 1 ||
      width > 3840 ||
      height > 2160
    )
      throw Error("browser_viewport_invalid");
    signal.throwIfAborted();
    if (this.closed) throw Error("browser_view_lost");
    signal.addEventListener("abort", this.stop, { once: true });
    try {
      this.connecting ??= connectCdp(this.endpoint, this.life.signal)
        .then((cdp) => {
          cdp.closed.then(this.stop);
          return cdp;
        })
        .catch((error) => {
          this.close();
          throw error;
        });
      const cdp = await this.connecting;
      const { targetInfos } = await cdp.call("Target.getTargets");
      if (
        !targetInfos.some(
          (target: any) => target.targetId === targetId && viewablePage(target),
        )
      )
        throw Error("browser_page_unavailable");
      for (const id of this.sessions.keys())
        if (!targetInfos.some((target: any) => target.targetId === id))
          this.sessions.delete(id);
      let sessionId = this.sessions.get(targetId);
      if (!sessionId) {
        if (this.sessions.size >= 64) throw Error("browser_view_capacity_exceeded");
        ({ sessionId } = await cdp.call("Target.attachToTarget", {
          targetId,
          flatten: true,
        }));
        this.sessions.set(targetId, sessionId);
      }
      await cdp.call(
        "Emulation.setDeviceMetricsOverride",
        { width, height, deviceScaleFactor: 1, mobile: false },
        sessionId,
      );
      signal.throwIfAborted();
      return { targetId, width, height };
    } finally {
      signal.removeEventListener("abort", this.stop);
    }
  }
}
