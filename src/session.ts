import { randomUUID } from "node:crypto";

export type SessionAction = "status" | "retain" | "renew" | "release" | "stop";
export type SessionState =
  | "unused"
  | "starting"
  | "live"
  | "retained"
  | "expired"
  | "stopped"
  | "lost";
export interface SessionStatus {
  state: SessionState;
  generation?: string;
  expiresAt?: number;
  hardExpiresAt?: number;
  connectedClients: number;
}

/** Live page retention is bounded by the current browser generation's hard deadline. */
export class BrowserSession {
  private state: SessionState = "unused";
  private generation?: string;
  private alive = false;
  private hardExpiresAt?: number;
  private expiresAt?: number;
  private timer?: ReturnType<typeof setTimeout>;
  constructor(
    private expire: () => void,
    private maxRetentionMs = 1_800_000,
  ) {}

  starting() {
    this.clear();
    this.state = "starting";
    this.alive = false;
    this.generation = randomUUID();
    this.hardExpiresAt = undefined;
    this.expiresAt = undefined;
  }
  ready(hardExpiresAt: number) {
    this.state = "live";
    this.alive = true;
    this.hardExpiresAt = hardExpiresAt;
  }
  ended(state: "stopped" | "lost" = "lost") {
    this.clear();
    this.alive = false;
    this.state =
      state === "lost" &&
      (this.state === "expired" || (this.state === "retained" && !this.retained))
        ? "expired"
        : state;
  }
  get retained() {
    return this.state === "retained" && Date.now() < (this.expiresAt ?? 0);
  }
  status(connectedClients: number): SessionStatus {
    if (this.state === "retained" && !this.retained) this.state = "expired";
    return {
      state: this.state,
      generation: this.generation,
      expiresAt: this.expiresAt,
      hardExpiresAt: this.hardExpiresAt,
      connectedClients,
    };
  }
  control(action: Exclude<SessionAction, "stop">, durationMs?: number) {
    if (action === "status") return;
    if (action === "release") {
      this.clear();
      if (this.alive && (this.state === "retained" || this.state === "expired"))
        this.state = "live";
      this.expiresAt = undefined;
      return;
    }
    if (action !== "retain" && action !== "renew")
      throw Error("browser_session_action_invalid");
    if (this.state !== "live" && !this.retained)
      throw Error("browser_session_not_live");
    if (action === "renew" && !this.retained)
      throw Error("browser_session_not_retained");
    const duration = durationMs ?? this.maxRetentionMs;
    if (!Number.isInteger(duration) || duration < 1 || duration > this.maxRetentionMs)
      throw Error("browser_session_duration_invalid");
    const expiresAt = Math.min(Date.now() + duration, this.hardExpiresAt ?? 0);
    if (expiresAt <= Date.now()) throw Error("browser_session_expired");
    // Repeated retain is idempotent; renewal must be explicit.
    if (action === "retain" && this.retained) return;
    this.clear();
    this.state = "retained";
    this.expiresAt = expiresAt;
    this.timer = setTimeout(
      () => {
        this.state = "expired";
        this.expire();
      },
      Math.max(1, expiresAt - Date.now()),
    );
    this.timer.unref();
  }
  private clear() {
    clearTimeout(this.timer);
    this.timer = undefined;
  }
}

/** Callable by the execution UID; the host control socket remains private. */
export async function sessionRequest(
  endpoint: string,
  action: SessionAction = "status",
  durationMs?: number,
  signal?: AbortSignal,
): Promise<SessionStatus> {
  const url = new URL(endpoint);
  if (
    !["ws:", "wss:"].includes(url.protocol) ||
    !/^\/execution\/[a-f0-9]{64}$/.test(url.pathname) ||
    url.search ||
    url.hash ||
    url.username ||
    url.password
  )
    throw Error("browser_session_endpoint_invalid");
  url.protocol = url.protocol === "wss:" ? "https:" : "http:";
  url.pathname += "/session";
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ action, durationMs }),
    redirect: "error",
    signal: AbortSignal.any([AbortSignal.timeout(30_000), ...(signal ? [signal] : [])]),
  });
  let input = "";
  for await (const chunk of response.body ?? []) {
    input += new TextDecoder().decode(chunk);
    if (Buffer.byteLength(input) > 8192)
      throw Error("browser_session_response_invalid");
  }
  let value: any;
  try {
    value = JSON.parse(input);
  } catch {
    throw Error("browser_session_failed");
  }
  if (!response.ok)
    throw Error(
      /^browser_[a-z_]+$/.test(value.error) ? value.error : "browser_session_failed",
    );
  return value;
}
