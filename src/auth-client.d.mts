import type { BrowserAuthRequest, BrowserAuthResult } from "./browser-auth.js";

export function requestAuth(
  endpoint: string,
  request: BrowserAuthRequest,
  signal?: AbortSignal,
): Promise<BrowserAuthResult>;
