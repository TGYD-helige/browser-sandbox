export type ViewerViewport = "follow" | { width: number; height: number };

export interface ViewerConnection {
  endpoint: string;
  token: string;
  viewport?: ViewerViewport;
}

export interface ViewerState {
  connected: boolean;
  control: "automation" | "stopping" | "human" | "uncertain" | "disconnected";
  leasedByYou?: boolean;
  leaseExpiresAt?: number;
  browser?: string;
  generation?: string;
}

export interface ViewerResult {
  type: "result" | "error";
  requestId?: number;
  result?: Record<string, unknown>;
  error?: string;
}

export class BrowserViewer extends HTMLElement {
  connect(connection: ViewerConnection): void;
  disconnect(): void;
  setViewport(viewport?: ViewerViewport): void;
}

declare global {
  interface HTMLElementTagNameMap {
    "browser-viewer": BrowserViewer;
  }
  interface HTMLElementEventMap {
    "browser-viewer-state": CustomEvent<ViewerState>;
    "browser-viewer-result": CustomEvent<ViewerResult>;
  }
}
