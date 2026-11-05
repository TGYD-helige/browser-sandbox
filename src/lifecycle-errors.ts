// Only the launcher may construct this after observing its captured child's exit.
// The checkpoint still failed; this proves profile reuse, not successful persistence.
export class VerifiedBrowserCheckpointFailure extends Error {
  constructor(cause) {
    super(
      /^browser_(cookie_save_failed_[a-z_]+|cookie_partition_unsupported|profile_flush_unconfirmed[a-z_]*)$/.test(
        cause?.message ?? "",
      )
        ? cause.message
        : "browser_cookie_save_failed",
      { cause },
    );
    this.name = "VerifiedBrowserCheckpointFailure";
  }
}

export class BrowserCleanupUnconfirmed extends Error {
  constructor(cause) {
    super("browser_cleanup_unconfirmed", { cause });
    this.name = "BrowserCleanupUnconfirmed";
  }
}
export const EXTENSION_STARTUP_ERRORS = new Set([
  "browser_extensions_directory_invalid",
  "browser_extensions_directory_unreadable_or_invalid",
  "browser_extensions_scan_busy",
  "browser_extensions_launch_request_too_large",
]);
