// Keep provider response bodies and execFile's captured output out of CI logs.
process.on("uncaughtException", (error) => {
  const name = /^[A-Za-z]{1,40}$/.test(error?.name) ? error.name : "Error";
  const location =
    error?.stack?.match(
      /\.github\/scripts\/e2e\/(?:pi|dsh|opencode|agent-browser|session-mcp|fixture)\.mjs:\d+:\d+/,
    )?.[0] ?? "unknown";
  console.error(`E2E_FAILURE ${name} at ${location}`);
  process.exit(1);
});
