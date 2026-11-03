const response = await fetch("http://127.0.0.1:9222/health", {
  signal: AbortSignal.timeout(2000),
});
if (!response.ok) process.exit(1);
