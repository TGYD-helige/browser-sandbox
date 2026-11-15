let healthy = false;
for (const port of [9222, Number(process.env.BROWSER_CONTROLLER_PORT ?? 8080)]) {
  try {
    const response = await fetch(`http://127.0.0.1:${port}/health`, {
      signal: AbortSignal.timeout(1000),
    });
    if (response.ok) {
      healthy = true;
      break;
    }
  } catch {}
}
if (!healthy) process.exit(1);
