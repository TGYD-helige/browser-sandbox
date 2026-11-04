import { channel } from "node:diagnostics_channel";
import { mkdir, writeFile } from "node:fs/promises";

// Loaded only by image verification; production keeps no timing records.
await mkdir("/run/browser-sandbox/benchmark", { recursive: true, mode: 0o700 });
channel("browser-sandbox.preparation").subscribe(({ executionId, ...phases }) => {
  writeFile(
    `/run/browser-sandbox/benchmark/${executionId}.json`,
    JSON.stringify(phases),
    { mode: 0o600 },
  ).catch(() => {
    process.exitCode = 1;
  });
});
