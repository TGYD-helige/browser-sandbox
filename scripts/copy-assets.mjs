import { cp } from "node:fs/promises";

await cp(
  new URL("../src/viewer", import.meta.url),
  new URL("../dist/viewer", import.meta.url),
  { recursive: true },
);
await cp(
  new URL("../skills/browser-sandbox-auth/scripts/auth.mjs", import.meta.url),
  new URL("../dist/auth-client.mjs", import.meta.url),
);
