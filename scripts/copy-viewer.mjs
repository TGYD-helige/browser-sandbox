import { cp } from "node:fs/promises";

await cp(
  new URL("../src/viewer", import.meta.url),
  new URL("../dist/viewer", import.meta.url),
  { recursive: true },
);
