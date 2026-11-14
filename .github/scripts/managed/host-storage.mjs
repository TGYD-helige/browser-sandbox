import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { archiveFixture } from "../benchmark-storage.mjs";

const root = await fs.mkdtemp(path.join(os.tmpdir(), "managed-host-storage-"));
const fixture = await archiveFixture(root, { host: "0.0.0.0", port: 9083 });
process.stdout.write("ready\n");
for (const signal of ["SIGINT", "SIGTERM"])
  process.once(signal, async () => {
    await fixture.close();
    await fs.rm(root, { recursive: true, force: true });
  });
