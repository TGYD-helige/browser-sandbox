import { benchmarkCdp } from "./benchmark-cdp.mjs";
import { benchmarkFiles } from "./benchmark-files.mjs";
import { benchmarkQueue } from "./benchmark-queue.mjs";
import { benchmarkSoak } from "./benchmark-soak.mjs";
import { benchmarkStorage } from "./benchmark-storage.mjs";
import { benchmarkViewer } from "./benchmark-viewer.mjs";

export async function benchmarkExtended(options) {
  const cases = [];
  cases.push(...(await benchmarkCdp(options)));
  cases.push(...(await benchmarkStorage(options)));
  cases.push(await benchmarkQueue(options));
  cases.push(...(await benchmarkFiles(options)));
  cases.push(...(await benchmarkViewer(options)));
  cases.push(await benchmarkSoak(options));
  return cases;
}
