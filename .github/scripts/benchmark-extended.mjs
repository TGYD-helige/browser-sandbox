import { benchmarkCdp } from "./benchmark-cdp.mjs";
import { benchmarkFiles } from "./benchmark-files.mjs";
import { benchmarkQueue } from "./benchmark-queue.mjs";
import { collectBenchmark } from "./benchmark-report.mjs";
import { benchmarkSoak } from "./benchmark-soak.mjs";
import { benchmarkStorage } from "./benchmark-storage.mjs";
import { benchmarkViewer } from "./benchmark-viewer.mjs";

export async function benchmarkExtended(options, report) {
  for (const benchmark of [
    benchmarkCdp,
    benchmarkStorage,
    benchmarkQueue,
    benchmarkFiles,
    benchmarkViewer,
    benchmarkSoak,
  ])
    await collectBenchmark(report, benchmark.name, () =>
      benchmark({ ...options, report }),
    );
}
