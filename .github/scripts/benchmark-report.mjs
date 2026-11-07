export function trackBenchmark(report, name, measurements, resources = {}) {
  if (report) report.current = { name, measurements, resources };
}

export async function collectBenchmark(report, name, run) {
  report.activeWorkload = name;
  report.pendingCases = [];
  const result = await run();
  report.cases.push(...(Array.isArray(result) ? result : [result]));
  delete report.pendingCases;
  delete report.current;
  delete report.sample;
}

export async function writeBenchmarkReport(report, run, write = console.log) {
  try {
    await run();
    report.status = "complete";
    delete report.activeWorkload;
    delete report.sample;
    delete report.current;
    delete report.pendingCases;
  } catch (error) {
    report.status = "failed";
    // Exception messages, stacks and assertion values can contain capability URLs.
    report.failure = {
      code: [
        "ERR_ASSERTION",
        "ETIMEDOUT",
        "ECONNRESET",
        "ECONNREFUSED",
        "ENOSPC",
      ].includes(error?.code)
        ? error.code
        : "benchmark_workload_failed",
    };
    throw error;
  } finally {
    write(JSON.stringify(report, null, 2));
  }
}
