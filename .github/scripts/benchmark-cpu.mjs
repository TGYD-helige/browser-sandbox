import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

export function selectBenchmarkCpus(value) {
  const selected = new Set();
  for (const range of value.trim().split(",")) {
    const match = /^(\d+)(?:-(\d+))?$/.exec(range);
    assert(match, "Invalid effective CPU set");
    const start = Number(match[1]),
      end = Number(match[2] ?? match[1]);
    assert(Number.isSafeInteger(start) && Number.isSafeInteger(end) && end >= start);
    for (let cpu = start; cpu <= end && selected.size < 2; cpu++) selected.add(cpu);
    if (selected.size === 2) break;
  }
  assert.equal(selected.size, 2, "Performance measurement requires two available CPUs");
  return [...selected].join(",");
}

export async function readCpuAccounting() {
  const values = Object.fromEntries(
    (await readFile("/sys/fs/cgroup/cpu.stat", "utf8"))
      .trim()
      .split("\n")
      .map((line) => {
        const [name, value] = line.trim().split(/\s+/);
        return [name, Number(value)];
      }),
  );
  for (const key of ["usage_usec", "nr_periods", "nr_throttled", "throttled_usec"])
    assert(
      Number.isFinite(values[key]) && values[key] >= 0,
      `Missing CPU accounting ${key}`,
    );
  return values;
}

if (process.argv[2] === "affinity") {
  const effective = (
    await readFile("/sys/fs/cgroup/cpuset.cpus.effective", "utf8")
  ).trim();
  const selected = selectBenchmarkCpus(effective);
  console.error(`Benchmark CPUs: available=${effective}; selected=${selected}`);
  console.log(selected);
}
