import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { browserSeccomp } from "../dist/prepare-seccomp.js";

const profile = browserSeccomp(
  JSON.parse(readFileSync(process.argv[2], "utf8")),
  process.arch,
);
const unsigned = (value, max = Number.MAX_SAFE_INTEGER) => {
  assert(Number.isSafeInteger(value) && value >= 0 && value <= max);
  return value;
};
const action = (kind, errno) => {
  if (kind === "SCMP_ACT_ALLOW") return kind;
  assert.equal(kind, "SCMP_ACT_ERRNO");
  return `SCMP_ACT_ERRNO(${unsigned(errno ?? 1, 0xffff)})`;
};

console.log(
  `#define DEFAULT_ACTION ${action(profile.defaultAction, profile.defaultErrnoRet)}`,
);
console.log("static int configure_filter(scmp_filter_ctx filter) {");
for (const arch of profile.architectures.slice(1)) {
  assert.match(arch, /^SCMP_ARCH_[A-Z0-9_]+$/);
  console.log(`  if (seccomp_arch_add(filter, ${arch}) < 0) return -1;`);
}
for (const rule of profile.syscalls) {
  const comparisons = (rule.args ?? []).map(({ index, op, value, valueTwo }) => {
    assert.match(op, /^SCMP_CMP_(NE|LT|LE|EQ|GE|GT|MASKED_EQ)$/);
    return `SCMP_CMP(${unsigned(index, 5)}, ${op}, ${unsigned(value)}ULL, ${unsigned(valueTwo ?? 0)}ULL)`;
  });
  const args = comparisons.length
    ? `(struct scmp_arg_cmp[]){${comparisons.join(", ")}}`
    : "NULL";
  for (const name of rule.names) {
    assert.match(name, /^[a-zA-Z0-9_]+$/);
    console.log(
      `  if (add_rule(filter, "${name}", ${action(rule.action, rule.errnoRet)}, ${comparisons.length}, ${args}) < 0) return -1;`,
    );
  }
}
console.log("  return 0;\n}");
