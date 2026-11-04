import { readFileSync, writeFileSync } from "node:fs";
import { pathToFileURL } from "node:url";

const capabilities = new Set([
  "CAP_SETUID",
  "CAP_SETGID",
  "CAP_CHOWN",
  "CAP_KILL",
  "CAP_DAC_OVERRIDE",
  "CAP_SYS_CHROOT",
]);
export function browserSeccomp(source, architecture) {
  const native = {
    x64: ["amd64", "SCMP_ARCH_X86_64"],
    arm64: ["arm64", "SCMP_ARCH_AARCH64"],
  }[architecture];
  if (!native) throw Error("unsupported_browser_architecture");
  const [arch, seccompArch] = native;
  const mapping = source.archMap.find((row) => row.architecture === seccompArch);
  if (!mapping || source.defaultAction !== "SCMP_ACT_ERRNO")
    throw Error("browser_seccomp_source_invalid");
  // OCI/CRI does not evaluate Moby's includes/excludes. Resolve them for the fixed provider capabilities.
  const syscalls = source.syscalls
    .filter(
      (rule) =>
        (!rule.includes?.arches || rule.includes.arches.includes(arch)) &&
        (!rule.includes?.caps ||
          rule.includes.caps.some((cap) => capabilities.has(cap))) &&
        !rule.excludes?.arches?.includes(arch) &&
        !rule.excludes?.caps?.some((cap) => capabilities.has(cap)),
    )
    .map(({ includes, excludes, comment, ...rule }) => rule);
  return {
    defaultAction: source.defaultAction,
    defaultErrnoRet: source.defaultErrnoRet,
    architectures: [seccompArch, ...mapping.subArchitectures],
    syscalls,
  };
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  writeFileSync(
    process.argv[3],
    JSON.stringify(
      browserSeccomp(JSON.parse(readFileSync(process.argv[2], "utf8")), process.arch),
      null,
      2,
    ) + "\n",
  );
}
