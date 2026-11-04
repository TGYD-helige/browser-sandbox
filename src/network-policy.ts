import { execFileSync } from "node:child_process";

/** Run in the browser's private network namespace before admitting executions. */
export function installBrowserNetworkPolicy(run = execFileSync) {
  if (process.getuid?.() !== 0) throw Error("browser_network_host_required");
  for (const binary of ["iptables", "ip6tables"]) {
    const destination = binary === "iptables" ? "127.0.0.0/8" : "::1/128";
    for (const uid of ["20001", "100000-1999999999"]) {
      for (const ports of ["3001", "42000:42999"]) {
        const rule = [
          "OUTPUT",
          "-d",
          destination,
          "-p",
          "tcp",
          "--dport",
          ports,
          "-m",
          "conntrack",
          "--ctstate",
          "NEW",
          "-m",
          "owner",
          "--uid-owner",
          uid,
          "-j",
          "REJECT",
        ];
        try {
          run(binary, ["-w", "-C", ...rule], { stdio: "ignore" });
        } catch {
          run(binary, ["-w", "-I", ...rule], { stdio: "ignore" });
        }
      }
    }
  }
}

/** Optional Chrome resolver restrictions; host egress policy remains the network security authority. */
export function normalizeAllowedDomains(value: unknown): string[] {
  if (value === undefined) return [];
  if (
    !Array.isArray(value) ||
    value.length > 64 ||
    value.some(
      (domain) =>
        typeof domain !== "string" ||
        domain.length > 253 ||
        !/^(\*\.)?([a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?\.)*[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?$/.test(
          domain,
        ),
    )
  )
    throw Error("browser_domains_invalid");
  return [...new Set(value.map((domain) => domain.toLowerCase()))].sort();
}
