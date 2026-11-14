import { execFileSync } from "node:child_process";
import { lookup } from "node:dns/promises";
import { readFileSync } from "node:fs";
import { isIP } from "node:net";
import { WORKER_UID } from "./managed-backend.js";

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

export interface StorageDestination {
  address: string;
  port: number;
}

/** Trusted deployment configuration, never populated from page or CDP input. */
export async function storageDestinations(
  environment = process.env,
  resolve = lookup,
): Promise<StorageDestination[]> {
  let destinations: StorageDestination[];
  try {
    destinations = JSON.parse(environment.BROWSER_STORAGE_EGRESS ?? "[]");
    if (
      !Array.isArray(destinations) ||
      destinations.length > 32 ||
      destinations.some(
        (item) =>
          !item ||
          !isIP(item.address) ||
          !Number.isInteger(item.port) ||
          item.port < 1 ||
          item.port > 65535,
      )
    )
      throw Error();
    if (environment.BROWSER_PROFILE_STORE_ORIGIN) {
      const url = new URL(environment.BROWSER_PROFILE_STORE_ORIGIN);
      if (
        !["http:", "https:"].includes(url.protocol) ||
        url.username ||
        url.password ||
        url.pathname !== "/" ||
        url.search ||
        url.hash
      )
        throw Error();
      let timer: ReturnType<typeof setTimeout>;
      const addresses = await Promise.race([
        resolve(url.hostname.replace(/^\[|\]$/g, ""), { all: true }),
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(Error("timeout")), 30000);
        }),
      ]).finally(() => clearTimeout(timer));
      for (const { address } of addresses) {
        if (!isIP(address) || destinations.length >= 64) throw Error();
        destinations.push({
          address,
          port: Number(url.port || (url.protocol === "https:" ? 443 : 80)),
        });
      }
    }
    return destinations;
  } catch {
    throw Error("browser_storage_egress_invalid");
  }
}

/** Managed workers serve one controller; untrusted pages cannot initiate private-network connections. */
export function installWorkerNetworkPolicy(
  run = execFileSync,
  resolvers = [
    ...readFileSync("/etc/resolv.conf", "utf8").matchAll(/^nameserver\s+(\S+)/gm),
  ].map((match) => match[1]),
  storage: StorageDestination[] = [],
) {
  for (const [binary, destination] of [
    ["iptables", "127.0.0.0/8"],
    ["ip6tables", "::1/128"],
  ]) {
    for (const uid of [20001, WORKER_UID]) {
      run(
        binary,
        [
          "-w",
          "-I",
          "OUTPUT",
          "-d",
          destination,
          "-p",
          "tcp",
          "--dport",
          "8081",
          "-m",
          "owner",
          "--uid-owner",
          String(uid),
          "-j",
          "REJECT",
        ],
        { stdio: "ignore" },
      );
    }
  }
  for (const [binary, destinations] of [
    [
      "iptables",
      [
        "10.0.0.0/8",
        "172.16.0.0/12",
        "192.168.0.0/16",
        "169.254.0.0/16",
        "100.64.0.0/10",
        "0.0.0.0/8",
      ],
    ],
    ["ip6tables", ["fc00::/7", "fe80::/10", "::ffff:0:0/96"]],
  ] as const) {
    for (const destination of destinations)
      run(
        binary,
        [
          "-w",
          "-I",
          "OUTPUT",
          "-d",
          destination,
          "-m",
          "conntrack",
          "--ctstate",
          "NEW",
          "-j",
          "REJECT",
        ],
        { stdio: "ignore" },
      );
  }
  // Allow only configured DNS resolvers; destination connections stay subject to the private-network deny.
  for (const resolver of new Set(resolvers)) {
    const family = isIP(resolver);
    if (!family) throw Error("browser_dns_configuration_invalid");
    for (const protocol of ["udp", "tcp"])
      run(
        family === 6 ? "ip6tables" : "iptables",
        [
          "-w",
          "-I",
          "OUTPUT",
          "-d",
          resolver,
          "-p",
          protocol,
          "--dport",
          "53",
          "-j",
          "ACCEPT",
        ],
        { stdio: "ignore" },
      );
  }
  // Archive access is limited to exact destinations and the current runtime identity.
  for (const { address, port } of storage)
    run(
      isIP(address) === 6 ? "ip6tables" : "iptables",
      [
        "-w",
        "-I",
        "OUTPUT",
        "-d",
        address,
        "-p",
        "tcp",
        "--dport",
        String(port),
        "-m",
        "owner",
        "--uid-owner",
        String(process.getuid()),
        "-j",
        "ACCEPT",
      ],
      { stdio: "ignore" },
    );
}
