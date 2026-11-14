import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { installBrowserNetworkPolicy } from "../dist/network-policy.js";
import { browserSeccomp } from "../dist/prepare-seccomp.js";

test("seccomp resolves capability rules for both architectures without allowing mount", () => {
  const source = JSON.parse(
    readFileSync(new URL("../config/seccomp-source.json", import.meta.url)),
  );
  for (const architecture of ["x64", "arm64"]) {
    const profile = browserSeccomp(source, architecture);
    assert.equal(profile.defaultAction, "SCMP_ACT_ERRNO");
    assert(profile.syscalls.some((rule) => rule.names.includes("chroot")));
    assert(!profile.syscalls.some((rule) => rule.names.includes("mount")));
    assert(!profile.syscalls.some((rule) => rule.includes || rule.excludes));
  }
});

test("network isolation rejects unprivileged installation", {
  skip: process.getuid?.() === 0,
}, () => {
  assert.throws(() => installBrowserNetworkPolicy(), /host_required/);
});

test("domain resolver policy accepts bounded host patterns and rejects launch-argument injection", async () => {
  const { normalizeAllowedDomains } = await import("../dist/network-policy.js");
  assert.deepEqual(
    normalizeAllowedDomains(["EXAMPLE.COM", "*.example.com", "example.com"]),
    ["*.example.com", "example.com"],
  );
  assert.deepEqual(normalizeAllowedDomains([]), []);
  for (const value of [
    ["example.com, EXCLUDE *"],
    ["https://example.com"],
    ["-bad.example"],
    ["example.com:443"],
    Array(65).fill("example.com"),
  ])
    assert.throws(() => normalizeAllowedDomains(value), /domains_invalid/);
});

test("managed archive egress pins its destination, port and runtime identity", async () => {
  const { installWorkerNetworkPolicy } = await import("../dist/network-policy.js");
  const { WORKER_UID } = await import("../dist/managed-backend.js");
  const calls = [];
  installWorkerNetworkPolicy(
    (binary, args) => calls.push([binary, args]),
    ["10.43.0.10"],
    [{ address: "10.42.1.2", port: 9083 }],
  );
  const allowed = calls.filter(
    ([, args]) => args.includes("ACCEPT") && args.includes("9083"),
  );
  assert.equal(allowed.length, 1);
  const dns = calls.filter(
    ([, args]) => args.includes("ACCEPT") && args.includes("53"),
  );
  assert.equal(dns.length, 2);
  for (const [, args] of dns) assert.equal(args[args.indexOf("-d") + 1], "10.43.0.10");
  for (const [, args] of allowed) {
    assert.equal(args[args.indexOf("-d") + 1], "10.42.1.2");
    assert.equal(args[args.indexOf("--uid-owner") + 1], String(process.getuid()));
    assert.equal(args[args.indexOf("--dport") + 1], "9083");
  }
  assert(
    calls.some(
      ([, args]) =>
        args.includes("20001") && args.includes("8081") && args.includes("REJECT"),
    ),
  );
  assert(
    calls.some(
      ([, args]) =>
        args.includes(String(WORKER_UID)) &&
        args.includes("8081") &&
        args.includes("REJECT"),
    ),
  );
});

test("storage egress resolves the configured origin and validates explicit endpoints", async () => {
  const { storageDestinations } = await import("../dist/network-policy.js");
  assert.deepEqual(await storageDestinations({}), []);
  assert.deepEqual(
    await storageDestinations(
      {
        BROWSER_STORAGE_EGRESS: JSON.stringify([{ address: "fd00::1", port: 9083 }]),
        BROWSER_PROFILE_STORE_ORIGIN: "https://storage.example:9443/",
      },
      async (hostname, options) => {
        assert.equal(hostname, "storage.example");
        assert.deepEqual(options, { all: true });
        return [{ address: "10.42.1.2" }];
      },
    ),
    [
      { address: "fd00::1", port: 9083 },
      { address: "10.42.1.2", port: 9443 },
    ],
  );
  for (const value of [
    "null",
    "{}",
    '[{"address":"10.0.0.0/8","port":8082}]',
    '[{"address":"127.0.0.1","port":0}]',
  ])
    await assert.rejects(
      storageDestinations({ BROWSER_STORAGE_EGRESS: value }),
      /egress_invalid/,
    );
  await assert.rejects(
    storageDestinations({ BROWSER_PROFILE_STORE_ORIGIN: "http://store:8082/path" }),
    /egress_invalid/,
  );
  await assert.rejects(
    storageDestinations(
      { BROWSER_PROFILE_STORE_ORIGIN: "http://store:8082" },
      async () => {
        throw Error("unavailable");
      },
    ),
    /egress_invalid/,
  );
});
