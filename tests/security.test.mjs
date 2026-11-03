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
