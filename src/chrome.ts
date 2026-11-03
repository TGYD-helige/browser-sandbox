#!/usr/bin/env node
import { chownSync, mkdirSync, readFileSync } from "node:fs";
import { join } from "node:path";

// Browserless owns lifecycle; the wrapper supplies Cloak's flags and drops the execution UID.
const supplied = process.argv
  .slice(2)
  .filter(
    (arg) => !["--no-sandbox", "--disable-setuid-sandbox"].includes(arg.split("=")[0]),
  );
const executionId = supplied
  .find((arg) => arg.startsWith("--browser-execution-id="))
  ?.split("=")[1];
if (!/^[0-9a-f-]{36}$/.test(executionId ?? "") || process.getuid() !== 0)
  throw Error("browser_identity_invalid");
const root = "/browser-data/" + executionId;
const { uid, profileDir, debugPort } = JSON.parse(
  readFileSync(join(root, "owner.json"), "utf8"),
);
const directory = supplied
  .find((arg) => arg.startsWith("--user-data-dir="))
  ?.slice("--user-data-dir=".length);
if (
  !Number.isInteger(uid) ||
  (uid !== 20001 && (uid < 100000 || uid >= 2000000000)) ||
  !Number.isInteger(debugPort) ||
  debugPort < 42000 ||
  debugPort >= 43000 ||
  directory !== profileDir
)
  throw Error("browser_identity_invalid");
for (const name of [
  "home",
  "tmp",
  "files",
  ...(directory === join(root, "profile") ? ["profile"] : []),
]) {
  const path = join(root, name);
  mkdirSync(path, { recursive: true, mode: 0o700 });
  chownSync(path, uid, uid);
}
const args = [
  ...JSON.parse(readFileSync("/opt/google/chrome/args.json", "utf8")),
  ...supplied.filter(
    (arg) =>
      ![
        "--browser-execution-id=",
        "--remote-debugging-port=",
        "--remote-debugging-address=",
      ].some((prefix) => arg.startsWith(prefix)),
  ),
].filter(
  (arg) => !["--no-sandbox", "--disable-setuid-sandbox"].includes(arg.split("=")[0]),
);
// Keep this exact root-owned path: Ubuntu's Chrome AppArmor profile permits
// user namespaces here while rejecting unconfined non-root executables.
args.push(
  "--remote-debugging-address=127.0.0.1",
  `--remote-debugging-port=${debugPort}`,
);
const binary = "/opt/google/chrome/chrome";
const env = {
  PATH: process.env.PATH,
  HOME: join(root, "home"),
  TMPDIR: join(root, "tmp"),
  LANG: "C.UTF-8",
  TZ: process.env.TZ ?? "UTC",
};
process.setgroups([]);
process.setgid(uid);
process.setuid(uid);
mkdirSync(directory, { recursive: true, mode: 0o700 });
process.execve(binary, [binary, ...args], env);
