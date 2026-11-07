import assert from "node:assert/strict";
import { globSync, readdirSync, realpathSync, statSync } from "node:fs";
import { createRequire } from "node:module";
import { basename, dirname, relative } from "node:path";

const require = createRequire(`${process.env.APP_DIR}/package.json`);
const { chromium } = require("playwright-core");
const browserRoot = process.env.PLAYWRIGHT_BROWSERS_PATH;
const executable = chromium.executablePath();
assert.equal(realpathSync(executable), "/opt/browser-sandbox/dist/chrome.js");
assert.deepEqual(readdirSync(dirname(executable)), [basename(executable)]);
assert.deepEqual(
  globSync("**/{chrome,headless_shell,chrome-headless-shell}", { cwd: browserRoot }),
  [relative(browserRoot, executable)],
  "Only the CloakBrowser launcher may remain in the upstream browser directory",
);
const chrome = statSync("/opt/google/chrome/chrome");
assert.equal(chrome.uid, 0);
assert.equal(chrome.mode & 0o022, 0);
assert.equal(statSync("/tmp").mode & 0o7777, 0o1777);
assert.equal(statSync("/home/blessuser").uid, 999);
assert.equal(process.version, process.env.NODE_VERSION);
assert.equal(require("./package.json").version, "2.57.0");
console.log("Image layout: only CloakBrowser, Browserless and ownership preserved.");
