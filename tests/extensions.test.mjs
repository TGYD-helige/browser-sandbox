import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs, { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { syncBuiltinESMExports } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { createExtensionReader, readExtensionDirs } from "../dist/extensions.js";

test("extension scanning stops when startup is cancelled", async () => {
  const abort = new AbortController();
  abort.abort();
  await assert.rejects(readExtensionDirs(undefined, abort.signal), {
    name: "AbortError",
  });
});

test("cancelled readers do not accumulate on a stalled scan with another active reader", () => {
  const child = spawnSync(
    process.execPath,
    [
      "--expose-gc",
      "--input-type=module",
      "-e",
      `
    import fs from 'node:fs/promises';
    import { syncBuiltinESMExports } from 'node:module';
    import { setImmediate } from 'node:timers/promises';
    import { createExtensionReader } from ${JSON.stringify(new URL("../dist/extensions.js", import.meta.url).href)};
    let scans = 0;
    fs.opendir = () => { scans++; return new Promise(() => {}); };
    syncBuiltinESMExports();
    const read = createExtensionReader('/extensions');
    const retained = read(new AbortController().signal);
    async function rotate(count) {
      for (let i = 0; i < count; i++) {
        const abort = new AbortController();
        const pending = read(abort.signal);
        abort.abort();
        try { await pending; } catch {}
      }
      for (let i = 0; i < 3; i++) { await setImmediate(); global.gc(); }
    }
    await rotate(500);
    const before = process.memoryUsage().heapUsed;
    await rotate(30000);
    console.log(JSON.stringify({ scans, growth: process.memoryUsage().heapUsed - before }));
  `,
    ],
    { encoding: "utf8", timeout: 15000 },
  );
  assert.equal(child.status, 0, child.stderr);
  const result = JSON.parse(child.stdout);
  assert.equal(result.scans, 1);
  assert(
    result.growth < 8 * 1024 * 1024,
    `cancelled readers retained ${result.growth} bytes`,
  );
});

test("overlapping scans share work without one cancelled reader cancelling another", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "browser-shared-extension-scan-"));
  const gate = Promise.withResolvers();
  const opendir = fs.opendir;
  let scans = 0;
  t.mock.method(fs, "opendir", async (...args) => {
    scans++;
    await gate.promise;
    return opendir(...args);
  });
  syncBuiltinESMExports();
  const read = createExtensionReader(root);
  const first = new AbortController();
  try {
    const cancelled = read(first.signal);
    const active = read(new AbortController().signal);
    first.abort();
    await assert.rejects(cancelled, { name: "AbortError" });
    assert.equal(scans, 1);
    gate.resolve();
    assert.deepEqual(await active, []);
    await mkdir(join(root, "new"));
    await writeFile(join(root, "new", "manifest.json"), "{}");
    assert.deepEqual(await read(new AbortController().signal), [join(root, "new")]);
    assert.equal(scans, 2);
  } finally {
    gate.resolve();
    t.mock.restoreAll();
    syncBuiltinESMExports();
    await rm(root, { recursive: true, force: true });
  }
});

test("cancellation during a scan stops before reading the next extension", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "browser-extension-cancel-"));
  const abort = new AbortController();
  const access = fs.access;
  let reads = 0;
  try {
    for (const name of ["first", "second"]) {
      await mkdir(join(root, name));
      await writeFile(join(root, name, "manifest.json"), "{}");
    }
    t.mock.method(fs, "access", async (...args) => {
      reads++;
      await access(...args);
      abort.abort();
    });
    syncBuiltinESMExports();
    await assert.rejects(readExtensionDirs(root, abort.signal), { name: "AbortError" });
    assert.equal(reads, 1);
  } finally {
    t.mock.restoreAll();
    syncBuiltinESMExports();
    await rm(root, { recursive: true, force: true });
  }
});

test("extension directory is optional and rescanned for each browser launch", async () => {
  assert.deepEqual(await readExtensionDirs(), []);
  const root = await mkdtemp(join(tmpdir(), "browser-extensions-"));
  try {
    assert.deepEqual(await readExtensionDirs(root), []);
    await writeFile(join(root, ".DS_Store"), "ignored");
    for (const name of ["second", "first extension"]) {
      await mkdir(join(root, name));
      await writeFile(join(root, name, "manifest.json"), "{}");
    }
    assert.deepEqual(await readExtensionDirs(root), [
      join(root, "first extension"),
      join(root, "second"),
    ]);
    await rm(join(root, "second"), { recursive: true });
    assert.deepEqual(await readExtensionDirs(root), [join(root, "first extension")]);
    await rm(join(root, "first extension", "manifest.json"));
    await assert.rejects(
      readExtensionDirs(root),
      /browser_extensions_directory_unreadable_or_invalid/,
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("extension scan rejects ambiguous paths, symlinks and excessive directories", async () => {
  for (const path of ["", "relative", "/tmp/a,b", "/tmp/a\n", "/tmp/a\0"])
    await assert.rejects(
      readExtensionDirs(path),
      /browser_extensions_directory_invalid/,
    );
  const root = await mkdtemp(join(tmpdir(), "browser-extensions-"));
  try {
    await symlink(tmpdir(), join(root, "linked"));
    await assert.rejects(
      readExtensionDirs(root),
      /browser_extensions_directory_unreadable_or_invalid/,
    );
    await rm(join(root, "linked"));
    for (let i = 0; i < 33; i++) {
      await mkdir(join(root, String(i)));
      await writeFile(join(root, String(i), "manifest.json"), "{}");
    }
    await assert.rejects(
      readExtensionDirs(root),
      /browser_extensions_directory_unreadable_or_invalid/,
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
