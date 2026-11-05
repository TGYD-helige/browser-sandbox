import assert from "node:assert/strict";
import fs, { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { syncBuiltinESMExports } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { readExtensionDirs } from "../dist/extensions.js";

test("extension scanning stops when startup is cancelled", async () => {
  const abort = new AbortController();
  abort.abort();
  await assert.rejects(readExtensionDirs(undefined, abort.signal), {
    name: "AbortError",
  });
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
