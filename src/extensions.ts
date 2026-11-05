import { constants } from "node:fs";
import { access, opendir } from "node:fs/promises";
import { isAbsolute, join } from "node:path";

const invalidPath = (value: string) =>
  [...value].some((character) => character === "," || character.charCodeAt(0) < 32);

export async function readExtensionDirs(
  root?: string,
  signal?: AbortSignal,
): Promise<string[]> {
  signal?.throwIfAborted();
  if (root === undefined) return [];
  if (!isAbsolute(root) || invalidPath(root))
    throw Error("browser_extensions_directory_invalid");
  const paths: string[] = [];
  try {
    const directory = await opendir(root);
    try {
      while (true) {
        signal?.throwIfAborted();
        const entry = await directory.read();
        signal?.throwIfAborted();
        if (!entry) break;
        if (entry.isSymbolicLink()) throw Error("extension_symlink");
        if (!entry.isDirectory()) continue;
        if (paths.length === 32 || invalidPath(entry.name))
          throw Error("extension_limit_or_name");
        const path = join(root, entry.name);
        await access(join(path, "manifest.json"), constants.R_OK);
        paths.push(path);
      }
    } finally {
      await directory.close();
    }
    signal?.throwIfAborted();
  } catch {
    signal?.throwIfAborted();
    throw Error("browser_extensions_directory_unreadable_or_invalid");
  }
  return paths.sort();
}

export function createExtensionReader(root: string) {
  let pending:
    | {
        abort: AbortController;
        result: Promise<string[]>;
        readers: number;
      }
    | undefined;
  return async (signal: AbortSignal): Promise<string[]> => {
    signal.throwIfAborted();
    if (pending?.abort.signal.aborted) throw Error("browser_extensions_scan_busy");
    if (!pending) {
      const abort = new AbortController();
      const result = readExtensionDirs(root, abort.signal);
      const scan = { abort, result, readers: 0 };
      pending = scan;
      result
        .finally(() => {
          if (pending === scan) pending = undefined;
        })
        .catch(() => {});
    }
    const scan = pending;
    scan.readers++;
    let cancel: () => void;
    try {
      // Only deployment files are read; cancelled executions need not retain their Profiles.
      const paths = await Promise.race([
        scan.result,
        new Promise<never>((_, reject) => {
          cancel = () => reject(signal.reason);
          signal.addEventListener("abort", cancel, { once: true });
          if (signal.aborted) cancel();
        }),
      ]);
      signal.throwIfAborted();
      return paths;
    } finally {
      signal.removeEventListener("abort", cancel);
      if (--scan.readers === 0) scan.abort.abort();
    }
  };
}
