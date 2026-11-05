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
        waiters: Set<(error: unknown, paths?: string[]) => void>;
      }
    | undefined;
  return async (signal: AbortSignal): Promise<string[]> => {
    signal.throwIfAborted();
    if (pending?.abort.signal.aborted) throw Error("browser_extensions_scan_busy");
    if (!pending) {
      const scan = {
        abort: new AbortController(),
        waiters: new Set<(error: unknown, paths?: string[]) => void>(),
      };
      pending = scan;
      const publish = (error: unknown, paths?: string[]) => {
        if (pending === scan) pending = undefined;
        for (const notify of scan.waiters) notify(error, paths);
      };
      readExtensionDirs(root, scan.abort.signal).then(
        (paths) => publish(undefined, paths),
        (error) => publish(error),
      );
    }
    const scan = pending;
    return new Promise<string[]>((resolve, reject) => {
      const finish = (error: unknown, paths?: string[]) => {
        if (!scan.waiters.delete(finish)) return;
        signal.removeEventListener("abort", cancel);
        if (error !== undefined) reject(error);
        else if (signal.aborted) reject(signal.reason);
        else resolve(paths);
        if (scan.waiters.size === 0) scan.abort.abort();
      };
      const cancel = () => finish(signal.reason);
      scan.waiters.add(finish);
      signal.addEventListener("abort", cancel, { once: true });
      if (signal.aborted) cancel();
    });
  };
}
