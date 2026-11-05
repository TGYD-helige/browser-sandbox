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
    for await (const entry of directory) {
      signal?.throwIfAborted();
      if (entry.isSymbolicLink()) throw Error("extension_symlink");
      if (!entry.isDirectory()) continue;
      if (paths.length === 32 || invalidPath(entry.name))
        throw Error("extension_limit_or_name");
      const path = join(root, entry.name);
      await access(join(path, "manifest.json"), constants.R_OK);
      paths.push(path);
    }
    signal?.throwIfAborted();
  } catch {
    signal?.throwIfAborted();
    throw Error("browser_extensions_directory_unreadable_or_invalid");
  }
  return paths.sort();
}
