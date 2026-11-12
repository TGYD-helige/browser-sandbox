import { spawn } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import { constants } from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";

export const validProfileId = (value) =>
  typeof value === "string" && /^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$/.test(value);
/** Every recursive walk holds a descriptor per level, so all walks share one depth bound. */
export const MAX_TREE_DEPTH = 128;
export const noFollowFlags =
  constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK;
const directoryFlags = noFollowFlags | constants.O_DIRECTORY;
/** Linux resolves children through an open descriptor, so a swapped ancestor is never followed. */
export const pinnedPath = (handle: { fd: number }, name: string) =>
  process.platform === "linux" ? "/proc/self/fd/" + handle.fd : name;
const unlinkEntry = (name: string) =>
  fs.unlink(name).then(
    () => true,
    (error) => {
      if (error.code === "ENOENT") return true;
      if (error.code !== "EISDIR" && error.code !== "EPERM") throw error;
      return false;
    },
  );
/** Remove a tree whose contents an execution UID may still rename or replace with links. */
export async function removeTree(
  name: string,
  signal?: AbortSignal,
  maxEntries = Infinity,
) {
  let count = 0,
    root: string;
  const spilled: string[] = [];
  const admit = (entries: number) => {
    signal?.throwIfAborted();
    if ((count += entries) > maxEntries)
      throw Error("browser_profile_capacity_exceeded");
  };
  const remove = async (target: string, depth = 0) => {
    admit(1);
    const directory = await fs.open(target, directoryFlags).catch(async (error) => {
      if (error.code === "ENOENT") return null;
      if (error.code !== "ENOTDIR" && error.code !== "ELOOP") throw error;
      await fs.unlink(target);
      return null;
    });
    if (!directory) return;
    try {
      const pinned = pinnedPath(directory, target),
        directories: string[] = [],
        files: string[] = [];
      if (depth === 0) root = pinned;
      for (const item of await fs.readdir(pinned, { withFileTypes: true }))
        (item.isDirectory() ? directories : files).push(path.join(pinned, item.name));
      // unlink never follows links; a listed file since swapped for a directory takes the pinned walk.
      for (let i = 0; i < files.length; i += 32) {
        const batch = files.slice(i, i + 32);
        admit(batch.length);
        const removed = await Promise.all(batch.map(unlinkEntry));
        directories.push(...batch.filter((_file, j) => !removed[j]));
      }
      for (const child of directories) {
        if (depth < 63) {
          await remove(child, depth + 1);
          continue;
        }
        // Hold at most 64 descriptors: move deeper subtrees under the pinned root and remove them from there.
        const spill = path.join(root, ".removing-" + randomBytes(8).toString("hex"));
        await fs.rename(child, spill).then(
          () => spilled.push(spill),
          (error) => {
            if (error.code !== "ENOENT") throw error;
          },
        );
      }
      while (depth === 0 && spilled.length) await remove(spilled.pop(), 1);
    } finally {
      await directory.close();
    }
    signal?.throwIfAborted();
    // The name may now be a link the execution swapped in; remove the link, never its target.
    await fs.rmdir(target).catch(async (error) => {
      if (error.code === "ENOENT") return;
      if (error.code !== "ENOTDIR") throw error;
      await fs.unlink(target);
    });
  };
  await remove(name);
}
export async function clearWindowState(profileDir, signal?: AbortSignal) {
  signal?.throwIfAborted();
  const profile = await fs.open(profileDir, directoryFlags).catch((error) => {
    if (error.code !== "ENOENT") throw error;
    return null;
  });
  if (!profile) return;
  let defaults: Awaited<ReturnType<typeof fs.open>>;
  try {
    // Linux isolation pins both ancestors across concurrent execution renames.
    const root = pinnedPath(profile, profileDir);
    defaults = await fs
      .open(path.join(root, "Default"), directoryFlags)
      .catch((error) => {
        if (error.code !== "ENOENT") throw error;
        return null;
      });
    if (!defaults) return;
    const directory = pinnedPath(defaults, path.join(root, "Default"));
    for (const name of ["Sessions", "Session Storage"])
      await removeTree(path.join(directory, name), signal, 100_000);
  } finally {
    await defaults?.close();
    await profile.close();
  }
}
// Every ancestor is runtime-owned. Pin each node before changing UID; never follow
// browser-controlled symlinks or transfer a hardlinked file outside this tree.
async function ownTree(directory, uid, signal?: AbortSignal) {
  let count = 0;
  const visit = async (name, depth = 0, parallel = true) => {
    signal?.throwIfAborted();
    if (++count > 100_000 || depth > MAX_TREE_DEPTH)
      throw Error("browser_profile_capacity_exceeded");
    const fd = await fs.open(name, noFollowFlags);
    try {
      const stat = await fd.stat();
      if (
        (!stat.isDirectory() && !stat.isFile()) ||
        (stat.isFile() && stat.nlink !== 1)
      )
        throw Error("browser_profile_entry_unsafe");
      if (stat.isDirectory()) {
        const pinned = "/proc/self/fd/" + fd.fd;
        const entries = await fs.readdir(pinned, { withFileTypes: true });
        const files = entries.filter((item) => !item.isDirectory());
        const batchSize = parallel ? 4 : 1;
        for (let offset = 0; offset < files.length; offset += batchSize) {
          const results = await Promise.allSettled(
            files.slice(offset, offset + batchSize).map((item) =>
              // A renamed file may now be a directory; keep that subtree serial.
              visit(path.join(pinned, item.name), depth + 1, false),
            ),
          );
          const failure = results.find((result) => result.status === "rejected");
          if (failure?.status === "rejected") throw failure.reason;
        }
        for (const item of entries)
          if (item.isDirectory())
            await visit(path.join(pinned, item.name), depth + 1, parallel);
      }
      // The browser container has CHOWN, but deliberately no FOWNER capability.
      signal?.throwIfAborted();
      if (stat.uid !== 0 || stat.gid !== 0) await fd.chown(0, 0);
      const mode = stat.isDirectory() ? 0o700 : 0o600;
      await fd.chmod(mode);
      if (uid !== 0) await fd.chown(uid, uid);
    } finally {
      await fd.close();
    }
  };
  await visit(directory);
}
async function fileLock(filename, signal?: AbortSignal) {
  signal?.throwIfAborted();
  const fd = await fs.open(
    filename,
    constants.O_CREAT | constants.O_WRONLY | constants.O_NOFOLLOW,
    0o600,
  );
  try {
    const stat = await fd.stat();
    if (!stat.isFile() || stat.nlink !== 1 || stat.uid !== 0)
      throw Error("browser_profile_lock_unsafe");
  } finally {
    await fd.close();
  }
  // flock spans processes/containers sharing the same locking-capable filesystem.
  // EOF releases the helper on parent death; the active receipt still fences an unverified Chrome.
  const child = spawn(
    "flock",
    [
      "-n",
      "-F",
      "-E",
      "73",
      filename,
      process.execPath,
      "-e",
      "process.stdout.write('locked\\n');process.stdin.resume();process.stdin.on('end',()=>process.exit(0))",
    ],
    { stdio: ["pipe", "pipe", "ignore"] },
  );
  child.stdin.on("error", () => {});
  const exited = new Promise<void>((resolve) => child.once("close", () => resolve()));
  const deadline = AbortSignal.any([
    ...(signal ? [signal] : []),
    AbortSignal.timeout(30_000),
  ]);
  try {
    await new Promise<void>((resolve, reject) => {
      const cleanup = () => {
        child.removeListener("error", failed);
        child.removeListener("exit", stopped);
        child.stdout.removeListener("data", ready);
        deadline.removeEventListener("abort", cancelled);
      };
      const done = (error?) => {
        cleanup();
        error ? reject(error) : resolve();
      };
      const failed = () => done(Error("browser_profile_lock_unavailable"));
      const stopped = (code) =>
        done(
          Error(
            code === 73 ? "browser_profile_busy" : "browser_profile_lock_unavailable",
          ),
        );
      const ready = () => done();
      const cancelled = () => done(deadline.reason);
      child.once("error", failed);
      child.once("exit", stopped);
      child.stdout.once("data", ready);
      deadline.addEventListener("abort", cancelled, { once: true });
      if (deadline.aborted) cancelled();
    });
  } catch (error) {
    child.kill("SIGKILL");
    await exited;
    throw error;
  }
  return async () => {
    child.stdin.end();
    const timer = setTimeout(() => child.kill("SIGKILL"), 1000);
    try {
      await exited;
    } finally {
      clearTimeout(timer);
    }
  };
}
/** Local private data only. No archive, database, credentials or remote transport. */
export function createLocalProfiles({ root }) {
  if (!path.isAbsolute(root ?? "")) throw Error("browser_profile_root_invalid");
  return {
    async acquire(profileId, executionId, uid, signal?: AbortSignal) {
      signal?.throwIfAborted();
      if (
        process.getuid?.() !== 0 ||
        !validProfileId(profileId) ||
        !Number.isSafeInteger(uid) ||
        uid <= 0
      )
        throw Error("browser_profile_host_required");
      await fs.mkdir(root, { recursive: true, mode: 0o711 });
      const parent = await fs.lstat(root);
      if (!parent.isDirectory() || parent.uid !== 0 || parent.mode & 0o022)
        throw Error("browser_profile_root_unsafe");
      const directory = path.join(
        root,
        createHash("sha256").update(profileId).digest("hex"),
      );
      await fs.mkdir(directory, { mode: 0o711 }).catch((error) => {
        if (error.code !== "EEXIST") throw error;
      });
      const stat = await fs.lstat(directory);
      if (!stat.isDirectory() || stat.uid !== 0 || stat.mode & 0o022)
        throw Error("browser_profile_root_unsafe");
      const unlock = await fileLock(path.join(directory, "lock"), signal);
      const active = path.join(directory, "active.json"),
        profileDir = path.join(directory, "profile");
      let created = false;
      try {
        signal?.throwIfAborted();
        // A free OS lock alone cannot prove an orphan Chrome has exited. Fail
        // closed after an unclean owner loss, without stealing permissions.
        const marker = await fs.open(active, "wx", 0o600).catch((error) => {
          if (error.code === "EEXIST") throw Error("browser_profile_stop_unconfirmed");
          throw error;
        });
        created = true;
        try {
          await marker.writeFile(JSON.stringify({ executionId, uid }));
        } finally {
          await marker.close();
        }
        await fs.mkdir(profileDir, { mode: 0o700 }).catch((error) => {
          if (error.code !== "EEXIST") throw error;
        });
        await ownTree(profileDir, uid, signal);
      } catch (error) {
        // No lease was handed out, so this attempt cannot have started Chrome.
        try {
          if (created) await fs.rm(active, { force: true });
        } finally {
          await unlock();
        }
        throw error;
      }
      let released = false;
      return {
        profileDir,
        journalDir: directory,
        async release(signal?: AbortSignal) {
          if (released) return;
          signal?.throwIfAborted();
          const exists = await fs.lstat(profileDir).catch((error) => {
            if (error.code === "ENOENT") return null;
            throw error;
          });
          if (exists) await ownTree(profileDir, 0, signal);
          await fs.rm(active);
          released = true;
          await unlock();
        },
        async abandon() {
          if (!released) {
            released = true;
            await unlock();
          }
        },
      };
    },
  };
}
