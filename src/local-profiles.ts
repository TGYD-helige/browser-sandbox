import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { once } from "node:events";
import { constants } from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";

export const validProfileId = (value) =>
  typeof value === "string" && /^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$/.test(value);
export async function clearWindowState(profileDir) {
  const flags =
    constants.O_RDONLY |
    constants.O_NOFOLLOW |
    constants.O_NONBLOCK |
    constants.O_DIRECTORY;
  const profile = await fs.open(profileDir, flags).catch((error) => {
    if (error.code !== "ENOENT") throw error;
    return null;
  });
  if (!profile) return;
  let defaults: Awaited<ReturnType<typeof fs.open>>;
  let count = 0;
  const pinned = (fd, name) =>
    process.platform === "linux" ? "/proc/self/fd/" + fd.fd : name;
  const remove = async (name, depth = 0) => {
    if (++count > 100_000 || depth > 128)
      throw Error("browser_profile_capacity_exceeded");
    const directory = await fs.open(name, flags).catch(async (error) => {
      if (error.code === "ENOENT") return null;
      if (error.code !== "ENOTDIR" && error.code !== "ELOOP") throw error;
      await fs.unlink(name);
      return null;
    });
    if (!directory) return;
    try {
      for (const item of await fs.readdir(pinned(directory, name)))
        await remove(path.join(pinned(directory, name), item), depth + 1);
    } finally {
      await directory.close();
    }
    await fs.rmdir(name);
  };
  try {
    // Linux isolation pins both ancestors across concurrent execution renames.
    const root = pinned(profile, profileDir);
    defaults = await fs.open(path.join(root, "Default"), flags).catch((error) => {
      if (error.code !== "ENOENT") throw error;
      return null;
    });
    if (!defaults) return;
    const directory = pinned(defaults, path.join(root, "Default"));
    for (const name of ["Sessions", "Session Storage"])
      await remove(path.join(directory, name));
  } finally {
    await defaults?.close();
    await profile.close();
  }
}
// Every ancestor is runtime-owned. Pin each node before changing UID; never follow
// browser-controlled symlinks or transfer a hardlinked file outside this tree.
async function ownTree(directory, uid) {
  let count = 0;
  const visit = async (name, depth = 0) => {
    if (++count > 100_000 || depth > 128)
      throw Error("browser_profile_capacity_exceeded");
    const fd = await fs.open(
      name,
      constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK,
    );
    try {
      const stat = await fd.stat();
      if (
        (!stat.isDirectory() && !stat.isFile()) ||
        (stat.isFile() && stat.nlink !== 1)
      )
        throw Error("browser_profile_entry_unsafe");
      if (stat.isDirectory())
        for (const item of await fs.readdir("/proc/self/fd/" + fd.fd))
          await visit("/proc/self/fd/" + fd.fd + "/" + item, depth + 1);
      // The browser container has CHOWN, but deliberately no FOWNER capability.
      await fd.chown(0, 0);
      await fd.chmod(stat.isDirectory() ? 0o700 : 0o600);
      await fd.chown(uid, uid);
    } finally {
      await fd.close();
    }
  };
  await visit(directory);
}
async function fileLock(filename) {
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
  const exited = once(child, "exit");
  exited.catch(() => {});
  await new Promise<void>((resolve, reject) => {
    child.once("error", () => reject(Error("browser_profile_lock_unavailable")));
    child.stdout.once("data", () => resolve());
    child.once("exit", (code) =>
      reject(
        Error(
          code === 73 ? "browser_profile_busy" : "browser_profile_lock_unavailable",
        ),
      ),
    );
  });
  return async () => {
    child.stdin.end();
    if (child.exitCode === null && child.signalCode === null) await exited;
  };
}
/** Local private data only. No archive, database, credentials or remote transport. */
export function createLocalProfiles({ root }) {
  if (!path.isAbsolute(root ?? "")) throw Error("browser_profile_root_invalid");
  return {
    async acquire(profileId, executionId, uid) {
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
      const unlock = await fileLock(path.join(directory, "lock"));
      const active = path.join(directory, "active.json"),
        profileDir = path.join(directory, "profile");
      let created = false;
      try {
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
        await ownTree(profileDir, uid);
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
        async release() {
          if (released) return;
          const exists = await fs.lstat(profileDir).catch((error) => {
            if (error.code === "ENOENT") return null;
            throw error;
          });
          if (exists) await ownTree(profileDir, 0);
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
