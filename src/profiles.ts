import { execFile } from "node:child_process";
import { createCipheriv, createDecipheriv, createHash, randomBytes } from "node:crypto";
import { createReadStream, createWriteStream } from "node:fs";
import fs from "node:fs/promises";
import { createRequire } from "node:module";
import path from "node:path";
import { Readable, Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import { promisify } from "node:util";
import { clearWindowState } from "./local-profiles.js";

// Resolve the pinned archive dependency only when persistence is used.
const require = createRequire(import.meta.url);
// Resolve only when archiving: the existing minimal Linux isolation fixture has no archive dependency.
const tar = () => require("tar-stream");
export const MAX_ARCHIVE_BYTES = 256 * 1024 * 1024;
const MAX_PROFILE_BYTES = 512 * 1024 * 1024;
const MAX_FILES = 100000;
const version = (signal?: AbortSignal) =>
  promisify(execFile)("/opt/google/chrome/chrome", ["--version"], {
    timeout: 10_000,
    signal,
    maxBuffer: 4096,
  }).then((r) => r.stdout.trim());
function compatibleVersion(saved, current) {
  if (typeof saved !== "string" || typeof current !== "string") return false;
  const parse = (value) =>
    /^(Google Chrome(?: for Testing)?|Chromium) (\d+)\.(\d+)\.(\d+)\.(\d+)$/.exec(
      value,
    );
  const before = parse(saved),
    after = parse(current);
  if (!before || !after || before[1] !== after[1]) return false;
  for (let index = 2; index < 6; index++) {
    if (before[index] !== after[index])
      return Number(after[index]) > Number(before[index]);
  }
  return true;
}
const digest = async (file, signal?: AbortSignal) => {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(file, { signal })) hash.update(chunk);
  return hash.digest("hex");
};
const transient = (name) =>
  /^(profile\/)?(Singleton(?:Socket|Lock|Cookie)|DevToolsActivePort)$/.test(name) ||
  /^(profile\/)?Default\/(Sessions|Session Storage|Cache|Code Cache|GPUCache)(\/|$)/.test(
    name,
  ) ||
  /^(profile\/)?(GPUCache|ShaderCache|GrShaderCache)(\/|$)/.test(name);
function bounded(limit) {
  let bytes = 0;
  return new Transform({
    transform(chunk, _, cb) {
      bytes += chunk.length;
      cb(bytes > limit ? Error("browser_profile_capacity_exceeded") : null, chunk);
    },
  });
}
function safeName(name) {
  return (
    typeof name === "string" &&
    name.length < 4096 &&
    !name.includes("\\") &&
    !name.includes("\0") &&
    !path.posix.isAbsolute(name) &&
    name.split("/").every((part) => part && part !== "." && part !== "..") &&
    (name === "profile" || name.startsWith("profile/") || name === "cookies.json")
  );
}
async function extract(plain, root, uid, gid = uid, signal?: AbortSignal) {
  const unpack = tar().extract();
  let bytes = 0,
    count = 0;
  let pending: Promise<void>;
  unpack.on("entry", (header, stream, next) => {
    pending = (async () => {
      signal?.throwIfAborted();
      if (
        !safeName(header.name) ||
        !["file", "directory"].includes(header.type) ||
        ++count > MAX_FILES ||
        !Number.isSafeInteger(header.size) ||
        header.size < 0 ||
        (bytes += header.size) > MAX_PROFILE_BYTES
      )
        throw Error("browser_profile_archive_invalid");
      if (transient(header.name)) {
        stream.resume();
        stream.once("end", next);
        return;
      }
      const target = path.join(root, header.name);
      if (header.type === "directory") {
        await fs.mkdir(target, { recursive: true, mode: 0o700 });
        stream.resume();
      } else {
        await fs.mkdir(path.dirname(target), { recursive: true, mode: 0o700 });
        await pipeline(
          stream,
          createWriteStream(target, { flags: "wx", mode: 0o600 }),
          { signal },
        );
      }
      signal?.throwIfAborted();
      next();
    })().catch((error) => unpack.destroy(error));
  });
  try {
    await pipeline(createReadStream(plain), unpack, { signal });
  } finally {
    // Drain an in-flight filesystem operation before releasing Profile ownership.
    await pending;
  }
  // Keep the new Profile root private until all descendants have been processed.
  async function own(dir) {
    for (const item of await fs.readdir(dir, { withFileTypes: true })) {
      signal?.throwIfAborted();
      const file = path.join(dir, item.name);
      if (item.isDirectory()) await own(file);
      signal?.throwIfAborted();
      await fs.chown(file, uid, gid);
    }
  }
  await own(path.join(root, "profile"));
  signal?.throwIfAborted();
  await fs.chown(path.join(root, "profile"), uid, gid);
}
async function pack(root, output, signal?: AbortSignal) {
  const archive = tar().pack();
  const writing = pipeline(
    archive,
    bounded(MAX_PROFILE_BYTES + 64 * 1024 * 1024),
    createWriteStream(output, { flags: "wx", mode: 0o600 }),
    { signal },
  );
  writing.catch(() => {});
  let bytes = 0,
    count = 0;
  async function visit(relative) {
    signal?.throwIfAborted();
    if (transient(relative)) return;
    if (!safeName(relative)) throw Error("browser_profile_entry_unsafe");
    const absolute = path.join(root, relative),
      stat = await fs.lstat(absolute);
    signal?.throwIfAborted();
    if (
      ++count > MAX_FILES ||
      (!stat.isFile() && !stat.isDirectory()) ||
      (stat.nlink > 1 && stat.isFile())
    )
      throw Error("browser_profile_entry_unsafe");
    if (stat.isDirectory()) {
      await new Promise<void>((resolve, reject) =>
        archive.entry({ name: relative, type: "directory", mode: 0o700 }, (error) =>
          error ? reject(error) : resolve(),
        ),
      );
      for (const child of (await fs.readdir(absolute)).sort())
        await visit(relative + "/" + child);
    } else {
      if ((bytes += stat.size) > MAX_PROFILE_BYTES)
        throw Error("browser_profile_capacity_exceeded");
      const entry = archive.entry({
        name: relative,
        size: stat.size,
        mode: 0o600,
      });
      await pipeline(createReadStream(absolute), entry, { signal });
    }
  }
  try {
    await visit("profile");
    if (await fs.stat(path.join(root, "cookies.json")).catch(() => null))
      await visit("cookies.json");
    archive.finalize();
    await writing;
  } catch (error) {
    archive.destroy(error);
    await writing.catch(() => {});
    throw error;
  }
}

/** Browser-owned archive lifecycle. Host only grants HTTP access and records the confirmed reference. */
export function createProfileArchive({
  browserVersion = version,
  fetchImpl = fetch,
}: {
  browserVersion?: (signal?: AbortSignal) => Promise<string>;
  fetchImpl?: (input: any, init?: any) => Promise<Response>;
} = {}) {
  return {
    async restore(entry, archive, signal) {
      signal?.throwIfAborted();
      if (archive?.error) throw Error("browser_profile_source_unavailable");
      const seed = archive.seed,
        currentVersion = await browserVersion(signal),
        root = entry.journalDir;
      signal?.throwIfAborted();
      entry.previousSha256 = seed?.sha256 ?? null;
      if (
        seed &&
        (seed.schema !== "browser-sandbox.profile/v1" ||
          !compatibleVersion(seed.browserVersion, currentVersion) ||
          !/^[a-f0-9]{64}$/.test(seed.key ?? "") ||
          !/^[a-f0-9]{64}$/.test(seed.sha256 ?? "") ||
          !/^[a-f0-9]{24}$/.test(seed.iv ?? "") ||
          !/^[a-f0-9]{32}$/.test(seed.tag ?? "") ||
          !Number.isSafeInteger(seed.byteSize) ||
          seed.byteSize < 1 ||
          seed.byteSize > MAX_ARCHIVE_BYTES)
      )
        throw Error("browser_profile_restore_incompatible");
      const encrypted = path.join(root, "restore.bin"),
        plain = path.join(root, "restore.tar");
      try {
        if (seed) {
          const response = await fetchImpl(seed.url, {
            redirect: "error",
            signal: AbortSignal.any([
              AbortSignal.timeout(120000),
              ...(signal ? [signal] : []),
            ]),
          });
          if (!response.ok || !response.body)
            throw Error("browser_profile_download_failed");
          await pipeline(
            Readable.fromWeb(
              response.body as import("node:stream/web").ReadableStream<Uint8Array>,
            ),
            bounded(seed.byteSize),
            createWriteStream(encrypted, { flags: "wx", mode: 0o600 }),
            ...(signal ? [{ signal }] : []),
          );
          if (
            (await fs.stat(encrypted)).size !== seed.byteSize ||
            (await digest(encrypted, signal)) !== seed.sha256
          )
            throw Error("browser_profile_digest_mismatch");
          const decipher = createDecipheriv(
            "aes-256-gcm",
            Buffer.from(seed.key, "hex"),
            Buffer.from(seed.iv, "hex"),
          );
          decipher.setAAD(Buffer.from(entry.profileId + "\n" + seed.browserVersion));
          decipher.setAuthTag(Buffer.from(seed.tag, "hex"));
          await pipeline(
            createReadStream(encrypted),
            decipher,
            bounded(MAX_PROFILE_BYTES + 64 * 1024 * 1024),
            createWriteStream(plain, { flags: "wx", mode: 0o600 }),
            ...(signal ? [{ signal }] : []),
          );
        }
        signal?.throwIfAborted();
        await fs.rm(entry.profileDir, { recursive: true, force: true });
        await fs.rm(path.join(root, "cookies.json"), { force: true });
        if (seed) await extract(plain, root, entry.uid, entry.gid, signal);
        else {
          await fs.mkdir(entry.profileDir, { mode: 0o700 });
          await fs.chown(entry.profileDir, entry.uid, entry.gid ?? entry.uid);
        }
        signal?.throwIfAborted();
        entry.profileVersion = currentVersion;
      } catch {
        throw Error("browser_profile_restore_failed");
      } finally {
        await fs.rm(encrypted, { force: true });
        await fs.rm(plain, { force: true });
      }
    },
    async save(entry, archive, signal) {
      const target = archive?.target;
      if (!target || !/^[a-f0-9]{64}$/.test(target.key ?? ""))
        throw Error("browser_profile_source_unavailable");
      const root = entry.journalDir,
        plain = path.join(root, "checkpoint.tar"),
        encrypted = path.join(root, "checkpoint.bin");
      try {
        signal?.throwIfAborted();
        await fs.rm(plain, { force: true });
        await fs.rm(encrypted, { force: true });
        await clearWindowState(entry.profileDir, signal);
        await pack(root, plain, signal);
        const iv = randomBytes(12),
          cipher = createCipheriv("aes-256-gcm", Buffer.from(target.key, "hex"), iv);
        cipher.setAAD(Buffer.from(entry.profileId + "\n" + entry.profileVersion));
        await pipeline(
          createReadStream(plain),
          cipher,
          bounded(MAX_ARCHIVE_BYTES),
          createWriteStream(encrypted, { flags: "wx", mode: 0o600 }),
          ...(signal ? [{ signal }] : []),
        );
        const sha256 = await digest(encrypted, signal),
          byteSize = (await fs.stat(encrypted)).size;
        signal?.throwIfAborted();
        const uploadSignal = AbortSignal.any([
          AbortSignal.timeout(120000),
          ...(signal ? [signal] : []),
        ]);
        const response = await fetchImpl(target.url, {
          method: "PUT",
          headers: { ...target.headers, "content-length": String(byteSize) },
          body: createReadStream(encrypted),
          duplex: "half",
          redirect: "error",
          signal: uploadSignal,
        });
        signal?.throwIfAborted();
        if (!response.ok) throw Error("browser_profile_upload_failed");
        return {
          used: true,
          previousSha256: entry.previousSha256,
          archive: {
            schema: "browser-sandbox.profile/v1",
            browserVersion: entry.profileVersion,
            iv: iv.toString("hex"),
            tag: cipher.getAuthTag().toString("hex"),
            sha256,
            byteSize,
            objectKey: target.objectKey,
            keyMaterial: target.keyMaterial,
          },
        };
      } finally {
        await fs.rm(plain, { force: true });
        await fs.rm(encrypted, { force: true });
      }
    },
  };
}
