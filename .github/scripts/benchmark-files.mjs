import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { lstat, rm } from "node:fs/promises";
import { request } from "node:http";
import { setTimeout as delay } from "node:timers/promises";
import { execution, resources, until, workload } from "./benchmark-fixture.mjs";
import { runBatch } from "./benchmark-load.mjs";

export async function upload(url, bytes) {
  const hash = createHash("sha256");
  const chunk = Buffer.alloc(64 * 1024, 0x5a);
  const response = await fetch(url, {
    method: "PUT",
    duplex: "half",
    redirect: "error",
    signal: AbortSignal.timeout(30000),
    body: (async function* () {
      for (let offset = 0; offset < bytes; offset += chunk.length) {
        const part = chunk.subarray(0, Math.min(chunk.length, bytes - offset));
        hash.update(part);
        yield part;
      }
    })(),
  });
  assert.equal(response.status, 201);
  assert.equal((await response.json()).byteSize, bytes);
  return hash.digest("hex");
}

export async function download(url, bytes, digest) {
  const response = await fetch(url, {
    redirect: "error",
    signal: AbortSignal.timeout(30000),
  });
  assert.equal(response.status, 200);
  const hash = createHash("sha256");
  let size = 0;
  for await (const chunk of response.body) {
    size += chunk.length;
    hash.update(chunk);
  }
  assert.equal(size, bytes);
  assert.equal(hash.digest("hex"), digest);
}

export function holdTransfer(url, method) {
  const req = request(url, {
    method,
    agent: false,
    headers: method === "PUT" ? { "content-length": 16 * 1024 * 1024 } : {},
  });
  req.setTimeout(10000, () => req.destroy(Error("Held transfer timed out")));
  let failure,
    cleanupStarted = false,
    admissionRejected = false,
    status;
  const remember = (error) => {
    if (!cleanupStarted && !admissionRejected) failure ??= error;
  };
  const closed = new Promise((resolve) =>
    req.once("close", () => {
      if (!cleanupStarted) remember(Error("Held transfer closed before cancellation"));
      resolve();
    }),
  );
  req.on("error", remember);
  const ready = new Promise((resolve, reject) => {
    req.once("response", (response) => {
      status = response.statusCode;
      response.on("error", remember);
      response.once("aborted", () =>
        remember(Error("Held transfer aborted before cancellation")),
      );
      if (response.statusCode !== 200) {
        admissionRejected = true;
        response.resume();
        reject(Error(`Held transfer was not admitted (${method} ${status})`));
      } else {
        response.pause();
        resolve();
      }
    });
    req.once("error", reject);
  });
  // PUT admission is observed through the fixture file, not a premature HTTP success.
  ready.catch(() => {});
  if (method === "PUT") {
    req.flushHeaders();
    req.write(Buffer.alloc(64 * 1024));
  } else req.end();
  return {
    ready,
    closed,
    status: () => status,
    error: () => failure,
    async close() {
      cleanupStarted = true;
      req.destroy();
      await closed;
      if (failure) throw failure;
    },
  };
}

export async function fillTransfers({ base, method, directory }) {
  const deadline = performance.now() + 10000;
  while (performance.now() < deadline) {
    const names = Array.from({ length: 16 }, () => randomUUID() + ".bin");
    const held = names.map((name) =>
      holdTransfer(method === "GET" ? base + "held.bin" : base + name, method),
    );
    let keep = false;
    try {
      let admitted;
      if (method === "GET") {
        const replies = await Promise.allSettled(held.map((entry) => entry.ready));
        for (let i = 0; i < replies.length; i++)
          if (replies[i].status === "rejected" && held[i].status() !== 429)
            throw replies[i].reason;
        admitted = replies.every((entry) => entry.status === "fulfilled");
      } else {
        admitted =
          (await until(
            async () => {
              for (const entry of held) {
                if (entry.error()) throw entry.error();
                if (entry.status() !== undefined && entry.status() !== 429)
                  throw Error(`Unexpected held upload status ${entry.status()}`);
              }
              if (held.some((entry) => entry.status() === 429)) return "retry";
              const sizes = await Promise.all(
                names.map(async (name) => {
                  try {
                    return (await lstat(`${directory}/${name}`)).size;
                  } catch (error) {
                    if (error.code === "ENOENT") return 0;
                    throw error;
                  }
                }),
              );
              return sizes.every((size) => size === 64 * 1024) ? "admitted" : null;
            },
            "Held uploads did not reach the runtime",
            Math.max(1, deadline - performance.now()),
          )) === "admitted";
      }
      if (admitted) {
        keep = true;
        return held;
      }
    } finally {
      if (!keep) {
        await Promise.all(held.map((entry) => entry.close()));
        if (method === "PUT")
          await Promise.all(
            names.map((name) => rm(`${directory}/${name}`, { force: true })),
          );
      }
    }
    await delay(10);
  }
  throw Error("File transfer admission did not recover sixteen slots");
}

export async function benchmarkFiles({ socket, origin, samples }) {
  const client = await execution(socket);
  try {
    const { cdp } = await client.ready(origin + "/page");
    const base = client.access.cdpUrl.replace(/^ws/, "http") + "/files/";
    const cases = [];
    for (const [name, bytes, concurrency] of [
      ["files-16MiB", 16 * 1024 * 1024, 1],
      ["files-four-4MiB", 4 * 1024 * 1024, 4],
    ]) {
      const measurements = { upload: [], download: [] },
        memoryBytes = [];
      for (let sample = -3; sample < samples; sample++) {
        const urls = Array.from(
          { length: concurrency },
          () => base + randomUUID() + ".bin",
        );
        let start = performance.now();
        const digests = await runBatch((i) => upload(urls[i], bytes), concurrency);
        const uploaded = performance.now() - start;
        start = performance.now();
        await runBatch((i) => download(urls[i], bytes, digests[i]), concurrency);
        const downloaded = performance.now() - start;
        await Promise.all(
          urls.map((url) =>
            rm(
              `/browser-data/${client.executionId}/files/${new URL(url).pathname.split("/").at(-1)}`,
            ),
          ),
        );
        if (sample >= 0) {
          measurements.upload.push(uploaded);
          measurements.download.push(downloaded);
          memoryBytes.push((await resources()).memoryBytes);
        }
      }
      cases.push(
        workload(
          name,
          measurements,
          { memoryBytes },
          {
            bytesPerSample: bytes * concurrency,
            uploadBytesPerSecond:
              (bytes * concurrency * samples * 1000) /
              measurements.upload.reduce((a, b) => a + b, 0),
            downloadBytesPerSecond:
              (bytes * concurrency * samples * 1000) /
              measurements.download.reduce((a, b) => a + b, 0),
          },
        ),
      );
    }
    const large = base + "held.bin",
      probe = base + "probe.bin";
    await upload(large, 16 * 1024 * 1024);
    await upload(probe, 1);
    for (const method of ["GET", "PUT"]) {
      const measurements = { controlRoundTrip: [], cancelAndRecover: [] };
      const sampled = {
        memoryBytes: [],
        heldMemoryBytes: [],
        retainedMemoryGrowthBytes: [],
        runtimeFdCount: [],
        processCount: [],
      };
      for (let sample = -3; sample < samples; sample++) {
        const before = await resources();
        const held = await fillTransfers({
          base,
          method,
          directory: `/browser-data/${client.executionId}/files`,
        });
        try {
          const denied = await fetch(probe, { signal: AbortSignal.timeout(5000) });
          await denied.arrayBuffer();
          assert.equal(
            denied.status,
            429,
            "All sixteen transfer slots must be occupied",
          );
          let start = performance.now();
          assert((await cdp.call("Browser.getVersion")).product);
          const control = performance.now() - start;
          const memory = (await resources()).memoryBytes;
          start = performance.now();
          await Promise.all(held.map((entry) => entry.close()));
          const recovered = await fillTransfers({ base, method: "GET" });
          try {
            const rejected = await fetch(probe, { signal: AbortSignal.timeout(5000) });
            await rejected.arrayBuffer();
            assert.equal(
              rejected.status,
              429,
              "Recovered slots must be held simultaneously",
            );
          } finally {
            await Promise.all(recovered.map((entry) => entry.close()));
          }
          const after = await until(async () => {
            const value = await resources();
            return value.runtimeFdCount <= before.runtimeFdCount + 2 &&
              value.processCount <= before.processCount + 2 &&
              value.memoryBytes <= before.memoryBytes + 128 * 1024 * 1024
              ? value
              : null;
          }, "Cancelled transfers retained runtime resources");
          assert(
            after.memoryBytes - before.memoryBytes <= 128 * 1024 * 1024,
            "Cancelled transfers retained excessive memory",
          );
          if (sample >= 0) {
            measurements.controlRoundTrip.push(control);
            measurements.cancelAndRecover.push(performance.now() - start);
            sampled.memoryBytes.push(after.memoryBytes);
            sampled.heldMemoryBytes.push(memory);
            sampled.retainedMemoryGrowthBytes.push(
              Math.max(0, after.memoryBytes - before.memoryBytes),
            );
            sampled.runtimeFdCount.push(after.runtimeFdCount);
            sampled.processCount.push(after.processCount);
          }
        } finally {
          await Promise.all(held.map((entry) => entry.close()));
        }
      }
      cases.push(
        workload(
          `files-${method === "GET" ? "slow-reader" : "cancel-upload"}`,
          measurements,
          sampled,
        ),
      );
    }
    return cases;
  } finally {
    await client.finish();
  }
}
