import assert from "node:assert/strict";
import { randomBytes, randomUUID } from "node:crypto";

const root = process.env.BROWSER_PACKAGE_ROOT ?? "/opt/browser-sandbox";
const { connectCdp } = await import(`${root}/dist/cdp.js`);
const { createControllerRuntime } = await import(`${root}/dist/index.js`);

const runtime = createControllerRuntime(
  process.env.BROWSER_TEST_CONTROL_ORIGIN ??
    process.env.TEST_CONTROL_ORIGIN ??
    "http://127.0.0.1:8082",
);
const deadline = () => AbortSignal.timeout(180000);
const report = [];
const cleanup = new Set();
const phase = process.env.BROWSER_PROFILE_PHASE ?? "roundtrip";
const cookie = (name, value, domain = "profile.example") => ({
  name,
  value,
  domain,
  path: "/",
  expires: Math.floor(Date.now() / 1000) + 86400,
  secure: true,
  httpOnly: true,
});
const payloadCookies = (count) =>
  Array.from({ length: count }, (_, i) =>
    cookie("payload", randomBytes(1024).toString("hex"), `p${i}.example`),
  );
let latest;
const hostAdapter = {
  async load() {
    return {
      seed: latest
        ? {
            ...latest.archive,
            key: latest.key,
            url: process.env.BROWSER_HOST_ARCHIVE_URL,
          }
        : null,
    };
  },
  async save() {
    const key = randomBytes(32).toString("hex");
    return {
      target: {
        key,
        keyMaterial: { key },
        objectKey: "archive",
        url: process.env.BROWSER_HOST_ARCHIVE_URL,
      },
    };
  },
  async commit(result) {
    if (latest?.archive.sha256 === result.archive.sha256)
      return { confirmedSha: result.archive.sha256 };
    assert.equal(result.previousSha256, latest?.archive.sha256 ?? null);
    latest = { archive: result.archive, key: result.archive.keyMaterial.key };
    return { confirmedSha: result.archive.sha256 };
  },
};
async function open(profileId, mode) {
  const registration = { executionId: randomUUID(), profileId };
  let storage, cdp;
  const finish = async () => {
    cdp?.close();
    await cdp?.closed;
    const start = performance.now();
    const saved = await runtime.request(
      { op: "finalize", executionId: registration.executionId },
      { signal: deadline() },
    );
    storage?.close();
    cleanup.delete(finish);
    return { saved, finalizeMs: performance.now() - start };
  };
  cleanup.add(finish);
  if (mode === "host") storage = await runtime.attachStorage(registration, hostAdapter);
  const { wsEndpoint } = await runtime.request({
    op: "register",
    ...registration,
    storage: mode === "host" ? true : undefined,
  });
  const start = performance.now();
  cdp = await connectCdp(wsEndpoint);
  await cdp.call("Browser.getVersion");
  return {
    registration,
    cdp,
    readyMs: performance.now() - start,
    preparation: (
      await runtime.request({
        op: "worker_metrics",
        executionId: registration.executionId,
      })
    ).preparation,
    finish,
  };
}
try {
  await assert.rejects(
    runtime.request({ op: "register", executionId: randomUUID(), profileId: "" }),
    /browser_profile_id_invalid/,
  );
  if (phase === "missing-storage") {
    await assert.rejects(
      runtime.request({
        op: "register",
        executionId: randomUUID(),
        profileId: "missing-storage",
      }),
      /browser_profile_storage_unavailable/,
    );
    console.log(JSON.stringify({ missingStorageRejected: true }));
  } else if (phase === "interrupt-seed") {
    const current = await open("managed-upload-interrupt", "volume");
    await current.cdp.call("Storage.setCookies", {
      cookies: [cookie("interrupt", "confirmed"), ...payloadCookies(512)],
    });
    const result = await current.finish();
    assert(result.saved.archive.byteSize > 262144);
    console.log(JSON.stringify({ archiveBytes: result.saved.archive.byteSize }));
  } else if (phase === "interrupt-hold") {
    const current = await open("managed-upload-interrupt", "volume");
    await current.cdp.call("Storage.setCookies", {
      cookies: [cookie("interrupt", "unconfirmed")],
    });
    await runtime.request({
      op: "session",
      executionId: current.registration.executionId,
      action: "retain",
      durationMs: 120000,
    });
    current.cdp.close();
    await current.cdp.closed;
    cleanup.delete(current.finish);
    console.log(JSON.stringify({ executionId: current.registration.executionId }));
  } else if (phase === "interrupt-finish") {
    await assert.rejects(
      runtime.request(
        { op: "finalize", executionId: process.env.BROWSER_TEST_EXECUTION_ID },
        { signal: deadline() },
      ),
      /browser_control_failed|browser_profile_upload_failed/,
    );
    console.log(JSON.stringify({ interruptedUpload: "rejected" }));
  } else if (phase === "interrupt-restore") {
    const current = await open("managed-upload-interrupt", "volume");
    const { cookies } = await current.cdp.call("Storage.getCookies");
    assert(
      cookies.some((item) => item.name === "interrupt" && item.value === "confirmed"),
    );
    await current.finish();
    console.log(JSON.stringify({ interruptedUpload: "preserved" }));
  } else if (phase === "lease-hold") {
    const current = await open("managed-live-lease", "volume");
    await runtime.request({
      op: "session",
      executionId: current.registration.executionId,
      action: "retain",
      durationMs: 120000,
    });
    current.cdp.close();
    await current.cdp.closed;
    cleanup.delete(current.finish);
    console.log(JSON.stringify({ executionId: current.registration.executionId }));
  } else if (phase === "lease-check") {
    await assert.rejects(
      runtime.request({
        op: "register",
        executionId: randomUUID(),
        profileId: "managed-live-lease",
      }),
      /browser_profile_busy/,
    );
    console.log(JSON.stringify({ recoveredLease: "busy" }));
  } else if (phase === "lease-reuse") {
    const executionId = randomUUID();
    await runtime.request({
      op: "register",
      executionId,
      profileId: "managed-live-lease",
    });
    await runtime.request({ op: "finalize", executionId });
    console.log(JSON.stringify({ recoveredLeaseReleased: true }));
  } else if (phase === "restart-seed") {
    const current = await open("managed-volume-restart", "volume");
    await current.cdp.call("Storage.setCookies", {
      cookies: [cookie("restart", "confirmed")],
    });
    await current.finish();
    console.log(JSON.stringify({ restartSeed: "confirmed" }));
  } else if (phase === "restart-restore") {
    const current = await open("managed-volume-restart", "volume");
    const { cookies } = await current.cdp.call("Storage.getCookies");
    assert(
      cookies.some((item) => item.name === "restart" && item.value === "confirmed"),
    );
    await current.finish();
    console.log(JSON.stringify({ controllerRestart: "restored" }));
  } else {
    for (const mode of ["host", "volume"])
      for (const count of [1, 512])
        for (let sample = 0; sample < 3; sample++) {
          latest = undefined;
          const profileId = `managed-${mode}-${count}-${randomUUID()}`;
          const marker = randomUUID();
          const first = await open(profileId, mode);
          await assert.rejects(
            runtime.request({ op: "register", executionId: randomUUID(), profileId }),
            /browser_profile_busy/,
          );
          await first.cdp.call("Storage.setCookies", {
            cookies: [cookie("marker", marker), ...payloadCookies(count)],
          });
          const saved = await first.finish();
          const next = await open(profileId, mode);
          const { cookies } = await next.cdp.call("Storage.getCookies");
          assert(
            cookies.some((item) => item.name === "marker" && item.value === marker),
          );
          assert.equal(cookies.filter((item) => item.name === "payload").length, count);
          await next.finish();
          report.push({
            mode,
            sample,
            cookies: count,
            archiveBytes: saved.saved.archive.byteSize,
            firstBrowserReadyMs: first.readyMs,
            restoreReadyMs: next.readyMs,
            restoreMs: next.preparation.profileRestoreMs,
            profileFinalizeMs: saved.saved.profileFinalizeMs,
            finalizeMs: saved.finalizeMs,
          });
          if (mode === "volume")
            assert.deepEqual(
              await runtime.request({ op: "profile_delete", profileId }),
              {
                deleted: true,
              },
            );
        }
    const summary = [];
    for (const mode of ["host", "volume"])
      for (const cookies of [1, 512]) {
        const rows = report.filter(
          (item) => item.mode === mode && item.cookies === cookies,
        );
        const measurements = {};
        for (const metric of ["restoreMs", "profileFinalizeMs", "finalizeMs"]) {
          const values = rows.map((item) => item[metric]).sort((a, b) => a - b);
          assert(values.every(Number.isFinite));
          measurements[metric] = {
            medianMs: values[Math.floor(values.length / 2)],
            p95Ms: values[Math.ceil(values.length * 0.95) - 1],
          };
        }
        summary.push({
          mode,
          cookies,
          samples: rows.length,
          archiveBytes: rows.map((item) => item.archiveBytes),
          measurements,
        });
      }
    console.log(
      JSON.stringify({
        architecture: process.arch,
        platform: process.platform,
        node: process.version,
        profiles: report,
        summary,
      }),
    );
  }
} finally {
  const results = await Promise.allSettled([...cleanup].map((finish) => finish()));
  if (results.some((result) => result.status === "rejected")) {
    console.error("browser_profile_test_cleanup_failed");
    process.exitCode = 1;
  }
  runtime.close();
}
