import assert from "node:assert/strict";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

export async function verifyJournalRollback({
  run,
  kubectl,
  namespace,
  directory,
  values,
}) {
  const legacy = join(directory, "chart-v0.2.0");
  const archive = join(directory, "chart-v0.2.0.tar");
  const chart = "charts/browser-sandbox";
  const previousChart = join(legacy, chart);
  await mkdir(legacy);
  await run("git", ["archive", "-o", archive, "v0.2.0", chart]);
  await run("tar", ["-xf", archive, "-C", legacy]);

  const config = join(directory, "journal-rollback-values.json");
  const settings = JSON.parse(await readFile(values, "utf8"));
  delete settings.controllerData;
  settings.profileStore = { enabled: false };
  await writeFile(config, JSON.stringify(settings));

  for (const [retain, localProfile] of [
    [false, false],
    [true, false],
    [false, true],
  ]) {
    const release = localProfile ? "profile-rollback" : `journal-${retain}`;
    const journalClaim = `${release}-browser-data`;
    const profileClaim = `${release}-browser-profiles`;
    const profileSettings = localProfile ? ["--set", "profileStore.enabled=true"] : [];
    const helm = (...args) =>
      run("helm", [...args, "-n", namespace, "--wait", "--timeout", "120s"]);
    const claimUid = (claim = journalClaim) =>
      kubectl(
        "get",
        "pvc",
        claim,
        "--ignore-not-found",
        "-o",
        "jsonpath={.metadata.uid}",
      );
    let uid;
    let profileUid;
    let cleanupError;
    try {
      // Use the current image in each revision to isolate Helm's claim lifecycle.
      await helm(
        "install",
        release,
        previousChart,
        "-f",
        config,
        "--set",
        `controllerData.retain=${retain}`,
        ...profileSettings,
        "--set",
        "profileStore.retainOnDelete=false",
      );
      uid = await claimUid();
      assert(uid, `Expected a bound journal claim for retain=${retain}`);

      if (localProfile) {
        profileUid = await claimUid(profileClaim);
        assert(profileUid, "Expected the original Profile PVC");
      }

      await helm(
        "upgrade",
        release,
        chart,
        "--reuse-values",
        "--set",
        "controller.sharedState=true",
        "--set",
        "profileStore.retainOnDelete=true",
      );
      assert.equal(
        await claimUid(),
        uid,
        "Single-replica migration replaced the journal claim",
      );
      if (localProfile)
        assert.equal(
          await kubectl(
            "get",
            "pvc",
            profileClaim,
            "-o",
            "jsonpath={.metadata.annotations.helm\\.sh/resource-policy}",
          ),
          "keep",
        );
      await helm(
        "upgrade",
        release,
        chart,
        "-f",
        config,
        "--set",
        `controllerData.retain=${retain}`,
        "--set",
        "controller.replicas=3",
        "--set",
        "controller.sharedState=true",
      );
      assert.equal(
        await claimUid(),
        uid,
        "Multi-replica upgrade replaced the journal claim",
      );
      if (localProfile) {
        assert.equal(
          await claimUid(profileClaim),
          profileUid,
          "Scaling lost the retained Profile claim",
        );
        await helm(
          "upgrade",
          release,
          previousChart,
          "-f",
          config,
          ...profileSettings,
          "--set",
          `controllerData.retain=${retain}`,
          "--set",
          `profileStore.existingClaim=${profileClaim}`,
        );
        assert.equal(
          await claimUid(profileClaim),
          profileUid,
          "Rollback replaced the Profile claim",
        );
        const deployment = JSON.parse(
          await kubectl("get", "deployment", `${release}-browser`, "-o", "json"),
        );
        const profileVolume = deployment.spec.template.spec.volumes.find(
          (volume) => volume.name === "profile-store",
        );
        assert.equal(profileVolume?.persistentVolumeClaim.claimName, profileClaim);
      } else await helm("rollback", release, "1");
      assert.equal(
        await claimUid(),
        uid,
        `Rollback replaced the journal for retain=${retain}`,
      );
    } finally {
      await helm("uninstall", release, "--ignore-not-found").catch((error) => {
        cleanupError = error;
      });
    }
    if (cleanupError) throw cleanupError;
    assert.equal(
      await claimUid(),
      retain ? uid : "",
      `Uninstall did not respect retain=${retain}`,
    );
    if (localProfile)
      assert.equal(
        await claimUid(profileClaim),
        profileUid,
        "Uninstall removed the reattached Profile claim",
      );
  }
}
