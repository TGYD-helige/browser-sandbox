import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import {
  assertFencedOwner,
  observeSingleRemoval,
  summarizeFailover,
  verifyAfterRollout,
} from "./active-active-checks.mjs";

/** Runs inside the existing native-architecture k3d job and uses its isolated kubeconfig. */
export async function activeActive({
  run,
  kubectl,
  namespace,
  directory,
  values,
  clientImage,
}) {
  const release = "ha";
  const deployment = "ha-browser";
  const prefix = "browser-sandbox.io/";
  const settings = JSON.parse(await readFile(values, "utf8"));
  // The preceding single-controller matrix retains a journal owned by a different installation.
  delete settings.controllerData?.existingClaim;
  settings.profileStore = { enabled: false };
  settings.controller = { replicas: 1 };
  settings.capacity = {
    maxWorkers: 4,
    maxPending: 2,
    maxQueued: 8,
    maxExecutionMs: 1200000,
    idleRegistrationMs: 900000,
  };
  settings.runtime = { TIMEOUT: "900000" };
  const config = join(directory, "active-active-values.json");
  await writeFile(config, JSON.stringify(settings));
  const helm = async (replicas, sharedState = true, install = false) =>
    run(
      "helm",
      [
        install ? "install" : "upgrade",
        release,
        "charts/browser-sandbox",
        "-n",
        namespace,
        "-f",
        config,
        "--set",
        `controller.replicas=${replicas}`,
        "--set",
        `controller.sharedState=${sharedState}`,
        "--wait",
        "--timeout",
        "180s",
      ],
      { timeout: 210000 },
    );
  const list = async (role) =>
    JSON.parse(
      await kubectl(
        "get",
        "pods",
        "-l",
        `app.kubernetes.io/instance=${release},app.kubernetes.io/component=${role}`,
        "-o",
        "json",
      ),
    ).items;
  const leases = async () =>
    JSON.parse(
      await kubectl(
        "get",
        "leases",
        "-l",
        `${prefix}installation=${release},${prefix}state=execution`,
        "-o",
        "json",
      ),
    ).items;
  const wait = async (description, read, predicate, timeout = 90000) => {
    const deadline = performance.now() + timeout;
    let last;
    while (true) {
      last = await read();
      if (predicate(last)) return last;
      assert(performance.now() < deadline, description);
      await delay(100);
    }
  };
  const owner = async (id) => {
    const lease = (await leases()).find(
      (item) => item.metadata.labels[prefix + "execution"] === id,
    );
    const pod = (await list("controller")).find(
      (item) =>
        item.metadata.uid === lease?.spec.holderIdentity &&
        !item.metadata.deletionTimestamp &&
        item.status.conditions?.some(
          (condition) => condition.type === "Ready" && condition.status === "True",
        ),
    );
    return { lease, pod };
  };
  const peers = async (id) => {
    const current = await wait(
      "Execution owner did not become ready",
      () => owner(id),
      (value) => Boolean(value.pod),
      45000,
    );
    const others = (await list("controller")).filter(
      (pod) =>
        pod.metadata.uid !== current.pod?.metadata.uid &&
        pod.status.conditions?.some((c) => c.type === "Ready" && c.status === "True"),
    );
    return { ...current, other: others[0] };
  };
  const client = async (action, id, pod, profileId, timeout = 120000) => {
    const agent = pod
      ? `http://${pod.status.podIP}:8080`
      : `http://${deployment}.${namespace}.svc:8080`;
    const control = pod
      ? `http://${pod.status.podIP}:8082`
      : `http://${deployment}-host.${namespace}.svc:8082`;
    return JSON.parse(
      await run(
        "kubectl",
        [
          "-n",
          namespace,
          "exec",
          "-i",
          "client",
          "--",
          "env",
          `TEST_CONTROL_ORIGIN=${control}`,
          `TEST_AGENT_ORIGIN=${agent}`,
          `TEST_ACTION=${action}`,
          `TEST_EXECUTION_ID=${id}`,
          ...(profileId ? [`TEST_PROFILE=${profileId}`] : []),
          `BROWSER_HOST_ARCHIVE_URL=http://${settings.networkPolicy.storageEgress[0].address}:9083/archive`,
          "node",
          "--input-type=module",
        ],
        {
          input: await readFile(
            ".github/scripts/managed/active-active-client.mjs",
            "utf8",
          ),
          timeout,
        },
      ),
    );
  };
  const fixture = JSON.parse(
    await readFile(
      ".github/scripts/managed/fixtures/journal-v0.2.0/11111111-1111-4111-8111-111111111111.json",
      "utf8",
    ),
  );
  fixture.installationId = release;
  // Only installation identity is substituted; the record itself came from the previous release.
  const fixtureText = JSON.stringify(fixture);
  async function journalJob(action) {
    const name = `ha-journal-${randomUUID().slice(0, 8)}`;
    const code = `const fs=require('node:fs');const root='/journal/executions';fs.mkdirSync(root,{recursive:true});const file=root+'/${fixture.executionId}.json';${action === "seed" ? `fs.writeFileSync(file,Buffer.from('${Buffer.from(fixtureText).toString("base64")}','base64'));fs.writeFileSync(root+'/22222222-2222-4222-8222-222222222222.json','{');` : `if(fs.readFileSync(file,'utf8')!==Buffer.from('${Buffer.from(fixtureText).toString("base64")}','base64').toString())throw Error('source_changed');if(fs.readFileSync(root+'/22222222-2222-4222-8222-222222222222.json','utf8')!=='{')throw Error('corrupt_source_changed');fs.unlinkSync(root+'/22222222-2222-4222-8222-222222222222.json');`}`;
    await run("kubectl", ["-n", namespace, "apply", "-f", "-"], {
      input: JSON.stringify({
        apiVersion: "v1",
        kind: "Pod",
        metadata: { name },
        spec: {
          restartPolicy: "Never",
          automountServiceAccountToken: false,
          securityContext: { runAsUser: 20000, runAsGroup: 20000, fsGroup: 20000 },
          containers: [
            {
              name: "fixture",
              image: clientImage,
              command: ["node", "-e", code],
              volumeMounts: [{ name: "journal", mountPath: "/journal" }],
            },
          ],
          volumes: [
            {
              name: "journal",
              persistentVolumeClaim: { claimName: `${deployment}-data` },
            },
          ],
        },
      }),
    });
    await wait(
      "Journal fixture job failed",
      async () => JSON.parse(await kubectl("get", "pod", name, "-o", "json")),
      (pod) => {
        assert.notEqual(pod.status.phase, "Failed");
        return pod.status.phase === "Succeeded";
      },
    );
    await kubectl("delete", "pod", name, "--wait=true", "--timeout=30s");
  }
  const readEvents = async (pod) =>
    (await kubectl("logs", pod.metadata.name)).split("\n").flatMap((line) => {
      try {
        return [JSON.parse(line)];
      } catch {
        return [];
      }
    });
  async function startLive(id, pod) {
    const file = `/tmp/ha-live-${id}.ready`;
    await kubectl(
      "exec",
      "client",
      "--",
      "node",
      "-e",
      `require('node:fs').rmSync('${file}',{force:true})`,
    );
    const live = client("live", id, pod, undefined, 210000);
    live.catch(() => {});
    await wait(
      "Live CDP connection was not ready",
      async () => {
        try {
          await kubectl(
            "exec",
            "client",
            "--",
            "node",
            "-e",
            `require('node:fs').readFileSync('${file}')`,
          );
          return true;
        } catch {
          return false;
        }
      },
      Boolean,
    );
    return { closed: live };
  }
  const report = {
    architecture: process.arch,
    githubSha: process.env.GITHUB_SHA ?? null,
  };
  const controllerProcess = async (pod) => {
    const id = pod.status.containerStatuses[0].containerID.replace(
      /^containerd:\/\//,
      "",
    );
    const tasks = await run("docker", [
      "exec",
      pod.spec.nodeName,
      "ctr",
      "-n",
      "k8s.io",
      "tasks",
      "ls",
    ]);
    const pid = tasks
      .split("\n")
      .find((line) => line.split(/\s+/)[0] === id)
      ?.split(/\s+/)[1];
    assert(
      /^[1-9][0-9]*$/.test(pid),
      "Controller process was not found in its node runtime",
    );
    return { node: pod.spec.nodeName, pid };
  };
  const signalProcess = (process, signal) =>
    run("docker", ["exec", process.node, "kill", `-${signal}`, process.pid]);
  let paused;
  let pausedProcess;
  let installed = false;
  let cordoned;
  const heldGcResources = [];
  let stage = "journal corruption";
  try {
    installed = true;
    await helm(1, false, true);
    await kubectl("scale", `deployment/${deployment}`, "--replicas=0");
    await wait(
      "Initial controller did not exit",
      () => list("controller"),
      (items) => items.length === 0,
    );
    await journalJob("seed");
    // Render/startup must reject corruption. Inspect reported CrashLoop state, never a fixed sleep.
    await run("helm", [
      "upgrade",
      release,
      "charts/browser-sandbox",
      "-n",
      namespace,
      "-f",
      config,
      "--set",
      "controller.sharedState=true",
    ]);
    await wait(
      "Corrupt journal was not rejected at startup",
      () => list("controller"),
      (items) =>
        items.some((pod) =>
          pod.status.containerStatuses?.some(
            (c) => c.restartCount > 0 && c.lastState?.terminated?.exitCode !== 0,
          ),
        ),
    );
    assert.equal(
      (await leases()).length,
      0,
      "Corrupt import published partial ownership",
    );
    await wait(
      "Corrupt journal rejection was not reported",
      async () => {
        const rejected = (await list("controller"))[0];
        const current = await kubectl("logs", rejected.metadata.name).catch(() => "");
        const previous = await kubectl(
          "logs",
          rejected.metadata.name,
          "--previous",
        ).catch(() => "");
        return current + previous;
      },
      (logs) => logs.includes("browser_execution_journal_corrupt"),
      15000,
    );
    await kubectl("scale", `deployment/${deployment}`, "--replicas=0");
    await wait(
      "Failed controller did not exit",
      () => list("controller"),
      (items) => items.length === 0,
    );
    await journalJob("repair");
    stage = "journal import";
    await helm(1);
    const migrated = await wait("v1 journal was not imported", leases, (items) =>
      items.some(
        (item) => item.metadata.labels[prefix + "execution"] === fixture.executionId,
      ),
    );
    assert(migrated.length);
    await kubectl("rollout", "restart", `deployment/${deployment}`);
    await kubectl("rollout", "status", `deployment/${deployment}`, "--timeout=120s");
    report.journalMigration = {
      previousRelease: "v0.2.0",
      idempotent: true,
      corruptSourcePreserved: true,
    };
    stage = "three replicas";
    await helm(3);
    const replicas = await wait(
      "Three ready controller Pods did not settle after scaling",
      () => list("controller"),
      (items) =>
        items.length === 3 &&
        items.every(
          (pod) =>
            !pod.metadata.deletionTimestamp &&
            pod.status.conditions?.some(
              (condition) => condition.type === "Ready" && condition.status === "True",
            ),
        ),
    );
    stage = "concurrent registration";
    const same = randomUUID();
    const registrations = await Promise.all(
      replicas.slice(0, 2).map((pod) => client("register", same, pod)),
    );
    assert.equal(registrations[0].path, registrations[1].path);
    await client("finish", same);
    stage = "Profile contention";
    const profileIds = [randomUUID(), randomUUID()];
    const contention = await Promise.allSettled(
      profileIds.map((id, i) => client("profile-contend", id, replicas[i])),
    );
    assert.equal(
      contention.filter((result) => result.status === "fulfilled").length,
      1,
    );
    const loser = contention.find((result) => result.status === "rejected");
    assert(String(loser.reason.errors).includes("browser_profile_busy"));
    await client(
      "finish",
      profileIds[contention.findIndex((result) => result.status === "fulfilled")],
      undefined,
      "ha-contended",
    );
    report.concurrentRegistrationAndProfileExclusivity = true;
    stage = "non-owner forwarding";
    const id = randomUUID();
    const prepared = await client("prepare", id);
    let current = await peers(id);
    await client("functional", id, current.other);
    const twice = await kubectl(
      "exec",
      "client",
      "--",
      "node",
      "-e",
      `fetch('http://${current.other.status.podIP}:8084/health',{signal:AbortSignal.timeout(2000)}).then(()=>process.exit(1),()=>{})`,
    );
    assert.equal(twice, "");
    report.nonOwnerCdpViewerSessionFilesAndHostForwarding = true;
    report.peerPortDeniedToHost = true;
    await kubectl(
      "exec",
      current.pod.metadata.name,
      "--",
      "node",
      "-e",
      `fetch('http://${current.other.status.podIP}:8084${prepared.path}/session',{signal:AbortSignal.timeout(5000)}).then(async r=>{if(r.status!==503||(await r.json()).error!=='browser_controller_owner_moved')process.exit(1)})`,
    );
    report.forwardedRequestNeverForwardedTwice = true;
    stage = "forwarding measurement";
    const snapshot = async (pod) =>
      JSON.parse(
        await kubectl(
          "exec",
          pod.metadata.name,
          "--",
          "node",
          "-e",
          String.raw`const fs=require('node:fs');const s=fs.readFileSync('/proc/1/stat','utf8').split(' ');const n=fs.readFileSync('/proc/net/dev','utf8').split('\n').find(x=>x.includes('eth0:')).split(':')[1].trim().split(/\s+/).map(Number);const result={cpuTicks:Number(s[13])+Number(s[14]),rx:n[0],tx:n[8]};if(!Object.values(result).every(Number.isFinite))throw Error('counter_invalid');console.log(JSON.stringify(result))`,
        ),
      );
    const measure = async (pod) => {
      const before = await Promise.all([
        snapshot(current.pod),
        snapshot(current.other),
      ]);
      const latency = await client("benchmark", id, pod);
      const after = await Promise.all([snapshot(current.pod), snapshot(current.other)]);
      return {
        ...latency,
        cpuTicks: after.reduce((n, v, i) => n + v.cpuTicks - before[i].cpuTicks, 0),
        rxBytes: after.reduce((n, v, i) => n + v.rx - before[i].rx, 0),
        txBytes: after.reduce((n, v, i) => n + v.tx - before[i].tx, 0),
        cpuTickHz: 100,
      };
    };
    report.forwarding = {
      ownerDirect: await measure(current.pod),
      nonOwner: await measure(current.other),
      environment:
        "native k3d; sum of owner and receiving controller /proc CPU/network counters; includes maintenance traffic",
    };
    const worker = (await list("worker")).find(
      (pod) => pod.metadata.labels[prefix + "execution"] === id,
    );
    stage = "worker execution identity";
    await kubectl(
      "exec",
      current.pod.metadata.name,
      "--",
      "node",
      "--input-type=module",
      "-e",
      `import{createControllerRuntime}from'/opt/browser-sandbox/dist/network-control.js';const r=createControllerRuntime('http://${worker.status.podIP}:8081',{epoch:${Number(current.lease.metadata.annotations[prefix + "epoch"]) + 1000}});try{await r.request({op:'worker_status',executionId:'${randomUUID()}'});throw Error('misroute_accepted')}catch(e){if(e.message!=='browser_identity_mismatch')throw e}finally{r.close()}`,
    );
    await client("verify", id, current.other);
    report.misroutedEpochDoesNotFenceAnotherExecution = true;
    stage = "persistent Profile failover";
    const persistentId = randomUUID();
    await client("prepare", persistentId, undefined, "ha-persistent");
    const persistentOwner = await peers(persistentId);
    const persistentEpoch = Number(
      persistentOwner.lease.metadata.annotations[prefix + "epoch"],
    );
    const persistentWorker = (await list("worker")).find(
      (pod) => pod.metadata.labels[prefix + "execution"] === persistentId,
    );
    await signalProcess(await controllerProcess(persistentOwner.pod), "KILL");
    await wait(
      "Persistent execution ownership did not move",
      () => owner(persistentId),
      (value) =>
        value.pod &&
        Number(value.lease.metadata.annotations[prefix + "epoch"]) > persistentEpoch,
      45000,
    );
    await client("verify", persistentId);
    assert.equal(
      (await list("worker")).find(
        (pod) => pod.metadata.labels[prefix + "execution"] === persistentId,
      ).metadata.uid,
      persistentWorker.metadata.uid,
    );
    await client("finish", persistentId).catch(() => {});
    await wait(
      "Disconnected host save did not retain recovery-blocked state",
      () => client("status", persistentId),
      (status) => status.execution.phase === "recovery-blocked",
    );
    await client("finish", persistentId, undefined, "ha-persistent");
    const restoredId = randomUUID();
    await client("restore", restoredId, undefined, "ha-persistent");
    await client("finish", restoredId, undefined, "ha-persistent");
    report.persistentProfileFailoverBlockedSaveReconnectAndColdRestore = true;
    stage = "owner kills";
    const failover = [];
    for (let sample = 0; sample < 5; sample++) {
      current = await peers(id);
      const epoch = Number(current.lease.metadata.annotations[prefix + "epoch"]);
      const live = await startLive(id, current.pod);
      const killedProcess = await controllerProcess(current.pod);
      const started = performance.now();
      await signalProcess(killedProcess, "KILL");
      await live.closed;
      await wait(
        "Owner kill did not transfer ownership",
        () => owner(id),
        (value) =>
          value.pod &&
          Number(value.lease.metadata.annotations[prefix + "epoch"]) > epoch,
        45000,
      );
      await client("verify", id);
      failover.push(performance.now() - started);
      assert.equal(
        (await list("worker")).find(
          (pod) => pod.metadata.labels[prefix + "execution"] === id,
        ).metadata.uid,
        worker.metadata.uid,
      );
      await kubectl("rollout", "status", `deployment/${deployment}`, "--timeout=120s");
    }
    report.failover = {
      ...summarizeFailover(failover),
      workerAndProfilePreserved: true,
    };
    current = await peers(id);
    stage = "SIGSTOP fencing";
    paused = current.pod.metadata.name;
    const oldEpoch = Number(current.lease.metadata.annotations[prefix + "epoch"]);
    const beforePauseEvents = (await readEvents(current.pod)).length;
    const pausedRestartCount = current.pod.status.containerStatuses[0].restartCount;
    const readyFile = `/tmp/ha-stale-${id}.ready`;
    const releaseFile = `/tmp/ha-stale-${id}.release`;
    // Keep the actual pre-takeover Secret resourceVersion in a waiting protocol client.
    const staleProbe = kubectl(
      "exec",
      paused,
      "--",
      "node",
      "--input-type=module",
      "-e",
      `import{readFile,writeFile}from'node:fs/promises';import{setTimeout as delay}from'node:timers/promises';import{createKubernetesClient,readKubernetesAccess,KubernetesError}from'/opt/browser-sandbox/dist/kubernetes-api.js';const access=await readKubernetesAccess();const api=createKubernetesClient(access);const path='/api/v1/namespaces/'+access.namespace+'/secrets/${current.lease.metadata.name}';const secret=await api(path,'GET',undefined,AbortSignal.timeout(5000));await writeFile('${readyFile}','ready');const deadline=Date.now()+60000;while(true){try{await readFile('${releaseFile}');break}catch(e){if(e.code!=='ENOENT')throw e}if(Date.now()>deadline)throw Error('probe_timeout');await delay(25)}try{await api(path,'PUT',secret,AbortSignal.timeout(5000));throw Error('stale_write_accepted')}catch(e){if(!(e instanceof KubernetesError)||e.status!==409)throw e}`,
    );
    staleProbe.catch(() => {});
    await wait(
      "Stale writer did not capture the old Secret",
      async () => {
        try {
          await kubectl(
            "exec",
            paused,
            "--",
            "node",
            "-e",
            `require('node:fs').readFileSync('${readyFile}')`,
          );
          return true;
        } catch {
          return false;
        }
      },
      Boolean,
    );
    pausedProcess = await controllerProcess(current.pod);
    await signalProcess(pausedProcess, "STOP");
    await wait(
      "Owner process did not report SIGSTOP",
      () =>
        run("docker", [
          "exec",
          pausedProcess.node,
          "cat",
          `/proc/${pausedProcess.pid}/status`,
        ]),
      (status) => /^State:\s+T/m.test(status),
    );
    const next = await wait(
      "Paused owner was not fenced by takeover",
      () => owner(id),
      (value) =>
        value.pod &&
        Number(value.lease.metadata.annotations[prefix + "epoch"]) > oldEpoch,
      45000,
    );
    await client("verify", id, next.pod);
    await signalProcess(pausedProcess, "CONT");
    paused = undefined;
    pausedProcess = undefined;
    await client("verify", id);
    await kubectl(
      "exec",
      current.pod.metadata.name,
      "--",
      "node",
      "-e",
      `require('node:fs').writeFileSync('${releaseFile}','go')`,
    );
    await staleProbe;
    await wait(
      "Resumed controller did not report fencing",
      () => readEvents(current.pod),
      (events) =>
        events
          .slice(beforePauseEvents)
          .some((event) => event.executionId === id && event.type === "fenced"),
      15000,
    );
    const resumed = JSON.parse(
      await kubectl("get", "pod", current.pod.metadata.name, "-o", "json"),
    );
    assert.equal(resumed.metadata.uid, current.pod.metadata.uid);
    assert.equal(resumed.status.containerStatuses[0].restartCount, pausedRestartCount);
    // The old process must run after SIGCONT, report its own fencing and report no worker removal.
    assertFencedOwner((await readEvents(current.pod)).slice(beforePauseEvents), id);
    await kubectl(
      "exec",
      current.pod.metadata.name,
      "--",
      "node",
      "--input-type=module",
      "-e",
      `import{createControllerRuntime}from'/opt/browser-sandbox/dist/network-control.js';const old=createControllerRuntime('http://${worker.status.podIP}:8081',{epoch:${oldEpoch},ownerUid:${JSON.stringify(current.lease.metadata.uid)}});const next=createControllerRuntime('http://${worker.status.podIP}:8081',{epoch:${Number(next.lease.metadata.annotations[prefix + "epoch"])},ownerUid:${JSON.stringify(next.lease.metadata.uid)}});try{try{await old.request({op:'finalize',executionId:'${id}'});throw Error('old_epoch_accepted')}catch(e){if(e.message!=='browser_controller_fenced')throw e}const status=await next.request({op:'worker_status',executionId:'${id}'});if(status.protocol!=='browser-sandbox.worker/v2')throw Error('worker_epoch_regressed')}finally{old.close();next.close()}`,
    );
    report.resumedOldControllerReportedFencingWithoutRemoval = true;
    report.workerEpochDidNotRegressAfterResume = true;
    assertFencedOwner((await readEvents(current.pod)).slice(beforePauseEvents), id);
    assert.equal(
      (await list("worker")).find(
        (pod) => pod.metadata.labels[prefix + "execution"] === id,
      ).metadata.uid,
      worker.metadata.uid,
    );
    report.splitBrainFenced = true;
    stage = "rolling update";
    current = await peers(id);
    const live = await startLive(id, current.pod);
    const rollingEpoch = Number(current.lease.metadata.annotations[prefix + "epoch"]);
    const rollingStarted = performance.now();
    const rolling = kubectl("rollout", "restart", `deployment/${deployment}`);
    rolling.catch(() => {});
    await live.closed;
    await wait(
      "Graceful rollout did not hand off execution ownership",
      () => owner(id),
      (value) =>
        value.pod &&
        Number(value.lease.metadata.annotations[prefix + "epoch"]) > rollingEpoch,
      45000,
    );
    await client("verify-rollout", id);
    report.gracefulRollout = {
      takeoverMs: performance.now() - rollingStarted,
      samples: 1,
      liveCdpBeforeShutdown: true,
    };
    await rolling;
    await kubectl("rollout", "status", `deployment/${deployment}`, "--timeout=180s");
    stage = "post-rollout CDP verification";
    const rolled = JSON.parse(
      await kubectl("get", "deployment", deployment, "-o", "json"),
    );
    const revision = "deployment.kubernetes.io/revision";
    const replicaSet = JSON.parse(
      await kubectl(
        "get",
        "replicasets",
        "-l",
        `app.kubernetes.io/instance=${release},app.kubernetes.io/component=controller`,
        "-o",
        "json",
      ),
    ).items.find(
      (item) =>
        item.metadata.annotations?.[revision] ===
          rolled.metadata.annotations[revision] &&
        item.metadata.ownerReferences?.some(
          (ref) => ref.controller && ref.uid === rolled.metadata.uid,
        ),
    );
    assert(replicaSet?.metadata.uid, "Rolled Deployment ReplicaSet not found");
    await verifyAfterRollout(
      wait,
      () => owner(id),
      replicaSet.metadata.uid,
      async (pod) =>
        JSON.parse(
          await kubectl(
            "exec",
            pod.metadata.name,
            "--",
            "node",
            "--input-type=module",
            "-e",
            `import{createControllerRuntime,sessionRequest}from'/opt/browser-sandbox/dist/index.js';const runtime=createControllerRuntime('http://127.0.0.1:8084');const signal=AbortSignal.timeout(5000);try{const status=await runtime.request({op:'controller_status',executionId:'${id}'},{signal});if(status.execution?.phase==='active')await sessionRequest('ws://127.0.0.1:8084${prepared.path}','status',undefined,signal);console.log(JSON.stringify(status))}finally{runtime.close()}`,
          ),
        ),
      () => client("verify", id),
    );
    report.postRolloutVerifiedReadyOwner = true;
    assert.equal(
      (await list("worker")).find(
        (pod) => pod.metadata.labels[prefix + "execution"] === id,
      ).metadata.uid,
      worker.metadata.uid,
    );
    report.rollingUpdatePreservedOriginalPageAndWorker = true;
    await client("finish", id);
    // A worker created through the normal adapter with no record is an orphan, claimed by one Lease winner.
    stage = "orphan reclamation";
    const orphanId = randomUUID();
    const orphan = {
      apiVersion: "v1",
      kind: "Pod",
      metadata: {
        name: `ha-orphan-${orphanId.slice(0, 8)}`,
        labels: {
          ...worker.metadata.labels,
          [prefix + "execution"]: orphanId,
          [prefix + "incarnation"]: randomUUID(),
        },
        annotations: { [prefix + "profile-id"]: "" },
        ownerReferences: [
          {
            apiVersion: "v1",
            kind: "ConfigMap",
            name: `${deployment}-workers`,
            uid: JSON.parse(
              await kubectl("get", "configmap", `${deployment}-workers`, "-o", "json"),
            ).metadata.uid,
            controller: true,
            blockOwnerDeletion: false,
          },
        ],
      },
      spec: structuredClone(worker.spec),
    };
    const environment = orphan.spec.containers[0].env;
    environment.find((entry) => entry.name === "BROWSER_EXECUTION_ID").value = orphanId;
    orphan.spec.containers[0].env = environment.filter(
      (entry) => entry.name !== "BROWSER_OWNER_UID",
    );
    await run("kubectl", ["-n", namespace, "apply", "-f", "-"], {
      input: JSON.stringify(orphan),
    });
    await wait(
      "Orphan was not reclaimed",
      () => list("worker"),
      (items) =>
        !items.some((pod) => pod.metadata.labels[prefix + "execution"] === orphanId),
      90000,
    );
    const removalCount = async () => {
      const logs = await Promise.all(
        (await list("controller")).map((pod) => kubectl("logs", pod.metadata.name)),
      );
      const removed = logs
        .flatMap((text) =>
          text.split("\n").flatMap((line) => {
            try {
              return [JSON.parse(line)];
            } catch {
              return [];
            }
          }),
        )
        .filter((event) => event.executionId === orphanId && event.type === "removed");
      return removed.length;
    };
    await wait(
      "Orphan removal event was not reported exactly once",
      removalCount,
      (count) => {
        assert(count <= 1, "Orphan was reclaimed more than once");
        return count === 1;
      },
    );
    report.orphanObservationMs = await observeSingleRemoval(removalCount);
    report.orphanReclaimedOnce = true;
    stage = "Lease UID garbage collection";
    const gcId = randomUUID();
    await client("register", gcId);
    const prior = await peers(gcId);
    await kubectl("delete", "lease", prior.lease.metadata.name, "--wait=true");
    await wait(
      "Deleted Lease did not fence its controller",
      () => readEvents(prior.pod),
      (events) =>
        events.some((event) => event.executionId === gcId && event.type === "fenced"),
    );
    await client("register", gcId);
    const recreated = await peers(gcId);
    assert.notEqual(recreated.lease.metadata.uid, prior.lease.metadata.uid);
    assert.equal(Number(recreated.lease.metadata.annotations[prefix + "epoch"]), 1);
    const staleOwner = {
      apiVersion: "coordination.k8s.io/v1",
      kind: "Lease",
      name: prior.lease.metadata.name,
      uid: prior.lease.metadata.uid,
      controller: true,
      blockOwnerDeletion: false,
    };
    const lateName = `ha-late-${gcId.slice(0, 8)}`;
    const metadata = {
      name: lateName,
      labels: {
        ...worker.metadata.labels,
        [prefix + "execution"]: gcId,
        [prefix + "incarnation"]: randomUUID(),
      },
      annotations: { [prefix + "profile-id"]: "", [prefix + "epoch"]: "1" },
      ownerReferences: [staleOwner],
      finalizers: ["browser-sandbox.io/gc-probe"],
    };
    const lateSpec = structuredClone(worker.spec);
    for (const entry of lateSpec.containers[0].env) {
      if (entry.name === "BROWSER_EXECUTION_ID") entry.value = gcId;
      if (entry.name === "BROWSER_OWNER_UID") entry.value = prior.lease.metadata.uid;
    }
    for (const resource of [
      { apiVersion: "v1", kind: "Pod", metadata, spec: lateSpec },
      {
        apiVersion: "v1",
        kind: "Secret",
        metadata,
        type: "Opaque",
        stringData: { probe: "old-uid" },
      },
    ]) {
      heldGcResources.push(`${resource.kind.toLowerCase()}/${lateName}`);
      await run("kubectl", ["-n", namespace, "create", "-f", "-"], {
        input: JSON.stringify(resource),
      });
    }
    for (const resource of heldGcResources)
      await wait(
        "GC did not mark the late old-UID dependent for deletion",
        async () => JSON.parse(await kubectl("get", resource, "-o", "json")),
        (item) => Boolean(item.metadata.deletionTimestamp),
      );
    await kubectl(
      "exec",
      recreated.pod.metadata.name,
      "--",
      "node",
      "--input-type=module",
      "-e",
      `import assert from 'node:assert/strict';import{readFile}from'node:fs/promises';import{readKubernetesAccess}from'/opt/browser-sandbox/dist/kubernetes-api.js';import{createKubernetesOwnership}from'/opt/browser-sandbox/dist/kubernetes-ownership.js';import{createKubernetesBackend}from'/opt/browser-sandbox/dist/kubernetes-backend.js';const env=process.env;const options={...await readKubernetesAccess(),installation:env.BROWSER_INSTALLATION,ownerName:env.BROWSER_KUBERNETES_OWNER};const ownership=await createKubernetesOwnership({...options,replica:env.BROWSER_CONTROLLER_ID,peerOrigin:env.BROWSER_PEER_ORIGIN});try{const backend=await createKubernetesBackend({...options,ownership,image:env.BROWSER_IMAGE,template:JSON.parse(await readFile(env.BROWSER_WORKER_TEMPLATE??'/etc/browser-sandbox/worker-template.json','utf8'))});assert(!(await backend.list(AbortSignal.timeout(15000))).some(w=>w.id.startsWith('${lateName}/')))}finally{await ownership.close()}`,
    );
    for (const resource of heldGcResources) {
      await kubectl(
        "patch",
        resource,
        "--type=merge",
        "-p",
        JSON.stringify({ metadata: { finalizers: [] } }),
      );
      await kubectl("wait", "--for=delete", resource, "--timeout=60s");
    }
    heldGcResources.length = 0;
    const liveLease = JSON.parse(
      await kubectl("get", "lease", recreated.lease.metadata.name, "-o", "json"),
    );
    const liveSecret = JSON.parse(
      await kubectl("get", "secret", recreated.lease.metadata.name, "-o", "json"),
    );
    assert.equal(liveLease.metadata.uid, recreated.lease.metadata.uid);
    assert.equal(
      liveSecret.metadata.ownerReferences[0].uid,
      recreated.lease.metadata.uid,
    );
    await client("finish", gcId);
    report.lateOldUidPodAndSecretGarbageCollected = true;
    report.discoverySkippedOldUidPod = true;
    report.recreatedExecutionEpochOneHasNewUid = true;

    stage = "shared worker drain without force";
    const drainId = randomUUID();
    await client("prepare", drainId);
    const drainOwner = await peers(drainId);
    const draining = (await list("worker")).find(
      (pod) => pod.metadata.labels[prefix + "execution"] === drainId,
    );
    assert.equal(draining.metadata.ownerReferences.length, 1);
    assert.equal(draining.metadata.ownerReferences[0].kind, "Lease");
    assert.equal(draining.metadata.ownerReferences[0].controller, true);
    assert.equal(
      draining.metadata.ownerReferences[0].uid,
      drainOwner.lease.metadata.uid,
    );
    cordoned = draining.spec.nodeName;
    await kubectl(
      "drain",
      cordoned,
      "--ignore-daemonsets",
      "--delete-emptydir-data",
      `--pod-selector=app.kubernetes.io/instance=${release},app.kubernetes.io/component=worker,${prefix}execution=${drainId}`,
      "--timeout=90s",
    );
    await kubectl("uncordon", cordoned);
    cordoned = undefined;
    assert(
      !(await list("worker")).some(
        (pod) => pod.metadata.labels[prefix + "execution"] === drainId,
      ),
    );
    report.sharedStateDrainWithoutForce = true;

    stage = "conditional Lease delete";
    const compactId = randomUUID();
    await client("register", compactId);
    const compactOwner = await peers(compactId);
    await client("finish", compactId);
    await kubectl(
      "exec",
      compactOwner.pod.metadata.name,
      "--",
      "node",
      "--input-type=module",
      "-e",
      `import assert from 'node:assert/strict';
       import{createKubernetesClient,readKubernetesAccess,KubernetesError}from'/opt/browser-sandbox/dist/kubernetes-api.js';
       const access=await readKubernetesAccess(), api=createKubernetesClient(access);
       const path='/apis/coordination.k8s.io/v1/namespaces/'+access.namespace+'/leases/${compactOwner.lease.metadata.name}';
       const signal=AbortSignal.timeout(15000);
       const {metadata,spec}=await api(path,'GET',undefined,signal);
       assert.equal(metadata.annotations['browser-sandbox.io/phase'],'finished');
       assert(!spec.holderIdentity);
       await assert.rejects(api(path,'DELETE',{preconditions:{uid:metadata.uid,resourceVersion:${JSON.stringify(compactOwner.lease.metadata.resourceVersion)}}},signal),error=>error instanceof KubernetesError && error.status===409);
       await api(path,'DELETE',{preconditions:{uid:metadata.uid,resourceVersion:metadata.resourceVersion}},signal);
       await assert.rejects(api(path,'GET',undefined,signal),error=>error instanceof KubernetesError && error.status===404);`,
    );
    report.conditionalLeaseDeleteAccepted = true;

    stage = "shared state uninstall";
    const sharedState = async () =>
      JSON.parse(
        await kubectl(
          "get",
          "leases,secrets",
          "-l",
          `${prefix}installation=${release}`,
          "-o",
          "json",
        ),
      ).items;
    const beforeUninstall = await sharedState();
    for (const [kind, state] of [
      ["Lease", "execution"],
      ["Secret", "execution"],
      ["Lease", "profile"],
      ["Secret", "migration"],
    ])
      assert(
        beforeUninstall.some(
          (item) =>
            item.kind === kind && item.metadata.labels?.[prefix + "state"] === state,
        ),
        `Uninstall fixture missing ${state} ${kind}`,
      );
    report.uninstallHadAllSharedStateKinds = true;
    await run(
      "helm",
      ["uninstall", release, "-n", namespace, "--wait", "--timeout", "120s"],
      { timeout: 150000 },
    );
    installed = false;
    await wait(
      "Uninstall retained shared Leases or Secrets",
      sharedState,
      (items) => items.length === 0,
    );
    report.uninstallRemovedAllSharedLeasesAndSecrets = true;
    await writeFile(
      "managed-results/active-active-kubernetes.json",
      JSON.stringify(report, null, 2) + "\n",
    );
    return report;
  } catch (error) {
    const diagnostic = { stage, githubSha: report.githubSha, controllers: [] };
    for (const pod of await list("controller").catch(() => [])) {
      const logs = await kubectl("logs", pod.metadata.name, "--tail=50").catch(
        () => "",
      );
      const previous = await kubectl(
        "logs",
        pod.metadata.name,
        "--previous",
        "--tail=50",
      ).catch(() => "");
      diagnostic.controllers.push({
        phase: pod.status.phase,
        codes: [
          ...new Set(
            (logs + previous).match(
              /\bbrowser_[a-z_]+\b|\b(?:EACCES|ENOENT|EPERM|SyntaxError|TypeError|RangeError)\b|status: [0-9]{3}/g,
            ) ?? [],
          ),
        ],
      });
    }
    await writeFile(
      "managed-results/active-active-diagnostics.json",
      JSON.stringify(diagnostic, null, 2) + "\n",
    );
    console.error(JSON.stringify({ activeActiveFailed: diagnostic }));
    throw error;
  } finally {
    if (pausedProcess) await signalProcess(pausedProcess, "CONT").catch(() => {});
    if (cordoned) await kubectl("uncordon", cordoned).catch(() => {});
    for (const resource of heldGcResources)
      await kubectl(
        "patch",
        resource,
        "--type=merge",
        "-p",
        JSON.stringify({ metadata: { finalizers: [] } }),
      ).catch(() => {});
    if (installed)
      await run(
        "helm",
        ["uninstall", release, "-n", namespace, "--wait", "--timeout", "120s"],
        { timeout: 150000 },
      ).catch(() => {});
  }
}
