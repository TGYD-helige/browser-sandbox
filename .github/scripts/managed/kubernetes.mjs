import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";

const directory = await mkdtemp(join(tmpdir(), "browser-kubernetes-"));
const suffix = randomUUID().slice(0, 8);
const cluster = process.env.TEST_K3D_CLUSTER ?? `browser-${suffix}`;
const namespace = `browser-${suffix}`;
const release = "trial";
const deployment = "trial-browser";
const image = process.env.BROWSER_IMAGE ?? "browser-sandbox:managed";
const nextImage = `browser-sandbox:protocol-${suffix}`;
const clientImage =
  process.env.BROWSER_CLIENT_IMAGE ?? "browser-sandbox-client:managed";
const kubeconfig = process.env.TEST_K3D_CLUSTER
  ? process.env.KUBECONFIG
  : join(directory, "kubeconfig");
assert(kubeconfig, "An isolated KUBECONFIG is required for an existing test cluster");
const environment = { ...process.env, KUBECONFIG: kubeconfig };
let stage = "cluster setup";
let installed = false;
let hostStorageAddress;
let clusterAttempted = false;
let cordoned;
let heldFinalizer;
let extraction;
const run = (binary, args, { input, timeout = 180000, env = {} } = {}) =>
  new Promise((resolve, reject) => {
    const child = spawn(binary, args, {
      env: { ...environment, ...env },
      stdio: ["pipe", "pipe", "pipe"],
    });
    let output = "",
      errors = "";
    const timer = setTimeout(() => child.kill("SIGKILL"), timeout);
    child.stdout.on("data", (data) => {
      output += data;
      if (output.length > 4194304) child.kill("SIGKILL");
    });
    child.stderr.on("data", (data) => {
      errors = (errors + data).slice(-1048576);
    });
    child.once("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.once("close", (code) => {
      clearTimeout(timer);
      if (code === 0) resolve(output.trim());
      else
        reject(
          Object.assign(Error(`${binary} failed (${code}): ${errors}`), {
            output,
            errors,
          }),
        );
    });
    child.stdin.on("error", () => {});
    child.stdin.end(input);
  });
const kubectl = (...args) =>
  run("kubectl", ["--request-timeout=15s", "-n", namespace, ...args]);
const pods = async () =>
  JSON.parse(
    await kubectl(
      "get",
      "pods",
      "-l",
      "app.kubernetes.io/component=worker",
      "-o",
      "json",
    ),
  ).items;
const client = async (action = "functional", executionId) => {
  await controllerReady();
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
        `TEST_CONTROL_ORIGIN=http://${deployment}-host.${namespace}.svc:8082`,
        `TEST_ACTION=${action}`,
        ...(executionId ? [`TEST_EXECUTION_ID=${executionId}`] : []),
        "node",
        "--input-type=module",
      ],
      {
        input: await readFile(".github/scripts/managed/kubernetes-client.mjs", "utf8"),
        timeout: 240000,
      },
    ),
  );
};
const profileClient = async (phase = "roundtrip", executionId) => {
  stage = `persistent Profiles: ${phase}`;
  await controllerReady();
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
        `TEST_CONTROL_ORIGIN=http://${deployment}-host.${namespace}.svc:8082`,
        `BROWSER_HOST_ARCHIVE_URL=http://${hostStorageAddress}:9083/archive`,
        `BROWSER_PROFILE_PHASE=${phase}`,
        ...(executionId ? [`BROWSER_TEST_EXECUTION_ID=${executionId}`] : []),
        "node",
        "--input-type=module",
      ],
      {
        input: await readFile(".github/scripts/managed/profiles-client.mjs", "utf8"),
        timeout: 600000,
      },
    ),
  );
};
const recoveryCases = async (phase) => {
  await controllerReady();
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
        `TEST_CONTROLLER_ORIGIN=http://${deployment}.${namespace}.svc:8080`,
        `BROWSER_HOST_ARCHIVE_URL=http://host-storage.${namespace}.svc:8082/archive`,
        `TEST_RECOVERY_PHASE=${phase}`,
        "node",
        "--input-type=module",
      ],
      {
        input: await readFile(".github/scripts/managed/recovery-scenarios.mjs", "utf8"),
        timeout: 600000,
      },
    ),
  );
};

const wait = async (description, predicate, timeout = 90000) => {
  const deadline = Date.now() + timeout;
  while (!(await predicate())) {
    assert(Date.now() < deadline, description);
    await delay(100);
  }
};
const controllerReady = async () =>
  wait(
    "Controller Service was not ready from the client",
    async () => {
      try {
        await kubectl(
          "exec",
          "client",
          "--",
          "node",
          "-e",
          `fetch('http://${deployment}.${namespace}.svc:8080/health',{signal:AbortSignal.timeout(2000)}).then(r=>{if(!r.ok)process.exit(1)},()=>process.exit(1))`,
        );
        return true;
      } catch {
        return false;
      }
    },
    30000,
  );
await mkdir("managed-results", { recursive: true });
try {
  if (!process.env.TEST_K3D_CLUSTER) {
    clusterAttempted = true;
    await run(
      "bash",
      [".github/scripts/managed/k3d-cluster.sh", "create", cluster, kubeconfig],
      { timeout: 360000 },
    );
  }
  const nodes = JSON.parse(await kubectl("get", "nodes", "-o", "json")).items;
  assert(nodes.length >= 3, "A three-node cluster is required");
  const expectedArchitecture = process.arch === "x64" ? "amd64" : process.arch;
  assert(
    nodes.every((node) => node.status.nodeInfo.architecture === expectedArchitecture),
    "Cluster nodes must match the native runner architecture",
  );
  const server = nodes.find(
    (node) =>
      node.metadata.labels["node-role.kubernetes.io/control-plane"] !== undefined,
  ).metadata.name;
  const workerNode = nodes.find((node) => node.metadata.name !== server).metadata.name;
  await run(
    "docker",
    [
      "build",
      "--build-arg",
      `BROWSER_IMAGE=${image}`,
      "-f",
      ".github/scripts/managed/Dockerfile.protocol",
      "-t",
      nextImage,
      ".",
    ],
    { timeout: 300000 },
  );
  for (const candidate of [image, clientImage, nextImage]) {
    await run("k3d", ["image", "import", candidate, "-c", cluster], {
      timeout: 300000,
    });
  }
  stage = "image digest lookup";
  const images = await run("docker", [
    "exec",
    server,
    "ctr",
    "-n",
    "k8s.io",
    "images",
    "ls",
  ]);
  const normalized = image.includes("/") ? image : `docker.io/library/${image}`;
  const row = images.split("\n").find((line) => line.split(/\s+/)[0] === normalized);
  assert(row, "Imported worker image was not found in containerd");
  const digest = row.split(/\s+/)[2];
  assert(/^sha256:[a-f0-9]{64}$/.test(digest));
  const repository = normalized.slice(0, normalized.lastIndexOf(":"));
  const pinned = `${repository}@${digest}`;
  const nextNormalized = `docker.io/library/${nextImage}`;
  const nextRow = images
    .split("\n")
    .find((line) => line.split(/\s+/)[0] === nextNormalized);
  assert(nextRow, "Imported protocol image was not found");
  const nextDigest = nextRow.split(/\s+/)[2];
  assert(/^sha256:[a-f0-9]{64}$/.test(nextDigest));
  const nextRepository = nextNormalized.slice(0, nextNormalized.lastIndexOf(":"));
  const nextPinned = `${nextRepository}@${nextDigest}`;
  extraction = await run("docker", ["create", image]);
  await run("docker", [
    "cp",
    `${extraction}:/opt/browser-sandbox/seccomp.json`,
    join(directory, "browser-sandbox.json"),
  ]);
  await run("docker", ["rm", extraction]);
  extraction = undefined;
  for (const node of nodes) {
    await run("docker", [
      "exec",
      node.metadata.name,
      "mkdir",
      "-p",
      "/var/lib/kubelet/seccomp",
    ]);
    await run("docker", [
      "cp",
      join(directory, "browser-sandbox.json"),
      `${node.metadata.name}:/var/lib/kubelet/seccomp/browser-sandbox.json`,
    ]);
    await run("docker", [
      "exec",
      node.metadata.name,
      "ctr",
      "-n",
      "k8s.io",
      "images",
      "tag",
      "--force",
      normalized,
      pinned,
    ]);
    await run("docker", [
      "exec",
      node.metadata.name,
      "ctr",
      "-n",
      "k8s.io",
      "images",
      "tag",
      "--force",
      nextNormalized,
      nextPinned,
    ]);
  }
  stage = "NetworkPolicy enforcement";
  // Fail before installing the controller if the CNI doesn't enforce policy.
  await run("node", [".github/scripts/managed/kubernetes-network.mjs"], {
    env: { PROBE_IMAGE: pinned },
    timeout: 240000,
  });
  await run("kubectl", ["create", "namespace", namespace]);
  await kubectl(
    "run",
    "host-storage",
    `--image=${clientImage}`,
    "--labels=browser-sandbox-test-storage=true",
    "--restart=Never",
    "--overrides=" +
      JSON.stringify({
        spec: {
          automountServiceAccountToken: false,
          containers: [
            {
              name: "host-storage",
              image: clientImage,
              command: [
                "node",
                "/opt/browser-sandbox/.github/scripts/managed/host-storage.mjs",
              ],
              readinessProbe: { tcpSocket: { port: 9083 }, periodSeconds: 1 },
            },
          ],
        },
      }),
  );
  await kubectl("wait", "--for=condition=Ready", "pod/host-storage", "--timeout=120s");
  hostStorageAddress = JSON.parse(
    await kubectl("get", "pod/host-storage", "-o", "json"),
  ).status.podIP;
  const values = join(directory, "values.json");
  await writeFile(
    values,
    JSON.stringify({
      image: { repository, digest },
      profileStore: { enabled: true },
      capacity: {
        maxWorkers: 2,
        maxPending: 1,
        maxQueued: 1,
        idleRegistrationMs: 30000,
        maxExecutionMs: 180000,
      },
      networkPolicy: {
        storageEgress: [{ address: hostStorageAddress, port: 9083 }],
        apiServerCidrs: [
          "10.43.0.1/32",
          ...nodes.map(
            (node) =>
              `${node.status.addresses.find((address) => address.type === "InternalIP").address}/32`,
          ),
        ],
      },
      runtime: { TIMEOUT: "180000" },
      controller: { nodeSelector: { "kubernetes.io/hostname": server } },
      worker: {
        nodeSelector: { "kubernetes.io/hostname": workerNode },
        terminationGracePeriodSeconds: 15,
      },
    }),
  );
  stage = "Helm install";
  installed = true;
  const installStarted = performance.now();
  await run("helm", [
    "install",
    release,
    "charts/browser-sandbox",
    "-n",
    namespace,
    "--create-namespace",
    "-f",
    values,
    "--wait",
    "--timeout",
    "120s",
    "--set",
    "profileStore.enabled=false",
  ]);
  const controllerStartupMs = performance.now() - installStarted;
  await kubectl(
    "run",
    "client",
    "--labels=browser-sandbox.io/host-control=true",
    `--image=${clientImage}`,
    "--restart=Never",
    "--overrides=" +
      JSON.stringify({
        spec: { automountServiceAccountToken: false, nodeName: server },
      }),
    "--command",
    "--",
    "node",
    "-e",
    "setInterval(()=>{},1000)",
  );
  await kubectl("wait", "--for=condition=Ready", "pod/client", "--timeout=120s");
  const missingStorage = await profileClient("missing-storage");
  assert.equal((await pods()).length, 0);
  await run("helm", [
    "upgrade",
    release,
    "charts/browser-sandbox",
    "-n",
    namespace,
    "-f",
    values,
    "--wait",
    "--timeout",
    "120s",
  ]);
  stage = "persistent Profiles";
  const profileReport = { ...(await profileClient()), ...missingStorage };
  await writeFile(
    "managed-results/profiles-kubernetes.json",
    `${JSON.stringify(profileReport, null, 2)}\n`,
  );
  await profileClient("restart-seed");
  const persistent = await profileClient("lease-hold");
  const leasedPod = (await pods()).find(
    (pod) =>
      pod.metadata.labels["browser-sandbox.io/execution"] === persistent.executionId,
  );
  assert(leasedPod, "Persistent worker was not created");
  await kubectl("rollout", "restart", `deployment/${deployment}`);
  await kubectl("rollout", "status", `deployment/${deployment}`, "--timeout=120s");
  await profileClient("lease-check");
  await profileClient("restart-restore");
  // Test-only cleanup of the deliberately orphaned empty Profile; production recovery is explicit.
  await kubectl(
    "delete",
    "pod",
    leasedPod.metadata.name,
    "--wait=true",
    "--timeout=90s",
  );
  await profileClient("lease-reuse");
  stage = "interrupted Profile upload";
  await profileClient("interrupt-seed");
  const digestCode =
    "const fs=require('node:fs'),crypto=require('node:crypto');const index=JSON.parse(fs.readFileSync('/var/lib/browser-sandbox/profiles/index.json'));process.stdout.write(index.profiles[crypto.createHash('sha256').update('managed-upload-interrupt').digest('hex')].archive.sha256)";
  const confirmedDigest = await kubectl(
    "exec",
    `deployment/${deployment}`,
    "--",
    "node",
    "-e",
    digestCode,
  );
  const interrupted = await profileClient("interrupt-hold");
  const interruptedPod = (await pods()).find(
    (pod) =>
      pod.metadata.labels["browser-sandbox.io/execution"] === interrupted.executionId,
  );
  assert(interruptedPod);
  await kubectl(
    "exec",
    interruptedPod.metadata.name,
    "--",
    "iptables",
    "-w",
    "-I",
    "OUTPUT",
    "-p",
    "tcp",
    "--dport",
    "8083",
    "-m",
    "owner",
    "--uid-owner",
    "0",
    "-m",
    "statistic",
    "--mode",
    "nth",
    "--every",
    "8",
    "--packet",
    "7",
    "-j",
    "REJECT",
    "--reject-with",
    "tcp-reset",
  );
  await profileClient("interrupt-finish", interrupted.executionId);
  assert.equal(
    await kubectl("exec", `deployment/${deployment}`, "--", "node", "-e", digestCode),
    confirmedDigest,
  );
  // Simulate loss of this test-owned failed worker after checking the durable predecessor.
  await kubectl(
    "delete",
    "pod",
    interruptedPod.metadata.name,
    "--grace-period=0",
    "--force",
    "--wait=true",
  );
  await kubectl("rollout", "restart", `deployment/${deployment}`);
  await kubectl("rollout", "status", `deployment/${deployment}`, "--timeout=120s");
  await profileClient("interrupt-restore");
  stage = "persistent Profile PVC retention";
  assert.equal((await pods()).length, 0, "Profile checks left a worker");
  await run(
    "helm",
    ["uninstall", release, "-n", namespace, "--wait", "--timeout", "300s"],
    { timeout: 330000 },
  );
  installed = false;
  const retainedStore = JSON.parse(
    await kubectl("get", "pvc", `${deployment}-profiles`, "-o", "json"),
  );
  assert.equal(retainedStore.metadata.annotations["helm.sh/resource-policy"], "keep");
  const reusedValues = JSON.parse(await readFile(values, "utf8"));
  reusedValues.profileStore.existingClaim = `${deployment}-profiles`;
  await writeFile(values, JSON.stringify(reusedValues));
  installed = true;
  await run("helm", [
    "install",
    release,
    "charts/browser-sandbox",
    "-n",
    namespace,
    "-f",
    values,
    "--wait",
    "--timeout",
    "120s",
  ]);
  await profileClient("restart-restore");
  await writeFile(
    "managed-results/profiles-kubernetes.json",
    JSON.stringify(
      {
        ...profileReport,
        controllerRestartRestored: true,
        recoveredLeaseRejected: true,
        recoveredLeaseReleased: true,
        interruptedUploadPreserved: true,
        helmUninstallRetainedAndRestoredPvc: true,
      },
      null,
      2,
    ) + "\n",
  );
  await controllerReady();
  stage = "recovery scenarios";
  await run("helm", [
    "upgrade",
    release,
    "charts/browser-sandbox",
    "-n",
    namespace,
    "--reuse-values",
    "--set",
    "capacity.idleRegistrationMs=500",
    "--wait",
    "--timeout=120s",
  ]);
  const recoveryValues = JSON.parse(await readFile(values, "utf8"));
  recoveryValues.capacity.idleRegistrationMs = 500;
  await writeFile(values, JSON.stringify(recoveryValues));
  const recoveryReport = [];
  for (const phase of ["core", "ttl-volume", "ttl-host"])
    recoveryReport.push(await recoveryCases(phase));
  await writeFile(
    "managed-results/recovery-kubernetes.json",
    `${JSON.stringify(recoveryReport, null, 2)}\n`,
  );
  stage = "creation failure scenarios";
  cordoned = workerNode;
  await run("kubectl", ["cordon", workerNode]);
  try {
    await client("unschedulable");
    assert.equal((await pods()).length, 0);
  } finally {
    await run("kubectl", ["uncordon", workerNode]);
    cordoned = undefined;
  }
  const namespacePods = JSON.parse(await kubectl("get", "pods", "-o", "json")).items
    .length;
  await kubectl("create", "quota", "recovery-quota", `--hard=pods=${namespacePods}`);
  await wait(
    "Resource quota usage did not settle",
    async () =>
      JSON.parse(await kubectl("get", "quota", "recovery-quota", "-o", "json")).status
        .used?.pods === String(namespacePods),
  );
  try {
    await client("quota");
    assert.equal((await pods()).length, 0);
  } finally {
    await kubectl("delete", "quota", "recovery-quota");
  }
  await run("docker", [
    "exec",
    workerNode,
    "ctr",
    "-n",
    "k8s.io",
    "images",
    "rm",
    pinned,
    normalized,
  ]);
  try {
    await client("image-failure");
    assert.equal((await pods()).length, 0);
  } finally {
    await run("k3d", ["image", "import", image, "-c", cluster], { timeout: 300000 });
    await run("docker", [
      "exec",
      workerNode,
      "ctr",
      "-n",
      "k8s.io",
      "images",
      "tag",
      "--force",
      normalized,
      pinned,
    ]);
  }
  stage = "rollout and drain";
  const held = await client("hold");
  const original = (await pods())[0];
  assert(original?.metadata.uid);
  await kubectl("rollout", "restart", `deployment/${deployment}`);
  await kubectl("rollout", "status", `deployment/${deployment}`, "--timeout=120s");
  assert(
    (await pods()).some(
      (pod) =>
        pod.metadata.uid === original.metadata.uid && pod.status.phase === "Running",
    ),
    "Controller rollout deleted the live worker",
  );
  await client("resume", held.executionId);
  const rule = [
    "INPUT",
    "-p",
    "tcp",
    "--dport",
    "8081",
    "-j",
    "REJECT",
    "--reject-with",
    "tcp-reset",
  ];
  await kubectl("exec", original.metadata.name, "--", "iptables", "-I", ...rule);
  try {
    await client("unknown", held.executionId);
    assert((await pods()).some((pod) => pod.metadata.uid === original.metadata.uid));
  } finally {
    await kubectl("exec", original.metadata.name, "--", "iptables", "-D", ...rule);
  }
  await run("helm", [
    "upgrade",
    release,
    "charts/browser-sandbox",
    "-n",
    namespace,
    "-f",
    values,
    "--set",
    `image.repository=${nextRepository}`,
    "--set",
    `image.digest=${nextDigest}`,
    "--wait",
    "--timeout",
    "120s",
  ]);
  await client("incompatible", held.executionId);
  assert((await pods()).some((pod) => pod.metadata.uid === original.metadata.uid));
  await run("helm", [
    "upgrade",
    release,
    "charts/browser-sandbox",
    "-n",
    namespace,
    "-f",
    values,
    "--wait",
    "--timeout",
    "120s",
  ]);
  await client("resume", held.executionId);
  // Optional PDB must make ordinary drain wait without deleting workers.
  await run("helm", [
    "upgrade",
    release,
    "charts/browser-sandbox",
    "-n",
    namespace,
    "-f",
    values,
    "--set",
    "worker.disruptionBudget.enabled=true",
    "--wait",
  ]);
  await wait("Worker PDB status was not observed", async () => {
    const budget = JSON.parse(
      await kubectl("get", "pdb", `${deployment}-workers`, "-o", "json"),
    );
    return (
      budget.status?.observedGeneration === budget.metadata.generation &&
      budget.status.currentHealthy >= 1 &&
      budget.status.desiredHealthy === 1
    );
  });
  cordoned = workerNode;
  await assert.rejects(
    run(
      "kubectl",
      [
        "drain",
        workerNode,
        "--ignore-daemonsets",
        "--delete-emptydir-data",
        "--timeout=8s",
      ],
      { timeout: 20000 },
    ),
    (error) => /disruption budget/i.test(`${error.output}\n${error.errors}`),
  );
  assert(
    (await pods()).some(
      (pod) =>
        pod.metadata.uid === original.metadata.uid && !pod.metadata.deletionTimestamp,
    ),
  );
  await run("kubectl", ["uncordon", workerNode]);
  cordoned = undefined;
  await run("helm", [
    "upgrade",
    release,
    "charts/browser-sandbox",
    "-n",
    namespace,
    "-f",
    values,
    "--wait",
  ]);
  const losing = await client("hold");
  assert.equal((await client("status", losing.executionId)).state, "retained");
  cordoned = workerNode;
  await run(
    "kubectl",
    [
      "drain",
      workerNode,
      "--ignore-daemonsets",
      "--delete-emptydir-data",
      "--timeout=60s",
    ],
    { timeout: 80000 },
  );
  const loss = await client("lost", losing.executionId);
  assert(loss.workerLossReported);
  assert.equal((await pods()).length, 0, "Drain silently replaced a lost worker");
  await client("finish", held.executionId);
  await client("finish", losing.executionId);
  await kubectl("rollout", "restart", `deployment/${deployment}`);
  await kubectl("rollout", "status", `deployment/${deployment}`, "--timeout=120s");
  await client("replay", held.executionId);
  await client("replay", losing.executionId);
  await run("kubectl", ["uncordon", workerNode]);
  cordoned = undefined;
  const deadlineHeld = await client("hold");
  const deadlinePod = (await pods()).find(
    (pod) =>
      pod.metadata.labels["browser-sandbox.io/execution"] === deadlineHeld.executionId,
  );
  assert(deadlinePod?.spec.activeDeadlineSeconds);
  await kubectl("scale", `deployment/${deployment}`, "--replicas=0");
  await wait(
    "Controller did not stop",
    async () =>
      JSON.parse(
        await kubectl(
          "get",
          "pods",
          "-l",
          "app.kubernetes.io/component=controller",
          "-o",
          "json",
        ),
      ).items.length === 0,
    120000,
  );
  await wait(
    "Pod lifetime backstop did not fire",
    async () => {
      const pod = JSON.parse(
        await kubectl("get", "pod", deadlinePod.metadata.name, "-o", "json"),
      );
      return pod.status.phase === "Failed" && pod.status.reason === "DeadlineExceeded";
    },
    (deadlinePod.spec.activeDeadlineSeconds + 60) * 1000,
  );
  await kubectl("scale", `deployment/${deployment}`, "--replicas=1");
  await kubectl("rollout", "status", `deployment/${deployment}`, "--timeout=120s");
  await client("lost", deadlineHeld.executionId);
  await client("finish", deadlineHeld.executionId);
  stage = "functional client";
  const functional = await client();
  assert.equal(functional.architecture, process.arch, "Client image must match the native runner architecture");
  assert.equal((await pods()).length, 0, "Temporary worker was not removed");
  stage = "safe uninstall";
  await client("hold");
  const finalWorker = (await pods())[0];
  heldFinalizer = finalWorker.metadata.name;
  await kubectl(
    "patch",
    "pod",
    heldFinalizer,
    "--type=merge",
    "-p",
    JSON.stringify({ metadata: { finalizers: ["browser-sandbox.io/test-hold"] } }),
  );
  const uninstalling = run(
    "helm",
    ["uninstall", release, "-n", namespace, "--wait", "--timeout", "300s"],
    { timeout: 330000 },
  );
  uninstalling.catch(() => {});
  await wait("Uninstall did not begin worker deletion", async () =>
    (await pods()).some(
      (pod) => pod.metadata.name === heldFinalizer && pod.metadata.deletionTimestamp,
    ),
  );
  assert(
    JSON.parse(
      await kubectl("get", "networkpolicy", `${deployment}-workers`, "-o", "json"),
    ).spec.ingress.length > 0,
    "Policy disappeared before worker deletion was confirmed",
  );
  await kubectl(
    "patch",
    "pod",
    heldFinalizer,
    "--type=merge",
    "-p",
    JSON.stringify({ metadata: { finalizers: [] } }),
  );
  heldFinalizer = undefined;
  await uninstalling;
  installed = false;
  const retainedPvc = JSON.parse(
    await kubectl("get", "pvc", `${deployment}-profiles`, "-o", "json"),
  );
  assert.equal(retainedPvc.metadata.annotations["helm.sh/resource-policy"], "keep");
  await wait(
    "Helm uninstall left worker Pods",
    async () => (await pods()).length === 0,
  );
  const report = {
    controllerStartupMs,
    ...functional,
    rolloutPreservedWorker: true,
    rolloutRecoveredOriginalPage: true,
    finishedIdsFencedAcrossRestart: true,
    incompatibleProtocolPreservedWorker: true,
    failedStatusPreservedWorker: true,
    controllerAbsentDeadlineEnforced: true,
    unschedulableQuotaAndPullFailuresExplicit: true,
    drainWithoutForce: true,
    pdbBlockedDrain: true,
    uninstallRemovedWorkers: true,
    uninstallRetainedProfileStore: true,
    heldArchitecture: held.architecture,
  };
  await writeFile(
    "managed-results/kubernetes.json",
    `${JSON.stringify(report, null, 2)}\n`,
  );
  console.log(JSON.stringify(report));
} catch (error) {
  // Logs may contain capability URLs. Retain only known diagnostic codes and Pod state.
  const list = await kubectl("get", "pods", "-o", "json")
    .then(JSON.parse)
    .catch(() => ({ items: [] }));
  const diagnostics = [];
  for (const pod of list.items) {
    const logs = await kubectl("logs", pod.metadata.name, "--tail=100").catch(() => "");
    diagnostics.push({
      role: pod.metadata.labels?.["app.kubernetes.io/component"] ?? "client",
      phase: pod.status.phase,
      waiting: pod.status.containerStatuses?.map(
        (container) => container.state.waiting?.reason,
      ),
      codes: [
        ...new Set(
          logs.match(/\bbrowser_[a-z_]+\b|\b(?:EPERM|ENOENT|EACCES|ENOSPC)\b/g) ?? [],
        ),
      ],
    });
  }
  await writeFile(
    "managed-results/kubernetes-diagnostics.json",
    JSON.stringify(diagnostics, null, 2),
  );
  // Error messages from browser tools may contain URLs; emit only infrastructure diagnostics.
  console.error(
    JSON.stringify({
      kubernetesFailed: true,
      stage,
      diagnostic: String(error.actual?.message || error.errors || error.message)
        .replace(/(?:https?|wss?):\/\/[^\s]+/g, "<url>")
        .replace(/[a-f0-9]{64}/g, "<digest>")
        .slice(-2000),
      codes: [...new Set(String(error).match(/\bbrowser_[a-z_]+\b/g) ?? [])],
      diagnostics,
    }),
  );
  process.exitCode = 1;
} finally {
  if (heldFinalizer)
    await kubectl(
      "patch",
      "pod",
      heldFinalizer,
      "--type=merge",
      "-p",
      JSON.stringify({ metadata: { finalizers: [] } }),
    ).catch(() => {});
  await run("docker", ["image", "rm", nextImage]).catch(() => {});
  if (cordoned) await run("kubectl", ["uncordon", cordoned]).catch(() => {});
  if (installed)
    await run("helm", [
      "uninstall",
      release,
      "-n",
      namespace,
      "--wait",
      "--timeout",
      "90s",
    ]).catch(() => {});
  await kubectl(
    "delete",
    "namespace",
    namespace,
    "--ignore-not-found",
    "--wait=false",
  ).catch(() => {});
  if (extraction) await run("docker", ["rm", extraction]).catch(() => {});
  if (clusterAttempted)
    await run("bash", [
      ".github/scripts/managed/k3d-cluster.sh",
      "delete",
      cluster,
      kubeconfig,
    ]).catch(() => {});
  await rm(directory, { recursive: true, force: true });
}
