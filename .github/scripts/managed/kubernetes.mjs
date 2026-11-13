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
const clientImage =
  process.env.BROWSER_CLIENT_IMAGE ?? "browser-sandbox-client:managed";
const kubeconfig = process.env.TEST_K3D_CLUSTER
  ? process.env.KUBECONFIG
  : join(directory, "kubeconfig");
assert(kubeconfig, "An isolated KUBECONFIG is required for an existing test cluster");
const environment = { ...process.env, KUBECONFIG: kubeconfig };
let stage = "cluster setup";
let installed = false;
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
const client = async (action = "functional", executionId) =>
  JSON.parse(
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
const wait = async (description, predicate, timeout = 90000) => {
  const deadline = Date.now() + timeout;
  while (!(await predicate())) {
    assert(Date.now() < deadline, description);
    await delay(100);
  }
};
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
  const server = nodes.find(
    (node) =>
      node.metadata.labels["node-role.kubernetes.io/control-plane"] !== undefined,
  ).metadata.name;
  const workerNode = nodes.find((node) => node.metadata.name !== server).metadata.name;
  for (const candidate of [image, clientImage]) {
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
  }
  stage = "NetworkPolicy enforcement";
  // Fail before installing the controller if the CNI doesn't enforce policy.
  await run("node", [".github/scripts/managed/kubernetes-network.mjs"], {
    env: { PROBE_IMAGE: pinned },
    timeout: 240000,
  });
  const values = join(directory, "values.json");
  await writeFile(
    values,
    JSON.stringify({
      image: { repository, digest },
      networkPolicy: {
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
  ]);
  const controllerStartupMs = performance.now() - installStarted;
  await kubectl(
    "run",
    "client",
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
  stage = "functional client";
  const functional = await client();
  assert.equal((await pods()).length, 0, "Temporary worker was not removed");
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
    /disruption budget/i,
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
  await run("kubectl", ["uncordon", workerNode]);
  cordoned = undefined;
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
  await wait(
    "Helm uninstall left worker Pods",
    async () => (await pods()).length === 0,
  );
  const report = {
    controllerStartupMs,
    ...functional,
    rolloutPreservedWorker: true,
    drainWithoutForce: true,
    pdbBlockedDrain: true,
    uninstallRemovedWorkers: true,
    heldArchitecture: held.architecture,
  };
  await writeFile(
    "managed-results/kubernetes.json",
    JSON.stringify(report, null, 2) + "\n",
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
      diagnostic: String(error.message)
        .split("\n")[0]
        .replace(/(?:https?|wss?):\/\/[^\s]+/g, "<url>")
        .replace(/[a-f0-9]{64}/g, "<digest>")
        .slice(0, 500),
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
