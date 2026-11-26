import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { activeActive } from "./active-active.mjs";
import { verifyJournalRollback } from "./journal-rollback.mjs";

const suite = process.argv[2] ?? "all";
assert(
  [
    "all",
    "fresh-node",
    "single-controller",
    "journal",
    "active-active",
    "probe-ab",
  ].includes(suite),
  `Unknown Kubernetes suite: ${suite}`,
);
const probeAB = suite === "probe-ab";
const runFreshNode = suite === "all" || suite === "fresh-node" || probeAB;
const runSingleController = suite === "all" || suite === "single-controller";

const directory = await mkdtemp(join(tmpdir(), "browser-kubernetes-"));
const suffix = randomUUID().slice(0, 8);
const cluster = process.env.TEST_K3D_CLUSTER ?? `browser-${suffix}`;
const namespace = `browser-${suffix}`;
const release = "trial";
const deployment = "trial-browser";
const image = process.env.BROWSER_IMAGE ?? "browser-sandbox:managed";
const probeImages = ["probe-startup", "probe-failure"].map(
  (variant) => `browser-sandbox:${variant}-${suffix}`,
);
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
        `TEST_CONTROL_ORIGIN=http://${deployment}-host.${namespace}.svc:8082`,
        `BROWSER_HOST_ARCHIVE_URL=http://${hostStorageAddress}:9083/archive`,
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
    "Controller agent and host services were not ready from the client",
    async () => {
      try {
        await kubectl(
          "exec",
          "client",
          "--",
          "node",
          "-e",
          `Promise.all([fetch('http://${deployment}.${namespace}.svc:8080/health',{signal:AbortSignal.timeout(2000)}),fetch('http://${deployment}-host.${namespace}.svc:8082/health',{signal:AbortSignal.timeout(2000)})]).then(([agent,host])=>{if(!agent.ok||host.status!==404)process.exit(1)},()=>process.exit(1))`,
        );
        return true;
      } catch {
        return false;
      }
    },
    30000,
  );
const jsonLogRecords = (output) =>
  output.split("\n").flatMap((line) => {
    const space = line.indexOf(" ");
    try {
      const record = JSON.parse(line.slice(space + 1));
      return record && typeof record === "object"
        ? [{ ...record, time: Date.parse(line.slice(0, space)) }]
        : [];
    } catch {
      return [];
    }
  });

const workerLifecycle = async (pod, startupProbe) => {
  const records = jsonLogRecords(
    await kubectl("logs", pod.metadata.name, "-c", "worker", "--timestamps"),
  );
  const events = [
    "worker_starting",
    ...(startupProbe ? ["worker_probe_succeeded"] : []),
    "worker_ready",
    "browser_launch_succeeded",
  ];
  const lifecycle = records.filter(
    ({ event }) => event === "worker_probe_succeeded" || events.includes(event),
  );
  assert.deepEqual(
    lifecycle.map(({ event }) => event),
    events,
  );
  return Object.fromEntries(lifecycle.map((record) => [record.event, record]));
};

const benchmarkColdStart = async (node, startupProbe = false, count = 10) => {
  const samples = [];
  for (let index = 0; index < count; index++) {
    const executionId = randomUUID();
    try {
      const clientTiming = await client("cold-start", executionId);
      const worker = (await pods()).find(
        (pod) => pod.metadata.labels["browser-sandbox.io/execution"] === executionId,
      );
      assert.equal(worker?.spec.nodeName, node.metadata.name);
      const logs = await workerLifecycle(worker, startupProbe);
      const controllerReady = jsonLogRecords(
        await kubectl(
          "logs",
          `deployment/${deployment}`,
          "-c",
          "controller",
          "--timestamps",
          "--since=2m",
        ),
      ).find((record) => record.type === "ready" && record.executionId === executionId);
      assert(controllerReady, "Controller did not report worker readiness");
      const events = JSON.parse(
        await kubectl(
          "get",
          "events",
          "--field-selector",
          `involvedObject.uid=${worker.metadata.uid}`,
          "-o",
          "json",
        ),
      ).items;
      const pulling = events.find((event) => event.reason === "Pulling");
      const pulled = events.find((event) => event.reason === "Pulled");
      const eventTime = (event) => Date.parse(event.firstTimestamp ?? event.eventTime);
      if (pulling) assert(pulled, "Image pull did not report completion");
      const created = Date.parse(worker.metadata.creationTimestamp);
      const started = Date.parse(
        worker.status.containerStatuses.find(({ name }) => name === "worker").state
          .running.startedAt,
      );
      const ready = Date.parse(
        worker.status.conditions.find(
          ({ type, status }) => type === "Ready" && status === "True",
        ).lastTransitionTime,
      );
      const probe = logs.worker_probe_succeeded;
      const launch = logs.browser_launch_succeeded;
      const sample = {
        ...clientTiming,
        registrationToFirstCdpMs: clientTiming.registrationMs + clientTiming.coldCdpMs,
        podToContainerMs: started - created,
        containerToPodReadyMs: ready - started,
        workerStartupMs: logs.worker_ready.durationMs,
        probeMs: startupProbe ? probe.durationMs : launch.executionRendererProbeMs,
        startupRendererProbeMs: startupProbe ? probe.rendererProbeMs : 0,
        probeCleanupMs: startupProbe ? probe.probeCleanupMs : null,
        browserlessAdmissionMs: launch.browserlessAdmissionMs,
        chromeStartupMs: launch.chromeStartupMs,
        browserHookSetupMs: launch.browserHookSetupMs,
        executionRendererProbeMs: launch.executionRendererProbeMs,
        browserlessHandshakeMs: launch.browserlessHandshakeMs,
        controllerReadyMs: controllerReady.elapsedMs,
        runtimeReadyToControllerReadyMs: controllerReady.time - logs.worker_ready.time,
        preparationMs: launch.preparationMs,
        profileAcquireMs: launch.profileAcquireMs,
        profileRestoreMs: launch.profileRestoreMs,
        browserLaunchMs: launch.browserLaunchMs,
        imagePullMs: pulling ? eventTime(pulled) - eventTime(pulling) : null,
      };
      for (const value of Object.values(sample))
        assert(value === null || Number.isFinite(value));
      samples.push(sample);
    } finally {
      await client("finish", executionId);
      assert(
        !(await pods()).some(
          (pod) => pod.metadata.labels["browser-sandbox.io/execution"] === executionId,
        ),
        "Benchmark worker survived finalization",
      );
    }
  }
  const summarize = (group) =>
    Object.fromEntries(
      Object.keys(group[0]).map((phase) => {
        const values = group
          .map((sample) => sample[phase])
          .filter((value) => value !== null)
          .sort((a, b) => a - b);
        if (!values.length) return [phase, { samples: 0 }];
        const middle = Math.floor(values.length / 2);
        return [
          phase,
          {
            median:
              values.length % 2
                ? values[middle]
                : (values[middle - 1] + values[middle]) / 2,
            p95: values[Math.ceil(values.length * 0.95) - 1],
            samples: values.length,
          },
        ];
      }),
    );
  const cached = samples.filter((sample) => sample.imagePullMs === null);
  const pulled = samples.filter((sample) => sample.imagePullMs !== null);
  return {
    node: node.status.nodeInfo,
    allocatable: node.status.allocatable,
    samples,
    ...(cached.length ? { cached: summarize(cached) } : {}),
    ...(pulled.length ? { pulled: summarize(pulled) } : {}),
  };
};

const writeReport = async (report) => {
  await writeFile(
    "managed-results/kubernetes.json",
    `${JSON.stringify(report, null, 2)}\n`,
  );
  console.log(JSON.stringify(report));
};

const runSuite = async () => {
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
  if (runSingleController)
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
  const candidates = [image];
  if (suite !== "journal") candidates.push(clientImage);
  if (runSingleController) candidates.push(nextImage);
  if (probeAB) {
    for (const [index, target] of ["startup", "failure"].entries()) {
      await run(
        "docker",
        [
          "build",
          "--build-arg",
          `BROWSER_IMAGE=${image}`,
          "--target",
          target,
          "-f",
          ".github/scripts/managed/Dockerfile.probe",
          "-t",
          probeImages[index],
          ".",
        ],
        { timeout: 300000 },
      );
      candidates.push(probeImages[index]);
    }
  }
  await run("k3d", ["image", "import", ...candidates, "-c", cluster], {
    timeout: 300000,
  });
  for (const candidate of candidates) {
    // Docker's containerd image store can export only the config digest as a reference.
    const candidateName = candidate.includes("/")
      ? candidate
      : `docker.io/library/${candidate}`;
    const imported = await run("docker", [
      "exec",
      server,
      "ctr",
      "-n",
      "k8s.io",
      "images",
      "ls",
    ]);
    if (!imported.split("\n").some((line) => line.split(/\s+/)[0] === candidateName)) {
      const configDigest = await run("docker", [
        "image",
        "inspect",
        "--format",
        "{{.Id}}",
        candidate,
      ]);
      assert(
        imported.split("\n").some((line) => line.split(/\s+/)[0] === configDigest),
        "Imported image lost its name and config digest",
      );
      for (const node of nodes)
        await run("docker", [
          "exec",
          node.metadata.name,
          "ctr",
          "-n",
          "k8s.io",
          "images",
          "tag",
          "--force",
          configDigest,
          candidateName,
        ]);
    }
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
  const reference = (tag) => {
    const name = tag.includes("/") ? tag : `docker.io/library/${tag}`;
    const row = images.split("\n").find((line) => line.split(/\s+/)[0] === name);
    const digest = row?.split(/\s+/)[2];
    assert(
      digest && /^sha256:[a-f0-9]{64}$/.test(digest),
      "Invalid imported image digest",
    );
    const repository = name.slice(0, name.lastIndexOf(":"));
    return { name, repository, digest, pinned: `${repository}@${digest}` };
  };
  const primary = reference(image);
  const { name: normalized, repository, digest, pinned } = primary;
  const next = runSingleController ? reference(nextImage) : undefined;
  const nextDigest = next?.digest;
  const nextRepository = next?.repository;
  const variants = probeAB ? probeImages.map(reference) : [];
  for (const node of nodes) {
    for (const ref of [primary, ...(next ? [next] : []), ...variants])
      await run("docker", [
        "exec",
        node.metadata.name,
        "ctr",
        "-n",
        "k8s.io",
        "images",
        "tag",
        "--force",
        ref.name,
        ref.pinned,
      ]);
  }
  if (runFreshNode) {
    stage = "NetworkPolicy enforcement";
    // Fail before installing the controller if the CNI doesn't enforce policy.
    await run("node", [".github/scripts/managed/kubernetes-network.mjs"], {
      env: { PROBE_IMAGE: pinned },
      timeout: 240000,
    });
  }
  await run("kubectl", ["create", "namespace", namespace]);
  if (runSingleController || suite === "active-active") {
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
            nodeName: server,
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
    await kubectl(
      "wait",
      "--for=condition=Ready",
      "pod/host-storage",
      "--timeout=120s",
    );
    const hostStoragePod = JSON.parse(
      await kubectl("get", "pod/host-storage", "-o", "json"),
    );
    assert.notEqual(
      hostStoragePod.spec.nodeName,
      workerNode,
      "Host archive fixture must remain off the drain target",
    );
    hostStorageAddress = hostStoragePod.status.podIP;
  }
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
        storageEgress: hostStorageAddress
          ? [{ address: hostStorageAddress, port: 9083 }]
          : [],
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
  const checkJournal = async () => {
    stage = "Helm journal claim upgrade and rollback";
    await verifyJournalRollback({ run, kubectl, namespace, directory, values });
  };
  if (suite === "journal") {
    await checkJournal();
    await writeReport({ helmJournalUpgradeAndRollbackPreservedPvc: true });
    return;
  }

  stage = "client setup";
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
  const freshNodeResults = {
    singleSharedStateControllerRecoveredWithoutJournalPvc: true,
    unconfinedWorker: {
      containerRuntimeVersion: nodes.find((node) => node.metadata.name === workerNode)
        .status.nodeInfo.containerRuntimeVersion,
      checkedWithoutNodeProfile: clusterAttempted,
    },
  };
  if (runFreshNode) {
    stage = "single shared-state controller and Unconfined worker";
    if (clusterAttempted)
      await run("docker", [
        "exec",
        workerNode,
        "test",
        "!",
        "-e",
        "/var/lib/kubelet/seccomp/browser-sandbox.json",
      ]);
    installed = true;
    await run("helm", [
      "install",
      release,
      "charts/browser-sandbox",
      "-n",
      namespace,
      "-f",
      values,
      "--set",
      "controller.sharedState=true,controllerData.skipCreation=true,profileStore.enabled=false",
      "--set",
      "worker.securityContext.seccompProfile.type=Unconfined",
      "--wait",
      "--timeout",
      "120s",
    ]);
    assert.equal(await kubectl("get", "pvc", "-o", "name"), "");
    await run("helm", [
      "upgrade",
      release,
      "charts/browser-sandbox",
      "-n",
      namespace,
      "--reuse-values",
      "--dry-run=server",
    ]);
    const shared = await client("hold");
    const [sharedWorker] = await pods();
    assert.equal(sharedWorker?.spec.nodeName, workerNode);
    assert.equal(sharedWorker.spec.automountServiceAccountToken, false);
    const { securityContext } = sharedWorker.spec.containers[0];
    assert.deepEqual(securityContext.seccompProfile, { type: "Unconfined" });
    assert.equal(securityContext.privileged, false);
    assert.equal(securityContext.allowPrivilegeEscalation, false);
    const workerStatus = await kubectl(
      "exec",
      sharedWorker.metadata.name,
      "--",
      "cat",
      "/proc/1/status",
    );
    assert.match(workerStatus, /^NoNewPrivs:\s+1$/m);
    assert.match(workerStatus, /^Seccomp:\s+0$/m);
    await kubectl("rollout", "restart", `deployment/${deployment}`);
    await kubectl("rollout", "status", `deployment/${deployment}`, "--timeout=120s");
    assert.equal((await pods())[0]?.metadata.uid, sharedWorker.metadata.uid);
    assert.deepEqual(await client("resume", shared.executionId), {
      samePage: true,
      sameGeneration: true,
    });
    await client("finish", shared.executionId);
    stage = "managed worker cold-start benchmark";
    const benchmarkNode = nodes.find((node) => node.metadata.name === workerNode);
    if (probeAB) {
      const selectImage = async (ref) => {
        await run("helm", [
          "upgrade",
          release,
          "charts/browser-sandbox",
          "-n",
          namespace,
          "--reuse-values",
          "--set-string",
          `image.repository=${ref.repository},image.digest=${ref.digest}`,
          "--wait",
          "--timeout",
          "120s",
        ]);
        await controllerReady();
      };
      const reports = [];
      for (const startup of [true, false, false, true]) {
        stage = `managed worker A/B ${startup ? "startup" : "execution"}`;
        await selectImage(startup ? variants[0] : { repository, digest });
        const report = await benchmarkColdStart(benchmarkNode, startup, 5);
        assert.equal(
          report.cached?.registrationToFirstCdpMs.samples,
          5,
          "Image must be cached",
        );
        reports.push({ probe: startup ? "startup" : "execution", ...report });
      }
      freshNodeResults.probeAB = reports;
      stage = "execution renderer failure blocks external CDP";
      await selectImage(variants[1]);
      const executionId = randomUUID();
      try {
        assert.deepEqual(await client("probe-failure", executionId), {
          rejected: true,
        });
        const worker = (await pods()).find(
          (pod) => pod.metadata.labels["browser-sandbox.io/execution"] === executionId,
        );
        assert(worker, "Probe failure did not reach a worker");
        const events = jsonLogRecords(
          await kubectl("logs", worker.metadata.name, "-c", "worker", "--timestamps"),
        ).map(({ event }) => event);
        assert(events.includes("browser_renderer_probe_failed"));
        assert(!events.includes("browser_launch_succeeded"));
        freshNodeResults.probeFailureBlockedCdp = true;
      } finally {
        await client("finish", executionId);
      }
    } else {
      freshNodeResults.coldStart = await benchmarkColdStart(benchmarkNode);
    }
    await run("helm", [
      "uninstall",
      release,
      "-n",
      namespace,
      "--wait",
      "--timeout",
      "120s",
    ]);
    installed = false;
    if (suite === "fresh-node" || probeAB) {
      await writeReport(freshNodeResults);
      return;
    }
  }

  stage = "provision Localhost seccomp profile";
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
  }

  const checkActiveActive = async () => {
    stage = "three-replica active-active";
    return activeActive({ run, kubectl, namespace, directory, values, clientImage });
  };
  if (suite === "active-active") {
    await writeReport({ activeActive: await checkActiveActive() });
    return;
  }

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
  const retainedJournal = JSON.parse(
    await kubectl("get", "pvc", `${deployment}-data`, "-o", "json"),
  );
  assert.equal(retainedJournal.metadata.annotations["helm.sh/resource-policy"], "keep");
  const reusedValues = JSON.parse(await readFile(values, "utf8"));
  reusedValues.controllerData = { existingClaim: `${deployment}-data` };
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
  await run("helm", [
    "upgrade",
    release,
    "charts/browser-sandbox",
    "-n",
    namespace,
    "-f",
    values,
    "--set",
    "capacity.maxExecutionMs=180000",
    "--wait",
  ]);
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
  assert.equal(
    functional.architecture,
    process.arch,
    "Client image must match the native runner architecture",
  );
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
  const finalJournal = JSON.parse(
    await kubectl("get", "pvc", `${deployment}-data`, "-o", "json"),
  );
  assert.equal(finalJournal.metadata.uid, retainedJournal.metadata.uid);
  assert.equal(finalJournal.metadata.annotations["helm.sh/resource-policy"], "keep");
  await wait(
    "Helm uninstall left worker Pods",
    async () => (await pods()).length === 0,
  );
  const additionalResults = {};
  if (suite === "all") {
    await checkJournal();
    additionalResults.helmJournalUpgradeAndRollbackPreservedPvc = true;
    additionalResults.activeActive = await checkActiveActive();
    Object.assign(additionalResults, freshNodeResults);
  }
  await writeReport({
    ...additionalResults,
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
  });
};

await mkdir("managed-results", { recursive: true });
try {
  await runSuite();
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
  for (const fixture of [
    ...(runSingleController ? [nextImage] : []),
    ...(probeAB ? probeImages : []),
  ])
    await run("docker", ["image", "rm", fixture]).catch(() => {});
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
