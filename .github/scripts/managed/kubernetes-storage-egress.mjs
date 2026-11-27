import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";

const kubeconfig = process.env.KUBECONFIG;
const image = process.env.PROBE_IMAGE;
const hostname = process.env.PROBE_STORAGE_HOST;
const allowedPort = Number(process.env.PROBE_STORAGE_PORT ?? 443);
const otherPort = Number(process.env.PROBE_STORAGE_OTHER_PORT ?? 80);
assert(
  kubeconfig && image && hostname,
  "Set KUBECONFIG, PROBE_IMAGE and PROBE_STORAGE_HOST",
);
assert(
  /^[a-z0-9][a-z0-9.-]*[a-z0-9]$/.test(hostname) && hostname.includes("."),
  "PROBE_STORAGE_HOST must be an exact DNS name",
);
assert(
  allowedPort !== otherPort &&
    [allowedPort, otherPort].every(
      (port) => Number.isInteger(port) && port > 0 && port < 65536,
    ),
  "Supply two distinct TCP ports",
);

const namespace = `browser-storage-egress-${randomUUID().slice(0, 8)}`;
const labels = {
  "app.kubernetes.io/name": "browser-sandbox",
  "app.kubernetes.io/instance": "probe",
  "app.kubernetes.io/component": "worker",
};
const kubectl = (args, input) =>
  execFileSync(
    "kubectl",
    ["--kubeconfig", kubeconfig, "--request-timeout=15s", "-n", namespace, ...args],
    { encoding: "utf8", input, timeout: 120000 },
  );
const apply = (manifest) =>
  kubectl(
    ["apply", "-f", "-"],
    typeof manifest === "string" ? manifest : JSON.stringify(manifest),
  );
function render(apiAddress, storageEgressDomains = []) {
  return execFileSync(
    "helm",
    [
      "template",
      "probe",
      fileURLToPath(new URL("../../../charts/browser-sandbox", import.meta.url)),
      "--namespace",
      namespace,
      "--show-only",
      "templates/networkpolicy.yaml",
      "--set-string",
      `image.digest=sha256:${"a".repeat(64)}`,
      "--set",
      `networkPolicy.apiServerCidrs[0]=${apiAddress}/32`,
      "--set-json",
      `networkPolicy.storageEgressDomains=${JSON.stringify(storageEgressDomains)}`,
    ],
    { encoding: "utf8", timeout: 15000 },
  );
}

function startPod(name, container, volumes = []) {
  apply({
    apiVersion: "v1",
    kind: "Pod",
    metadata: { name, labels },
    spec: {
      automountServiceAccountToken: false,
      restartPolicy: "Never",
      activeDeadlineSeconds: 240,
      terminationGracePeriodSeconds: 5,
      containers: [{ name, image, ...container }],
      volumes,
    },
  });
  kubectl(["wait", "--for=condition=Ready", `pod/${name}`, "--timeout=90s"]);
}

const connect = `const net=require("node:net");
const [host,port]=process.argv.slice(1);
const socket=net.connect({host,port:Number(port)});
socket.setTimeout(1200);
const report=(result)=>{console.log(result);socket.destroy()};
socket.once("connect",()=>report("connected"));
socket.once("error",(error)=>report(error.code));
socket.once("timeout",()=>report("timeout"));`;
async function expectConnection(pod, port, expected, phase, uid = 0) {
  const source =
    uid === 0 ? connect : `process.setgid(${uid});process.setuid(${uid});${connect}`;
  const deadline = Date.now() + 20000;
  for (;;) {
    const result = kubectl([
      "exec",
      pod,
      "--",
      "node",
      "-e",
      source,
      hostname,
      String(port),
    ]).trim();
    assert(
      [
        "connected",
        "timeout",
        "ECONNREFUSED",
        "EHOSTUNREACH",
        "ENETUNREACH",
        "ETIMEDOUT",
      ].includes(result),
      `${phase}: unexpected TCP probe result ${result}`,
    );
    if ((result === "connected") === expected) return;
    assert(
      Date.now() < deadline,
      `${phase}: expected ${hostname}:${port} as UID ${uid} to be ${expected ? "reachable" : "blocked"}, got ${result}`,
    );
    await delay(250);
  }
}

let created = false;
try {
  kubectl(["create", "namespace", namespace]);
  created = true;
  const api = JSON.parse(
    kubectl(["get", "service", "kubernetes", "-n", "default", "-o", "json"]),
  ).spec.clusterIP;
  startPod("policy-probe", {
    command: ["node", "-e", "setInterval(() => {}, 1000)"],
    securityContext: {
      allowPrivilegeEscalation: false,
      capabilities: { drop: ["ALL"] },
    },
  });

  await expectConnection("policy-probe", allowedPort, true, "unrestricted fixture");
  await expectConnection("policy-probe", otherPort, true, "unrestricted fixture");
  apply(render(api));
  await expectConnection(
    "policy-probe",
    allowedPort,
    false,
    "Kubernetes NetworkPolicy negative control",
  );
  await expectConnection(
    "policy-probe",
    otherPort,
    false,
    "Kubernetes NetworkPolicy negative control",
  );

  const storage = [{ hostname, port: allowedPort }];
  apply(render(api, storage));
  await expectConnection("policy-probe", allowedPort, true, "Cilium storage hostname");
  await expectConnection("policy-probe", otherPort, false, "Cilium unlisted port");

  startPod(
    "runtime-worker",
    {
      args: ["worker"],
      env: [
        { name: "BROWSER_EXECUTION_ID", value: randomUUID() },
        {
          name: "BROWSER_POD_IP",
          valueFrom: { fieldRef: { fieldPath: "status.podIP" } },
        },
        { name: "BROWSER_WORKER_ORIGIN", value: "http://$(BROWSER_POD_IP):8081" },
        { name: "BROWSER_STORAGE_EGRESS", value: JSON.stringify(storage) },
      ],
      readinessProbe: { httpGet: { path: "/health", port: 8081 }, periodSeconds: 1 },
      securityContext: {
        runAsUser: 0,
        allowPrivilegeEscalation: false,
        seccompProfile: { type: "Unconfined" },
        capabilities: {
          drop: ["ALL"],
          add: [
            "SETUID",
            "SETGID",
            "CHOWN",
            "KILL",
            "DAC_OVERRIDE",
            "SYS_CHROOT",
            "NET_ADMIN",
          ],
        },
      },
      volumeMounts: [{ name: "shm", mountPath: "/dev/shm" }],
    },
    [{ name: "shm", emptyDir: { medium: "Memory", sizeLimit: "512Mi" } }],
  );
  await expectConnection(
    "runtime-worker",
    allowedPort,
    true,
    "trusted archive runtime",
  );
  await expectConnection(
    "runtime-worker",
    allowedPort,
    false,
    "Chromium isolation",
    20001,
  );
  await expectConnection("runtime-worker", otherPort, false, "unlisted storage port");
  console.log(
    "Storage egress verified through Kubernetes, Cilium, and the managed worker",
  );
} finally {
  if (created) kubectl(["delete", "namespace", namespace, "--wait=false"]);
}
