import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";

const namespace = process.env.PROBE_NAMESPACE || `browser-net-${process.pid}`;
const image = process.env.PROBE_IMAGE || "node:22-bookworm-slim";
const kubectl = (...args) =>
  execFileSync("kubectl", ["--request-timeout=15s", "-n", namespace, ...args], {
    encoding: "utf8",
    timeout: args.includes("wait") ? 140000 : 20000,
    stdio: ["pipe", "pipe", "pipe"],
  });
const apply = (manifest) =>
  execFileSync(
    "kubectl",
    ["--request-timeout=15s", "-n", namespace, "apply", "-f", "-"],
    {
      input: typeof manifest === "string" ? manifest : JSON.stringify(manifest),
      encoding: "utf8",
      timeout: 20000,
    },
  );
const address = (name) =>
  JSON.parse(kubectl("get", "pod", name, "-o", "json")).status.podIP;
const connects = (name, host, port) => {
  const source = `const net=require('node:net');const s=net.connect(${JSON.stringify({ host, port })});s.setTimeout(1200);s.on('connect',()=>{s.destroy();process.exit(0)});s.on('timeout',()=>process.exit(1));s.on('error',()=>process.exit(1));`;
  try {
    kubectl("exec", name, "--", "node", "-e", source);
    return true;
  } catch (error) {
    // A kubectl failure must not masquerade as a NetworkPolicy denial.
    if (String(error.stderr).includes("command terminated with exit code 1"))
      return false;
    throw error;
  }
};
async function expectConnection(name, host, port, expected) {
  const deadline = Date.now() + 30000;
  while (true) {
    if (connects(name, host, port) === expected) return;
    assert(
      Date.now() < deadline,
      `${name} -> ${host}:${port}: expected connection=${expected}`,
    );
    await delay(250);
  }
}

let created = false;
try {
  const nodes = JSON.parse(kubectl("get", "nodes", "-o", "json")).items;
  assert(nodes.length >= 3, "NetworkPolicy tests require at least three nodes");
  const agents = nodes.filter(
    (node) =>
      node.metadata.labels["node-role.kubernetes.io/control-plane"] === undefined,
  );
  assert(agents.length >= 2, "NetworkPolicy tests require two worker nodes");
  // create, rather than apply, refuses to adopt somebody else's namespace.
  execFileSync("kubectl", ["create", "namespace", namespace], {
    stdio: "pipe",
    timeout: 20000,
  });
  created = true;
  for (const [index, role] of [
    "controller",
    "worker",
    "agent",
    "other-worker",
    "host",
  ].entries()) {
    apply({
      apiVersion: "v1",
      kind: "Pod",
      metadata: {
        name: role,
        namespace,
        labels: {
          "app.kubernetes.io/name": "browser-sandbox",
          "app.kubernetes.io/instance": "probe",
          "app.kubernetes.io/component": role === "other-worker" ? "worker" : role,
          ...(role === "host" ? { "browser-sandbox.io/host-control": "true" } : {}),
        },
      },
      spec: {
        nodeName: agents[index % 2].metadata.name,
        terminationGracePeriodSeconds: 5,
        automountServiceAccountToken: false,
        containers: [
          {
            name: "probe",
            image,
            imagePullPolicy: "IfNotPresent",
            command: [
              "node",
              "-e",
              "const http=require('node:http');const servers=[8080,8082].map(port=>http.createServer((q,s)=>s.end('ok')).listen(port,'0.0.0.0'));process.on('SIGTERM',()=>{for(const server of servers){server.closeAllConnections();server.close()}})",
            ],
            readinessProbe: { tcpSocket: { port: 8080 }, periodSeconds: 1 },
            securityContext: {
              allowPrivilegeEscalation: false,
              capabilities: { drop: ["ALL"] },
            },
          },
        ],
      },
    });
  }
  kubectl("wait", "--for=condition=Ready", "pod", "--all", "--timeout=120s");
  const worker = address("worker");
  const controller = address("controller");
  const other = address("other-worker");
  await expectConnection("agent", worker, 8080, true);
  await expectConnection("agent", controller, 8082, true);
  await expectConnection("host", controller, 8082, true);
  await expectConnection("worker", controller, 8080, true);
  await expectConnection("worker", other, 8080, true);
  const api = JSON.parse(
    kubectl("get", "service", "kubernetes", "-n", "default", "-o", "json"),
  ).spec.clusterIP;
  const apiNode = nodes
    .find(
      (node) =>
        node.metadata.labels["node-role.kubernetes.io/control-plane"] !== undefined,
    )
    .status.addresses.find((address) => address.type === "InternalIP").address;
  await expectConnection("worker", api, 443, true);
  await expectConnection("worker", apiNode, 6443, true);
  const policy = execFileSync(
    "helm",
    [
      "template",
      "probe",
      fileURLToPath(new URL("../../../charts/browser-sandbox", import.meta.url)),
      "--namespace",
      namespace,
      "--set",
      `image.digest=sha256:${"a".repeat(64)}`,
      "--show-only",
      "templates/networkpolicy.yaml",
      "--set",
      `networkPolicy.apiServerCidrs[0]=${api}/32`,
    ],
    { encoding: "utf8" },
  );
  apply(policy);
  await expectConnection("agent", worker, 8080, false);
  await expectConnection("agent", controller, 8080, true);
  await expectConnection("agent", controller, 8082, false);
  await expectConnection("host", controller, 8082, true);
  await expectConnection("controller", worker, 8080, true);
  await expectConnection("worker", controller, 8080, false);
  await expectConnection("worker", other, 8080, false);
  await expectConnection("worker", api, 443, false);
  await expectConnection("worker", apiNode, 6443, false);
  await expectConnection("worker", "169.254.169.254", 80, false);
  console.log(
    "NetworkPolicy enforced: agent worker/control ingress, worker management/peer/API/metadata egress denied; trusted host control and agent access allowed",
  );
} finally {
  if (created) {
    kubectl("delete", "namespace", namespace, "--wait=false");
    kubectl("wait", "--for=delete", `namespace/${namespace}`, "--timeout=90s");
  }
}
