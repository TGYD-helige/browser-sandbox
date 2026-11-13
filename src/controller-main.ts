import { createController } from "./controller.js";

const backendName = process.env.BROWSER_BACKEND ?? "docker";
let backend: import("./managed-backend.js").ManagedBackend;
if (backendName === "kubernetes") {
  const { readFile } = await import("node:fs/promises");
  const { createKubernetesBackend } = await import("./kubernetes-backend.js");
  const serviceAccount = "/var/run/secrets/kubernetes.io/serviceaccount";
  backend = createKubernetesBackend({
    installation: process.env.BROWSER_INSTALLATION ?? "default",
    namespace:
      process.env.BROWSER_KUBERNETES_NAMESPACE ??
      (await readFile(`${serviceAccount}/namespace`, "utf8")).trim(),
    ownerName: process.env.BROWSER_KUBERNETES_OWNER,
    image: process.env.BROWSER_IMAGE,
    ca: await readFile(`${serviceAccount}/ca.crt`),
    tokenFile: `${serviceAccount}/token`,
    template: JSON.parse(
      await readFile(
        process.env.BROWSER_WORKER_TEMPLATE ??
          "/etc/browser-sandbox/worker-template.json",
        "utf8",
      ),
    ),
    environment: process.env,
  });
} else if (backendName === "docker") {
  const { createDockerBackend, createDockerClient, readDockerSeccomp } = await import(
    "./docker-backend.js"
  );
  const container = process.env.BROWSER_CONTROLLER_CONTAINER ?? process.env.HOSTNAME;
  if (!container) throw Error("browser_controller_container_required");
  const socketPath = process.env.BROWSER_DOCKER_SOCKET;
  const image =
    process.env.BROWSER_IMAGE ??
    (await createDockerClient(socketPath)(`/containers/${container}/json`)).Image;
  backend = createDockerBackend({
    installation: process.env.BROWSER_INSTALLATION ?? "default",
    controllerContainer: container,
    image,
    socketPath,
    seccomp: await readDockerSeccomp(),
    environment: process.env,
    extensionsSource: process.env.BROWSER_EXTENSIONS_SOURCE,
  });
} else throw Error("browser_backend_invalid");
const controller = await createController({
  onWorkerEvent: (event) => console.log(JSON.stringify(event)),
  backend,
  port: Number(process.env.BROWSER_CONTROLLER_PORT ?? 8080),
  host: process.env.BROWSER_CONTROLLER_HOST ?? "0.0.0.0",
  publicOrigin: process.env.BROWSER_CONTROLLER_ORIGIN,
  controlPort: Number(process.env.BROWSER_HOST_CONTROL_PORT ?? 8082),
  controlHost: process.env.BROWSER_HOST_CONTROL_HOST ?? "127.0.0.1",
  viewerOrigins: process.env.BROWSER_VIEWER_ORIGINS?.split(","),
});
for (const signal of ["SIGINT", "SIGTERM"])
  process.once(signal, async () => {
    try {
      await controller.stop({ preserveWorkers: backendName === "kubernetes" });
      process.exit(0);
    } catch {
      process.exit(1);
    }
  });
