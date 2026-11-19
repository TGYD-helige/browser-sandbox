import { createServer } from "node:http";
import { createController, validateControllerRecord } from "./controller.js";
import { createProfileStore } from "./profile-store.js";

const root = process.env.BROWSER_PROFILE_STORE_ROOT;
const replicas = Number(process.env.BROWSER_CONTROLLER_REPLICAS ?? 1);
if (!Number.isSafeInteger(replicas) || replicas < 1 || replicas > 100)
  throw Error("browser_controller_configuration_invalid");
if (replicas > 1 && root) throw Error("browser_profile_store_replicas_unsupported");
let ownership: import("./kubernetes-ownership.js").KubernetesOwnership;
if (root)
  process.env.BROWSER_PROFILE_STORE_ORIGIN ??= `http://${process.env.BROWSER_CONTROLLER_CONTAINER ?? process.env.HOSTNAME}:8083`;
else delete process.env.BROWSER_PROFILE_STORE_ORIGIN;
const maxExecutionMs = Number(process.env.BROWSER_MAX_EXECUTION_MS ?? 3600000);
const backendName = process.env.BROWSER_BACKEND ?? "docker";
let backend: import("./managed-backend.js").ManagedBackend;
if (backendName === "kubernetes") {
  const { readFile } = await import("node:fs/promises");
  const { createKubernetesBackend } = await import("./kubernetes-backend.js");
  const { readKubernetesAccess } = await import("./kubernetes-api.js");
  const access = await readKubernetesAccess();
  if (replicas > 1 || process.env.BROWSER_SHARED_STATE === "true") {
    const { createKubernetesOwnership } = await import("./kubernetes-ownership.js");
    ownership = await createKubernetesOwnership({
      ...access,
      installation: process.env.BROWSER_INSTALLATION ?? "default",
      ownerName: process.env.BROWSER_KUBERNETES_OWNER,
      replica: process.env.BROWSER_CONTROLLER_ID,
      peerOrigin: process.env.BROWSER_PEER_ORIGIN,
    });
    if (replicas === 1) {
      const { importExecutionJournal } = await import("./kubernetes-migration.js");
      await importExecutionJournal({
        directory:
          process.env.BROWSER_CONTROLLER_STATE ?? "/controller-data/executions",
        installationId: process.env.BROWSER_INSTALLATION ?? "default",
        ownership,
        validate: validateControllerRecord,
      });
    }
  }
  backend = await createKubernetesBackend({
    ownership,
    installation: process.env.BROWSER_INSTALLATION ?? "default",
    ...access,
    ownerName: process.env.BROWSER_KUBERNETES_OWNER,
    image: process.env.BROWSER_IMAGE,
    template: JSON.parse(
      await readFile(
        process.env.BROWSER_WORKER_TEMPLATE ??
          "/etc/browser-sandbox/worker-template.json",
        "utf8",
      ),
    ),
    environment: process.env,
    creationTimeoutMs: Number(process.env.BROWSER_WORKER_CREATION_TIMEOUT ?? 90000),
    maxExecutionMs,
  });
  // Release-owned (epoch 0) workers predate shared state; without the import their records are unreachable.
  if (
    replicas > 1 &&
    !(await ownership.migrationComplete()) &&
    (await backend.list(AbortSignal.timeout(30000))).some(
      (worker) => worker.epoch === 0,
    )
  )
    throw Error("browser_execution_migration_required");
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
const store = root
  ? await createProfileStore({
      root,
      baseUrl: process.env.BROWSER_PROFILE_STORE_ORIGIN,
      maxProfiles: Number(process.env.BROWSER_PROFILE_MAX_COUNT ?? 10000),
      maxBytes: Number(process.env.BROWSER_PROFILE_MAX_BYTES ?? 10 * 1024 ** 3),
      maxStagedBytes: Number(process.env.BROWSER_PROFILE_MAX_STAGED_BYTES ?? 1024 ** 3),
      maxTransfers: Number(process.env.BROWSER_PROFILE_MAX_TRANSFERS ?? 4),
      idleTtlMs: process.env.BROWSER_PROFILE_IDLE_TTL_MS
        ? Number(process.env.BROWSER_PROFILE_IDLE_TTL_MS)
        : undefined,
    })
  : undefined;
const storageServer = store
  ? createServer((req, res) => {
      store
        .handle(req, res)
        .then((handled) => {
          if (!handled) res.writeHead(404).end();
        })
        .catch(() => res.destroy());
    })
  : undefined;
if (storageServer)
  await new Promise<void>((resolve, reject) => {
    storageServer.once("error", reject);
    storageServer.listen(8083, "0.0.0.0", resolve);
  });
const controller = await createController({
  profileStore: store,
  ownership,
  replicas,
  peerPort: Number(process.env.BROWSER_PEER_PORT ?? 8084),
  onWorkerEvent: (event) => console.log(JSON.stringify(event)),
  backend,
  installationId: process.env.BROWSER_INSTALLATION ?? "default",
  stateDirectory: process.env.BROWSER_CONTROLLER_STATE ?? "/controller-data/executions",
  maxWorkers: Number(process.env.BROWSER_MAX_WORKERS ?? 8),
  maxPending: Number(process.env.BROWSER_MAX_PENDING ?? 2),
  maxQueued: Number(process.env.BROWSER_MAX_QUEUED ?? 32),
  creationTimeoutMs: Number(process.env.BROWSER_WORKER_CREATION_TIMEOUT ?? 90000),
  maxExecutionMs,
  idleRegistrationMs: Number(process.env.BROWSER_IDLE_REGISTRATION_MS ?? 900000),
  port: Number(process.env.BROWSER_CONTROLLER_PORT ?? 8080),
  host: process.env.BROWSER_CONTROLLER_HOST ?? "0.0.0.0",
  publicOrigin: process.env.BROWSER_CONTROLLER_ORIGIN,
  controlPort: Number(process.env.BROWSER_HOST_CONTROL_PORT ?? 8082),
  controlHost: process.env.BROWSER_HOST_CONTROL_HOST ?? "127.0.0.1",
  viewerOrigins: process.env.BROWSER_VIEWER_ORIGINS?.split(","),
});
const maintenance = store
  ? setInterval(() => {
      controller
        .expireProfiles(AbortSignal.timeout(30000))
        .catch(() => console.error("browser_profile_expiry_failed"));
    }, 60000)
  : undefined;
maintenance?.unref();
for (const signal of ["SIGINT", "SIGTERM"])
  process.once(signal, async () => {
    try {
      clearInterval(maintenance);
      try {
        await controller.stop();
      } finally {
        await store?.close();
        if (storageServer) {
          storageServer.closeAllConnections();
          await new Promise<void>((resolve) => storageServer.close(() => resolve()));
        }
      }
      process.exit(0);
    } catch {
      process.exit(1);
    }
  });
