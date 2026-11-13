import { createController } from "./controller.js";
import {
  createDockerBackend,
  createDockerClient,
  readDockerSeccomp,
} from "./docker-backend.js";

const container = process.env.BROWSER_CONTROLLER_CONTAINER ?? process.env.HOSTNAME;
if (!container) throw Error("browser_controller_container_required");
const socketPath = process.env.BROWSER_DOCKER_SOCKET;
const image =
  process.env.BROWSER_IMAGE ??
  (await createDockerClient(socketPath)(`/containers/${container}/json`)).Image;
const controller = await createController({
  onWorkerEvent: (event) => console.log(JSON.stringify(event)),
  backend: createDockerBackend({
    installation: process.env.BROWSER_INSTALLATION ?? "default",
    controllerContainer: container,
    image,
    socketPath,
    seccomp: await readDockerSeccomp(),
    environment: process.env,
    extensionsSource: process.env.BROWSER_EXTENSIONS_SOURCE,
  }),
  port: Number(process.env.BROWSER_CONTROLLER_PORT ?? 8080),
  host: process.env.BROWSER_CONTROLLER_HOST ?? "0.0.0.0",
  publicOrigin: process.env.BROWSER_CONTROLLER_ORIGIN,
  viewerOrigins: process.env.BROWSER_VIEWER_ORIGINS?.split(","),
});
for (const signal of ["SIGINT", "SIGTERM"])
  process.once(signal, async () => {
    try {
      await controller.stop();
      process.exit(0);
    } catch {
      process.exit(1);
    }
  });
