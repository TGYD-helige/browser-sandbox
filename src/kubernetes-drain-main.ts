import { readKubernetesAccess } from "./kubernetes-api.js";
import { drainKubernetesWorkers } from "./kubernetes-drain.js";

try {
  const timeoutMs = Number(process.env.BROWSER_DRAIN_TIMEOUT_MS ?? 240000);
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 1800000)
    throw Error("browser_kubernetes_configuration_invalid");
  await drainKubernetesWorkers(
    {
      ...(await readKubernetesAccess()),
      installation: process.env.BROWSER_INSTALLATION,
      controllerName: process.env.BROWSER_KUBERNETES_CONTROLLER,
      ownerName: process.env.BROWSER_KUBERNETES_OWNER,
    },
    AbortSignal.timeout(timeoutMs),
  );
} catch (error) {
  console.error(
    /^browser_[a-z_]+$/.test(error?.message)
      ? error.message
      : "browser_kubernetes_drain_failed",
  );
  process.exitCode = 1;
}
