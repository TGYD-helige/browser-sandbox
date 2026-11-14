import { setTimeout as delay } from "node:timers/promises";
import {
  createKubernetesClient,
  type KubernetesAccess,
  KubernetesError,
} from "./kubernetes-api.js";

export interface KubernetesDrainOptions extends KubernetesAccess {
  installation: string;
  controllerName: string;
  ownerName: string;
}

/** Helm must keep the worker network policy until every owned worker has exited. */
export async function drainKubernetesWorkers(
  options: KubernetesDrainOptions,
  signal: AbortSignal,
): Promise<void> {
  if (
    [
      options.namespace,
      options.installation,
      options.controllerName,
      options.ownerName,
    ].some((name) => !/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(name))
  )
    throw Error("browser_kubernetes_configuration_invalid");
  const api = createKubernetesClient(options);
  const base = `/api/v1/namespaces/${options.namespace}`;
  const labels = {
    "app.kubernetes.io/name": "browser-sandbox",
    "app.kubernetes.io/instance": options.installation,
  };
  async function list(role: string) {
    const selector = Object.entries({ ...labels, "app.kubernetes.io/component": role })
      .map(([name, value]) => `${name}=${value}`)
      .join(",");
    const result = await api(
      `${base}/pods?labelSelector=${encodeURIComponent(selector)}&limit=100`,
      "GET",
      undefined,
      signal,
    );
    if (
      !Array.isArray(result.items) ||
      (!result.items.length && result.metadata?.continue)
    )
      throw Error("browser_kubernetes_response_invalid");
    return result.items;
  }
  try {
    const owner = await api(
      `${base}/configmaps/${options.ownerName}`,
      "GET",
      undefined,
      signal,
    );
    if (
      !owner.metadata?.uid ||
      !Object.entries(labels).every(
        ([name, value]) => owner.metadata.labels?.[name] === value,
      )
    )
      throw Error("browser_worker_ownership_mismatch");
    const scalePath = `/apis/apps/v1/namespaces/${options.namespace}/deployments/${options.controllerName}/scale`;
    const scale = await api(scalePath, "GET", undefined, signal);
    if (
      scale.metadata?.name !== options.controllerName ||
      !scale.metadata.resourceVersion
    )
      throw Error("browser_kubernetes_response_invalid");
    await api(
      scalePath,
      "PATCH",
      {
        metadata: { resourceVersion: scale.metadata.resourceVersion },
        spec: { replicas: 0 },
      },
      signal,
    );
    while ((await list("controller")).length) await delay(100, undefined, { signal });
    while (true) {
      const workers = await list("worker");
      if (!workers.length) return;
      for (const pod of workers) {
        if (
          !pod.metadata?.uid ||
          !/^[a-z0-9][a-z0-9-]*$/.test(pod.metadata.name) ||
          !Object.entries({ ...labels, "app.kubernetes.io/component": "worker" }).every(
            ([name, value]) => pod.metadata.labels?.[name] === value,
          ) ||
          !pod.metadata.ownerReferences?.some(
            (reference) =>
              reference.apiVersion === "v1" &&
              reference.kind === "ConfigMap" &&
              reference.name === options.ownerName &&
              reference.uid === owner.metadata.uid &&
              reference.controller === true,
          )
        )
          throw Error("browser_worker_ownership_mismatch");
        if (!pod.metadata.deletionTimestamp) {
          await api(
            `${base}/pods/${pod.metadata.name}`,
            "DELETE",
            {
              apiVersion: "v1",
              kind: "DeleteOptions",
              preconditions: { uid: pod.metadata.uid },
              propagationPolicy: "Background",
            },
            signal,
          ).catch((error) => {
            if (!(error instanceof KubernetesError && error.status === 404))
              throw error;
          });
        }
      }
      await delay(100, undefined, { signal });
    }
  } catch (error) {
    if (signal.aborted) throw Error("browser_kubernetes_drain_timeout");
    throw error;
  }
}
