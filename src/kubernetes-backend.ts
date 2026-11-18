import { createHash } from "node:crypto";
import { isIP } from "node:net";
import { setTimeout as delay } from "node:timers/promises";
import {
  createKubernetesClient,
  hasKubernetesOwner,
  type KubernetesAccess,
  KubernetesError,
  kubernetesOwnerReference,
  listKubernetesPages,
  readKubernetesOwner,
  workerOwnerCheck,
} from "./kubernetes-api.js";
import type { KubernetesOwnership } from "./kubernetes-ownership.js";
import { WorkerCleanupFailure } from "./lifecycle-errors.js";
import { validProfileId } from "./local-profiles.js";
import type { ManagedBackend, ManagedWorker } from "./managed-backend.js";
import { WORKER_ENVIRONMENT } from "./managed-backend.js";
import { CONTROL_TIMEOUT_MS, createControllerRuntime } from "./network-control.js";

export interface KubernetesWorkerTemplate {
  resources?: object;
  securityContext: {
    runAsUser?: number;
    allowPrivilegeEscalation?: boolean;
    capabilities?: { drop?: string[]; add?: string[] };
    seccompProfile: { type: "Localhost"; localhostProfile: string };
  };
  shmSize?: string;
  terminationGracePeriodSeconds?: number;
  nodeSelector?: Record<string, string>;
  tolerations?: object[];
  affinity?: object;
  imagePullSecrets?: { name: string }[];
  extensions?: {
    configMap?: { name: string };
    persistentVolumeClaim?: { claimName: string };
  };
}
export interface KubernetesBackendOptions extends KubernetesAccess {
  installation: string;
  ownerName: string;
  image: string;
  environment?: Record<string, string>;
  template: KubernetesWorkerTemplate;
  creationTimeoutMs?: number;
  maxExecutionMs?: number;
  ownership?: KubernetesOwnership;
}

const identity = (value: string) => /^[a-f0-9-]{36}$/.test(value);
const dnsName = (value: string) => /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(value);
const prefix = "browser-sandbox.io/";
/** Optional adapter: no Kubernetes dependency or credentials enter the local runtime. */
export async function createKubernetesBackend(
  options: KubernetesBackendOptions,
): Promise<ManagedBackend> {
  const template = options.template;
  const grace = template.terminationGracePeriodSeconds ?? 60;
  const creationTimeoutMs = options.creationTimeoutMs ?? 120000;
  const maxExecutionMs = options.maxExecutionMs ?? 3600000;
  const hardTimeoutMs = Number(options.environment?.TIMEOUT ?? 1800000);
  if (
    !dnsName(options.namespace) ||
    !dnsName(options.installation) ||
    !dnsName(options.ownerName) ||
    !/^[^\s]+@sha256:[a-f0-9]{64}$/.test(options.image) ||
    !Number.isSafeInteger(maxExecutionMs) ||
    maxExecutionMs < 1 ||
    maxExecutionMs > 2147483647 ||
    !Number.isSafeInteger(hardTimeoutMs) ||
    hardTimeoutMs < 1 ||
    hardTimeoutMs > 2147483647 ||
    !Number.isSafeInteger(creationTimeoutMs) ||
    creationTimeoutMs < 1 ||
    creationTimeoutMs > 600000 ||
    !Number.isSafeInteger(grace) ||
    grace < 1 ||
    grace > 600 ||
    template.securityContext?.seccompProfile?.type !== "Localhost" ||
    !template.securityContext.seccompProfile.localhostProfile ||
    (template.extensions &&
      Number(Boolean(template.extensions.configMap)) +
        Number(Boolean(template.extensions.persistentVolumeClaim)) !==
        1)
  )
    throw Error("browser_kubernetes_configuration_invalid");
  const api = createKubernetesClient(options);
  const parent = await readKubernetesOwner(api, options, AbortSignal.timeout(15000));
  const base = `/api/v1/namespaces/${options.namespace}`;
  const profileLabel = (profileId?: string) =>
    profileId
      ? createHash("sha256").update(profileId).digest("hex").slice(0, 63)
      : "temporary";
  const labels = (executionId: string, incarnation: string, profileId?: string) => ({
    "app.kubernetes.io/name": "browser-sandbox",
    "app.kubernetes.io/instance": options.installation,
    "app.kubernetes.io/component": "worker",
    [prefix + "installation"]: options.installation,
    [prefix + "execution"]: executionId,
    [prefix + "incarnation"]: incarnation,
    [prefix + "profile"]: profileLabel(profileId),
  });
  const owned = (pod: any, expected: Record<string, string>, uid?: string) => {
    if (
      (uid && pod.metadata.uid !== uid) ||
      !Object.entries(expected).every(
        ([key, value]) => pod.metadata.labels?.[key] === value,
      )
    )
      throw Error("browser_worker_ownership_mismatch");
  };
  const leaseOwner = ({ name, uid }: { name: string; uid: string }) =>
    kubernetesOwnerReference("Lease", name, uid);
  const podEpoch = (pod: any) =>
    Number(pod.metadata.annotations?.[prefix + "epoch"] ?? 0);
  const workerOrigin = (ip: string, error: string) => {
    const family = isIP(ip);
    if (!family) throw Error(error);
    return `http://${family === 6 ? `[${ip}]` : ip}:8081`;
  };
  const assertWorkerLease = async (pod: any, id: string, releaseOwned: boolean) => {
    const current = await options.ownership.assertOwned(id);
    const epoch = podEpoch(pod);
    if (
      (!releaseOwned && !hasKubernetesOwner(pod, leaseOwner(current))) ||
      !Number.isSafeInteger(epoch) ||
      epoch < 0 ||
      epoch > current.epoch
    )
      throw Error("browser_controller_fenced");
    return current;
  };
  async function getPod(name: string, signal: AbortSignal) {
    try {
      return await api(`${base}/pods/${name}`, "GET", undefined, signal);
    } catch (error) {
      if (error instanceof KubernetesError && error.status === 404) return undefined;
      throw error;
    }
  }
  async function removeOwned(
    name: string,
    expected: Record<string, string>,
    signal: AbortSignal,
    uid?: string,
    requirePresent = false,
  ) {
    const pod = await getPod(name, signal);
    if (!pod) {
      if (requirePresent) throw Error("browser_worker_cleanup_unconfirmed");
      return;
    }
    owned(pod, expected, uid);
    const releaseOwned = hasKubernetesOwner(pod, parent);
    if (options.ownership)
      await assertWorkerLease(pod, expected[prefix + "execution"], releaseOwned);
    else if (!releaseOwned) throw Error("browser_worker_ownership_mismatch");
    await api(
      `${base}/pods/${name}`,
      "DELETE",
      {
        apiVersion: "v1",
        kind: "DeleteOptions",
        preconditions: {
          uid: pod.metadata.uid,
          ...(options.ownership
            ? { resourceVersion: pod.metadata.resourceVersion }
            : {}),
        },
        propagationPolicy: "Background",
      },
      signal,
    ).catch((error) => {
      if (!(error instanceof KubernetesError && error.status === 404)) throw error;
    });
    while (true) {
      const remaining = await getPod(name, signal);
      if (!remaining || remaining.metadata.uid !== pod.metadata.uid) return;
      await delay(100, undefined, { signal });
    }
  }
  return {
    preserveWorkersOnShutdown: true,
    async create(placement, callerSignal) {
      if (!identity(placement.executionId) || !identity(placement.incarnation))
        throw Error("browser_identity_invalid");
      const fence = options.ownership
        ? await options.ownership.assertOwned(placement.executionId)
        : undefined;
      const ownerRef = fence ? leaseOwner(fence) : parent;
      const assertSameOwner = async () => {
        if (!fence) return;
        const current = await options.ownership.assertOwned(placement.executionId);
        if (current.uid !== fence.uid || current.epoch !== fence.epoch)
          throw Error("browser_controller_fenced");
      };
      const epoch = fence?.epoch;
      const signal = AbortSignal.any([
        callerSignal,
        AbortSignal.timeout(creationTimeoutMs),
      ]);
      const expected = labels(
        placement.executionId,
        placement.incarnation,
        placement.profileId,
      );
      const name = `bs-${options.installation.slice(0, 30)}-${createHash("sha256").update(placement.incarnation).digest("hex").slice(0, 20)}`;
      let attempted = false;
      let uid: string;
      try {
        const environment: { name: string; value: string }[] =
          WORKER_ENVIRONMENT.filter(
            (key) => options.environment?.[key] !== undefined,
          ).map((key) => ({ name: key, value: options.environment[key] }));
        environment.push(
          { name: "CONCURRENT", value: "1" },
          { name: "QUEUED", value: "0" },
          { name: "BROWSER_EXECUTION_ID", value: placement.executionId },
          ...(fence ? [{ name: "BROWSER_OWNER_UID", value: fence.uid }] : []),
          { name: "BROWSER_WORKER_ORIGIN", value: "http://$(BROWSER_POD_IP):8081" },
        );
        if (template.extensions)
          environment.push({
            name: "BROWSER_EXTENSIONS_DIR",
            value: "/opt/browser-extensions",
          });
        attempted = true;
        const pod = await api(
          `${base}/pods`,
          "POST",
          {
            apiVersion: "v1",
            kind: "Pod",
            metadata: {
              name,
              labels: expected,
              annotations: {
                [prefix + "profile-id"]: placement.profileId ?? "",
                ...(epoch
                  ? {
                      [prefix + "epoch"]: String(epoch),
                    }
                  : {}),
              },
              ownerReferences: [ownerRef],
            },
            spec: {
              restartPolicy: "Never",
              automountServiceAccountToken: false,
              enableServiceLinks: false,
              activeDeadlineSeconds:
                Math.ceil(
                  (Math.max(hardTimeoutMs, maxExecutionMs) +
                    creationTimeoutMs +
                    CONTROL_TIMEOUT_MS) /
                    1000,
                ) + grace,
              terminationGracePeriodSeconds: grace,
              imagePullSecrets: template.imagePullSecrets ?? [],
              nodeSelector: template.nodeSelector ?? {},
              tolerations: template.tolerations ?? [],
              affinity: template.affinity ?? {},
              containers: [
                {
                  name: "worker",
                  image: options.image,
                  args: ["worker"],
                  env: [
                    {
                      name: "BROWSER_POD_IP",
                      valueFrom: { fieldRef: { fieldPath: "status.podIP" } },
                    },
                    ...environment,
                  ],
                  securityContext: {
                    ...template.securityContext,
                    privileged: false,
                    allowPrivilegeEscalation: false,
                  },
                  resources: template.resources ?? {},
                  readinessProbe: {
                    httpGet: { path: "/health", port: 8081 },
                    periodSeconds: 1,
                    timeoutSeconds: 2,
                  },
                  volumeMounts: [
                    { name: "shm", mountPath: "/dev/shm" },
                    ...(template.extensions
                      ? [
                          {
                            name: "extensions",
                            mountPath: "/opt/browser-extensions",
                            readOnly: true,
                          },
                        ]
                      : []),
                  ],
                },
              ],
              volumes: [
                {
                  name: "shm",
                  emptyDir: {
                    medium: "Memory",
                    sizeLimit: template.shmSize ?? "512Mi",
                  },
                },
                ...(template.extensions
                  ? [
                      {
                        name: "extensions",
                        ...(template.extensions.configMap
                          ? { configMap: template.extensions.configMap }
                          : {
                              persistentVolumeClaim: {
                                ...template.extensions.persistentVolumeClaim,
                                readOnly: true,
                              },
                            }),
                      },
                    ]
                  : []),
              ],
            },
          },
          signal,
        );
        uid = pod.metadata.uid;
        if (!uid) throw Error("browser_kubernetes_response_invalid");
        await assertSameOwner();
        while (true) {
          const state = await getPod(name, signal);
          if (!state) throw Error("browser_worker_lost");
          owned(state, expected, uid);
          if (!hasKubernetesOwner(state, ownerRef))
            throw Error("browser_controller_fenced");
          if (
            state.metadata.deletionTimestamp ||
            ["Failed", "Succeeded"].includes(state.status?.phase)
          )
            throw Error("browser_worker_lost");
          if (
            state.status?.conditions?.some(
              (condition) =>
                condition.type === "PodScheduled" &&
                condition.status === "False" &&
                condition.reason === "Unschedulable",
            )
          )
            throw Error("browser_worker_unschedulable");
          const waiting =
            state.status?.containerStatuses?.map(
              (container) => container.state?.waiting?.reason,
            ) ?? [];
          if (
            waiting.some((reason) =>
              ["ErrImagePull", "ImagePullBackOff", "InvalidImageName"].includes(reason),
            )
          )
            throw Error("browser_worker_image_unavailable");
          if (
            waiting.some((reason) =>
              [
                "CreateContainerConfigError",
                "CreateContainerError",
                "RunContainerError",
              ].includes(reason),
            )
          )
            throw Error("browser_worker_configuration_invalid");
          if (
            state.status?.phase === "Running" &&
            state.status.conditions?.some(
              (condition) => condition.type === "Ready" && condition.status === "True",
            )
          ) {
            const origin = workerOrigin(
              state.status.podIP,
              "browser_kubernetes_response_invalid",
            );
            await assertSameOwner();
            return {
              id: `${name}/${uid}`,
              epoch,
              executionId: placement.executionId,
              incarnation: placement.incarnation,
              origin,
              profileId: placement.profileId,
              runtime: createControllerRuntime(origin, {
                automation: placement.automation,
                epoch,
                ownerUid: fence?.uid,
              }),
            };
          }
          await delay(100, undefined, { signal });
        }
      } catch (error) {
        const rejected =
          !uid &&
          error instanceof KubernetesError &&
          error.status >= 400 &&
          error.status < 500 &&
          error.status !== 408 &&
          error.status !== 499;
        if (attempted && !rejected) {
          try {
            await removeOwned(
              name,
              expected,
              AbortSignal.timeout((grace + 10) * 1000),
              uid,
              !uid,
            );
          } catch (cleanup) {
            throw new WorkerCleanupFailure(error, cleanup);
          }
        }
        if (signal.aborted && !rejected)
          throw Error(
            callerSignal.aborted
              ? "browser_worker_creation_cancelled"
              : "browser_worker_creation_timeout",
          );
        if (error instanceof KubernetesError && error.quotaExceeded)
          throw Error("browser_worker_quota_exceeded", { cause: error });
        throw error;
      }
    },
    async list(signal) {
      const selector = encodeURIComponent(
        `browser-sandbox.io/installation=${options.installation},app.kubernetes.io/component=worker`,
      );
      const workers: ManagedWorker[] = [];
      const liveOwner = workerOwnerCheck(api, options, parent, signal);
      for await (const page of listKubernetesPages(
        api,
        `${base}/pods`,
        selector,
        signal,
        10000,
        "browser_worker_discovery_failed",
      )) {
        for (const pod of page.items) {
          const executionId = pod.metadata?.labels?.[prefix + "execution"],
            incarnation = pod.metadata?.labels?.[prefix + "incarnation"];
          const profileId = pod.metadata?.annotations?.[prefix + "profile-id"];
          if (
            !identity(executionId) ||
            !identity(incarnation) ||
            typeof profileId !== "string" ||
            (profileId && !validProfileId(profileId)) ||
            !dnsName(pod.metadata.name) ||
            !pod.metadata.uid
          )
            throw Error("browser_worker_discovery_failed");
          owned(pod, labels(executionId, incarnation, profileId));
          if (!(await liveOwner(pod))) continue;
          const origin = () =>
            workerOrigin(pod.status?.podIP, "browser_worker_not_ready");
          const epoch = podEpoch(pod);
          if (options.ownership && (!Number.isSafeInteger(epoch) || epoch < 0))
            throw Error("browser_worker_discovery_failed");
          let runtime: ReturnType<typeof createControllerRuntime>;
          workers.push({
            id: `${pod.metadata.name}/${pod.metadata.uid}`,
            epoch: options.ownership ? epoch : undefined,
            executionId,
            incarnation,
            profileId: profileId || undefined,
            get origin() {
              return origin();
            },
            get runtime() {
              return (runtime ??= createControllerRuntime(origin()));
            },
          });
        }
      }
      return workers;
    },
    async fence(worker, epoch, signal, stopped = false) {
      if (!options.ownership) return;
      const [name, uid] = worker.id.split("/");
      const pod = await getPod(name, signal);
      if (!pod) return;
      owned(pod, labels(worker.executionId, worker.incarnation, worker.profileId), uid);
      const releaseOwned = hasKubernetesOwner(pod, parent);
      const current = await assertWorkerLease(pod, worker.executionId, releaseOwned);
      if (current.epoch !== epoch) throw Error("browser_controller_fenced");
      if (podEpoch(pod) < epoch || releaseOwned)
        await api(
          `${base}/pods/${name}`,
          "PATCH",
          {
            metadata: {
              resourceVersion: pod.metadata.resourceVersion,
              ownerReferences: [leaseOwner(current)],
              annotations: {
                [prefix + "epoch"]: String(epoch),
              },
            },
          },
          signal,
        );
      worker.epoch = epoch;
      worker.runtime.setEpoch?.(epoch, current.uid);
      if (stopped) return;
      await worker.runtime.request(
        { op: "worker_status", executionId: worker.executionId },
        { signal, timeoutMs: 10000 },
      );
    },
    async remove(worker: ManagedWorker, callerSignal) {
      const [name, uid, extra] = worker.id.split("/");
      if (
        !dnsName(name) ||
        !uid ||
        extra ||
        !identity(worker.executionId) ||
        !identity(worker.incarnation)
      )
        throw Error("browser_worker_ownership_mismatch");
      await removeOwned(
        name,
        labels(worker.executionId, worker.incarnation, worker.profileId),
        AbortSignal.any([callerSignal, AbortSignal.timeout((grace + 10) * 1000)]),
        uid,
      );
    },
  };
}
