import { createHash } from "node:crypto";
import { isIP } from "node:net";
import { setTimeout as delay } from "node:timers/promises";
import {
  createKubernetesClient,
  type KubernetesAccess,
  KubernetesError,
} from "./kubernetes-api.js";
import { WorkerCleanupFailure } from "./lifecycle-errors.js";
import { validProfileId } from "./local-profiles.js";
import type { ManagedBackend, ManagedWorker } from "./managed-backend.js";
import { WORKER_ENVIRONMENT } from "./managed-backend.js";
import { createControllerRuntime } from "./network-control.js";

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
}

const identity = (value: string) => /^[a-f0-9-]{36}$/.test(value);
const dnsName = (value: string) => /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(value);
const prefix = "browser-sandbox.io/";
/** Optional adapter: no Kubernetes dependency or credentials enter the local runtime. */
export function createKubernetesBackend(
  options: KubernetesBackendOptions,
): ManagedBackend {
  const template = options.template;
  const grace = template.terminationGracePeriodSeconds ?? 60;
  const creationTimeoutMs = options.creationTimeoutMs ?? 120000;
  const hardTimeoutMs = Number(options.environment?.TIMEOUT ?? 1800000);
  if (
    !dnsName(options.namespace) ||
    !dnsName(options.installation) ||
    !dnsName(options.ownerName) ||
    !/^[^\s]+@sha256:[a-f0-9]{64}$/.test(options.image) ||
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
  const owned = (
    pod: any,
    expected: Record<string, string>,
    ownerUid: string,
    uid?: string,
  ) => {
    if (
      (uid && pod.metadata.uid !== uid) ||
      !Object.entries(expected).every(
        ([key, value]) => pod.metadata.labels?.[key] === value,
      ) ||
      !pod.metadata.ownerReferences?.some(
        (owner) =>
          owner.apiVersion === "v1" &&
          owner.kind === "ConfigMap" &&
          owner.name === options.ownerName &&
          owner.uid === ownerUid &&
          owner.controller === true,
      )
    )
      throw Error("browser_worker_ownership_mismatch");
  };
  async function readOwner(signal: AbortSignal): Promise<string> {
    const owner = await api(
      `${base}/configmaps/${options.ownerName}`,
      "GET",
      undefined,
      signal,
    );
    if (
      owner.metadata?.labels?.["app.kubernetes.io/instance"] !== options.installation ||
      owner.metadata.labels["app.kubernetes.io/name"] !== "browser-sandbox" ||
      !owner.metadata.uid
    )
      throw Error("browser_worker_ownership_mismatch");
    return owner.metadata.uid;
  }
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
    owned(pod, expected, await readOwner(signal), uid);
    await api(
      `${base}/pods/${name}`,
      "DELETE",
      {
        apiVersion: "v1",
        kind: "DeleteOptions",
        preconditions: { uid: pod.metadata.uid },
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
      const ownerUid = await readOwner(signal);
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
              annotations: { [prefix + "profile-id"]: placement.profileId ?? "" },
              ownerReferences: [
                {
                  apiVersion: "v1",
                  kind: "ConfigMap",
                  name: options.ownerName,
                  uid: ownerUid,
                  controller: true,
                  blockOwnerDeletion: false,
                },
              ],
            },
            spec: {
              restartPolicy: "Never",
              automountServiceAccountToken: false,
              enableServiceLinks: false,
              activeDeadlineSeconds:
                Math.ceil((hardTimeoutMs + creationTimeoutMs) / 1000) + grace,
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
        while (true) {
          const state = await getPod(name, signal);
          if (!state) throw Error("browser_worker_lost");
          owned(state, expected, ownerUid, uid);
          if (
            state.metadata.deletionTimestamp ||
            ["Failed", "Succeeded"].includes(state.status?.phase)
          )
            throw Error("browser_worker_lost");
          if (
            state.status?.containerStatuses?.some((container) =>
              [
                "ErrImagePull",
                "ImagePullBackOff",
                "InvalidImageName",
                "CreateContainerConfigError",
              ].includes(container.state?.waiting?.reason),
            )
          )
            throw Error("browser_worker_start_failed");
          if (
            state.status?.phase === "Running" &&
            state.status.conditions?.some(
              (condition) => condition.type === "Ready" && condition.status === "True",
            )
          ) {
            if (!isIP(state.status.podIP))
              throw Error("browser_kubernetes_response_invalid");
            const host =
              isIP(state.status.podIP) === 6
                ? `[${state.status.podIP}]`
                : state.status.podIP;
            const origin = `http://${host}:8081`;
            return {
              id: `${name}/${uid}`,
              executionId: placement.executionId,
              incarnation: placement.incarnation,
              origin,
              profileId: placement.profileId,
              runtime: createControllerRuntime(origin, {
                automation: placement.automation,
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
        throw error;
      }
    },
    async list(signal) {
      const ownerUid = await readOwner(signal);
      const selector = encodeURIComponent(
        `browser-sandbox.io/installation=${options.installation},app.kubernetes.io/component=worker`,
      );
      const workers: ManagedWorker[] = [];
      const cursors = new Set<string>();
      let cursor = "";
      do {
        const page = await api(
          `${base}/pods?labelSelector=${selector}&limit=100&continue=${encodeURIComponent(cursor)}`,
          "GET",
          undefined,
          signal,
        );
        if (
          !Array.isArray(page.items) ||
          page.items.length > 100 ||
          workers.length + page.items.length > 10000
        )
          throw Error("browser_worker_discovery_failed");
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
            !pod.metadata.uid ||
            !pod.metadata.ownerReferences?.some((item) => item.uid === ownerUid)
          )
            throw Error("browser_worker_discovery_failed");
          owned(pod, labels(executionId, incarnation, profileId), ownerUid);
          const ip = pod.status?.podIP;
          const origin = () => {
            if (!isIP(ip)) throw Error("browser_worker_not_ready");
            return `http://${isIP(ip) === 6 ? `[${ip}]` : ip}:8081`;
          };
          let runtime: ReturnType<typeof createControllerRuntime>;
          workers.push({
            id: `${pod.metadata.name}/${pod.metadata.uid}`,
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
        cursor = page.metadata?.continue ?? "";
        if (
          typeof cursor !== "string" ||
          cursor.length > 8192 ||
          (cursor && cursors.has(cursor))
        )
          throw Error("browser_worker_discovery_failed");
        cursors.add(cursor);
      } while (cursor);
      return workers;
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
