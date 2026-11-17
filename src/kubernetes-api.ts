import { readFile } from "node:fs/promises";
import { request as httpRequest } from "node:http";
import { request as httpsRequest } from "node:https";
import { isIP } from "node:net";

export interface KubernetesAccess {
  namespace: string;
  apiServer?: string;
  ca?: Buffer;
  tokenFile?: string;
}
function defaultApiServer(): string {
  const host = process.env.KUBERNETES_SERVICE_HOST;
  if (!host) throw Error("browser_kubernetes_api_host_required");
  return `https://${isIP(host) === 6 ? `[${host}]` : host}:${process.env.KUBERNETES_SERVICE_PORT_HTTPS ?? 443}`;
}
export async function readKubernetesAccess(): Promise<KubernetesAccess> {
  const apiServer = defaultApiServer();
  const directory = "/var/run/secrets/kubernetes.io/serviceaccount";
  return {
    apiServer,
    namespace:
      process.env.BROWSER_KUBERNETES_NAMESPACE ??
      (await readFile(`${directory}/namespace`, "utf8")).trim(),
    ca: await readFile(`${directory}/ca.crt`),
    tokenFile: `${directory}/token`,
  };
}

export class KubernetesError extends Error {
  constructor(
    public status: number,
    public quotaExceeded = false,
  ) {
    super("browser_kubernetes_operation_failed");
  }
}

export function createKubernetesClient(options: KubernetesAccess) {
  const server = new URL(options.apiServer ?? defaultApiServer());
  if (
    server.username ||
    server.password ||
    (server.protocol !== "https:" &&
      !(
        server.protocol === "http:" && ["127.0.0.1", "[::1]"].includes(server.hostname)
      ))
  )
    throw Error("browser_kubernetes_configuration_invalid");
  return async (
    path: string,
    method: string,
    body: unknown,
    callerSignal: AbortSignal,
    onEvent?: (event: any) => void,
  ): Promise<any> => {
    const signal = AbortSignal.any([callerSignal, AbortSignal.timeout(15000)]);
    signal.throwIfAborted();
    const token = options.tokenFile
      ? (await readFile(options.tokenFile, "utf8")).trim()
      : undefined;
    signal.throwIfAborted();
    const payload = body === undefined ? undefined : JSON.stringify(body);
    if (payload && Buffer.byteLength(payload) > 1048576)
      throw Error("browser_kubernetes_request_too_large");
    return new Promise((resolve, reject) => {
      const request = (server.protocol === "https:" ? httpsRequest : httpRequest)(
        new URL(path, server),
        {
          method,
          ca: options.ca,
          signal,
          headers: {
            "content-type":
              method === "PATCH" ? "application/merge-patch+json" : "application/json",
            ...(payload
              ? { "content-length": String(Buffer.byteLength(payload)) }
              : {}),
            ...(token ? { authorization: `Bearer ${token}` } : {}),
          },
        },
        (response) => {
          if (onEvent && response.statusCode === 200) {
            let input = "";
            response.setEncoding("utf8");
            response.on("data", (chunk) => {
              input += chunk;
              if (Buffer.byteLength(input) > 1048576) {
                request.destroy(Error("browser_kubernetes_response_too_large"));
                return;
              }
              let end: number;
              while ((end = input.indexOf("\n")) >= 0) {
                const line = input.slice(0, end);
                input = input.slice(end + 1);
                if (!line.trim()) continue;
                try {
                  onEvent(JSON.parse(line));
                } catch {
                  request.destroy(Error("browser_kubernetes_response_invalid"));
                  return;
                }
              }
            });
            response.on("error", () => reject(Error("browser_kubernetes_unavailable")));
            response.on("end", () =>
              input.trim()
                ? reject(Error("browser_kubernetes_response_invalid"))
                : resolve({}),
            );
            return;
          }
          const chunks: Buffer[] = [];
          let size = 0;
          response.on("data", (chunk) => {
            size += chunk.length;
            if (size > 1048576)
              request.destroy(Error("browser_kubernetes_response_too_large"));
            else chunks.push(chunk);
          });
          response.on("error", () => reject(Error("browser_kubernetes_unavailable")));
          response.on("end", () => {
            if (response.statusCode < 200 || response.statusCode >= 300) {
              let quotaExceeded = false;
              if (response.statusCode === 403) {
                try {
                  const failure = JSON.parse(Buffer.concat(chunks).toString());
                  quotaExceeded =
                    typeof failure.message === "string" &&
                    /\bexceeded quota\b/i.test(failure.message);
                } catch {}
              }
              reject(new KubernetesError(response.statusCode, quotaExceeded));
              return;
            }
            try {
              resolve(size ? JSON.parse(Buffer.concat(chunks).toString()) : {});
            } catch {
              reject(Error("browser_kubernetes_response_invalid"));
            }
          });
        },
      );
      request.on("error", () => reject(Error("browser_kubernetes_unavailable")));
      request.end(payload);
    });
  };
}

export function kubernetesOwnerReference(
  kind: "ConfigMap" | "Lease",
  name: string,
  uid: string,
) {
  return {
    apiVersion: kind === "Lease" ? "coordination.k8s.io/v1" : "v1",
    kind,
    name,
    uid,
    controller: true,
    blockOwnerDeletion: false,
  };
}

export function hasKubernetesOwner(
  resource: any,
  expected: ReturnType<typeof kubernetesOwnerReference>,
) {
  const refs = resource?.metadata?.ownerReferences;
  return (
    refs?.length === 1 &&
    refs[0]?.controller === true &&
    ["apiVersion", "kind", "name", "uid"].every((key) => refs[0][key] === expected[key])
  );
}

export async function readKubernetesOwner(
  api: ReturnType<typeof createKubernetesClient>,
  options: KubernetesAccess & { installation: string; ownerName: string },
  signal: AbortSignal,
) {
  const owner = await api(
    `/api/v1/namespaces/${options.namespace}/configmaps/${options.ownerName}`,
    "GET",
    undefined,
    signal,
  );
  if (
    !owner.metadata?.uid ||
    owner.metadata.labels?.["app.kubernetes.io/instance"] !== options.installation ||
    owner.metadata.labels["app.kubernetes.io/name"] !== "browser-sandbox"
  )
    throw Error("browser_worker_ownership_mismatch");
  return kubernetesOwnerReference("ConfigMap", options.ownerName, owner.metadata.uid);
}

/** Resolve the single controller chain; a missing/replaced Lease belongs to GC. */
export async function readKubernetesWorkerOwner(
  api: ReturnType<typeof createKubernetesClient>,
  namespace: string,
  pod: any,
  parent: ReturnType<typeof kubernetesOwnerReference>,
  signal: AbortSignal,
): Promise<ReturnType<typeof kubernetesOwnerReference> | undefined> {
  const refs = pod.metadata?.ownerReferences;
  const ref = refs?.[0];
  if (refs?.length !== 1 || !ref.controller)
    throw Error("browser_worker_ownership_mismatch");
  if (hasKubernetesOwner(pod, parent)) return ref;
  if (ref.apiVersion !== "coordination.k8s.io/v1" || ref.kind !== "Lease")
    throw Error("browser_worker_ownership_mismatch");
  let lease: any;
  try {
    lease = await api(
      `/apis/coordination.k8s.io/v1/namespaces/${namespace}/leases/${encodeURIComponent(ref.name)}`,
      "GET",
      undefined,
      signal,
    );
  } catch (error) {
    if (error instanceof KubernetesError && error.status === 404) return;
    throw error;
  }
  if (lease.metadata?.uid !== ref.uid) return;
  if (
    lease.metadata.labels?.["browser-sandbox.io/state"] !== "execution" ||
    lease.metadata.labels?.["browser-sandbox.io/execution"] !==
      pod.metadata.labels?.["browser-sandbox.io/execution"] ||
    !hasKubernetesOwner(lease, parent)
  )
    throw Error("browser_worker_ownership_mismatch");
  return ref;
}
