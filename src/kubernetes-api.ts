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
