import { readFile } from "node:fs/promises";
import { request as httpRequest } from "node:http";
import { setTimeout as delay } from "node:timers/promises";
import { WorkerCleanupFailure } from "./lifecycle-errors.js";
import {
  type ManagedBackend,
  type ManagedWorker,
  WORKER_ENVIRONMENT,
} from "./managed-backend.js";
import { createControllerRuntime } from "./network-control.js";
import { WORKER_PROTOCOL } from "./worker-protocol.js";

const prefix = "browser-sandbox.";
export interface DockerBackendOptions {
  installation: string;
  controllerContainer: string;
  image: string;
  socketPath?: string;
  seccomp: object;
  environment?: Record<string, string>;
  memoryBytes?: number;
  shmBytes?: number;
}

/** Docker management credentials and the socket are confined to the controller. */
export function createDockerBackend(options: DockerBackendOptions): ManagedBackend {
  if (
    !/^[a-z0-9][a-z0-9-]{0,47}$/.test(options.installation) ||
    !/^(sha256:[a-f0-9]{64}|[^\s]+@sha256:[a-f0-9]{64})$/.test(options.image)
  )
    throw Error("browser_docker_configuration_invalid");
  const api = createDockerClient(options.socketPath);
  const networks = new Map<string, string>();
  const labels = (executionId: string, incarnation: string) => ({
    [prefix + "installation"]: options.installation,
    [prefix + "execution"]: executionId,
    [prefix + "incarnation"]: incarnation,
  });
  async function removeNetwork(
    network: string,
    expected: Record<string, string>,
    signal: AbortSignal,
  ) {
    const state = await api(`/networks/${network}`, "GET", undefined, signal).catch(
      absent,
    );
    if (!state) return;
    if (
      !Object.entries(expected).every(([key, value]) => state.Labels?.[key] === value)
    )
      throw Error("browser_worker_ownership_mismatch");
    const connected = Object.entries(state.Containers ?? {}).some(
      ([id, value]: [string, any]) =>
        id.startsWith(options.controllerContainer) ||
        value.Name === options.controllerContainer,
    );
    if (connected)
      await api(
        `/networks/${network}/disconnect`,
        "POST",
        { Container: options.controllerContainer, Force: true },
        signal,
      );
    await api(`/networks/${network}`, "DELETE", undefined, signal).catch(absent);
  }
  async function removeOwned(
    id: string,
    expected: Record<string, string>,
    signal: AbortSignal,
  ) {
    const container = await api(`/containers/${id}/json`, "GET", undefined, signal);
    if (
      !Object.entries(expected).every(
        ([key, value]) => container.Config.Labels?.[key] === value,
      )
    )
      throw Error("browser_worker_ownership_mismatch");
    await api(`/containers/${id}?force=true&v=true`, "DELETE", undefined, signal);
  }
  return {
    async list(signal) {
      const filter = encodeURIComponent(
        JSON.stringify({ label: [`${prefix}installation=${options.installation}`] }),
      );
      const containers = await api(
        `/containers/json?all=true&filters=${filter}`,
        "GET",
        undefined,
        signal,
      );
      if (!Array.isArray(containers)) throw Error("browser_docker_response_invalid");
      const workers: ManagedWorker[] = [];
      try {
        for (const item of containers) {
          const state = await api(
            `/containers/${item.Id}/json`,
            "GET",
            undefined,
            signal,
          );
          const tags = state.Config?.Labels;
          const executionId = tags?.[prefix + "execution"],
            incarnation = tags?.[prefix + "incarnation"],
            network = tags?.[prefix + "network"];
          if (
            tags?.[prefix + "installation"] !== options.installation ||
            !/^[a-f0-9-]{36}$/.test(executionId ?? "") ||
            !/^[a-f0-9-]{36}$/.test(incarnation ?? "") ||
            !network
          )
            throw Error("browser_worker_ownership_mismatch");
          const origin =
            tags[prefix + "origin"] ??
            state.Config.Env?.find((value) =>
              value.startsWith("BROWSER_WORKER_ORIGIN="),
            )?.slice("BROWSER_WORKER_ORIGIN=".length);
          if (!origin || !/^http:\/\/bs-[a-z0-9-]+:8081$/.test(origin))
            throw Error("browser_worker_ownership_mismatch");
          const net = await api(`/networks/${network}`, "GET", undefined, signal);
          if (
            !Object.entries(labels(executionId, incarnation)).every(
              ([key, value]) => net.Labels?.[key] === value,
            )
          )
            throw Error("browser_worker_ownership_mismatch");
          const controller = await api(
            `/containers/${options.controllerContainer}/json`,
            "GET",
            undefined,
            signal,
          );
          if (!net.Containers?.[controller.Id])
            await api(
              `/networks/${network}/connect`,
              "POST",
              { Container: options.controllerContainer },
              signal,
            );
          networks.set(item.Id, network);
          workers.push({
            id: item.Id,
            executionId,
            incarnation,
            origin,
            runtime: createControllerRuntime(origin),
          });
        }
        return workers;
      } catch (error) {
        for (const worker of workers) worker.runtime.close?.();
        throw error;
      }
    },
    async create(placement, signal) {
      const name = `bs-${placement.incarnation}`;
      const expected = labels(
        placement.executionId,
        placement.incarnation,
        placement.profileId,
      );
      let network: string, id: string;
      try {
        network = (
          await api(
            "/networks/create",
            "POST",
            { Name: name, Driver: "bridge", EnableIPv6: false, Labels: expected },
            signal,
          )
        ).Id;
        await api(
          `/networks/${network}/connect`,
          "POST",
          { Container: options.controllerContainer },
          signal,
        );
        const environment = WORKER_ENVIRONMENT.filter(
          (key) => options.environment?.[key] !== undefined,
        ).map((key) => `${key}=${options.environment[key]}`);
        id = (
          await api(
            `/containers/create?name=${name}`,
            "POST",
            {
              Image: options.image,
              Cmd: ["worker"],
              Env: [
                ...environment,
                "CONCURRENT=1",
                "QUEUED=0",
                `BROWSER_EXECUTION_ID=${placement.executionId}`,
                `BROWSER_WORKER_ORIGIN=http://${name}:8081`,
              ],
              Labels: {
                ...expected,
                [prefix + "network"]: network,
                [prefix + "origin"]: `http://${name}:8081`,
                [prefix + "protocol"]: WORKER_PROTOCOL,
              },
              HostConfig: {
                NetworkMode: network,
                CapDrop: ["ALL"],
                CapAdd: [
                  "SETUID",
                  "SETGID",
                  "CHOWN",
                  "KILL",
                  "DAC_OVERRIDE",
                  "SYS_CHROOT",
                  "NET_ADMIN",
                ],
                SecurityOpt: [
                  "no-new-privileges",
                  `seccomp=${JSON.stringify(options.seccomp)}`,
                ],
                ShmSize: options.shmBytes ?? 536870912,
                Memory: options.memoryBytes ?? 2147483648,
                Init: true,
                RestartPolicy: { Name: "no" },
              },
            },
            signal,
          )
        ).Id;
        await api(`/containers/${id}/start`, "POST", undefined, signal);
        const origin = `http://${name}:8081`;
        while (true) {
          signal.throwIfAborted();
          const state = await api(`/containers/${id}/json`, "GET", undefined, signal);
          if (!state.State.Running) throw Error("browser_worker_start_failed");
          try {
            const health = await fetch(`${origin}/health`, {
              signal: AbortSignal.any([signal, AbortSignal.timeout(2000)]),
            });
            if (health.ok) break;
          } catch {
            signal.throwIfAborted();
          }
          await delay(100, undefined, { signal });
        }
        networks.set(id, network);
        return {
          id,
          executionId: placement.executionId,
          incarnation: placement.incarnation,
          origin,
          runtime: createControllerRuntime(origin, {
            automation: placement.automation,
          }),
        };
      } catch (error) {
        const cleanup = AbortSignal.timeout(30000);
        const failures: unknown[] = [];
        try {
          if (id) await removeOwned(id, expected, cleanup);
        } catch (failure) {
          failures.push(failure);
        }
        try {
          if (network) await removeNetwork(network, expected, cleanup);
        } catch (failure) {
          failures.push(failure);
        }
        if (failures.length) throw new WorkerCleanupFailure(error, ...failures);
        throw error;
      }
    },
    async remove(worker: ManagedWorker, signal) {
      const expected = labels(worker.executionId, worker.incarnation);
      const state = await api(
        `/containers/${worker.id}/json`,
        "GET",
        undefined,
        signal,
      ).catch((error) => {
        if (error.statusCode === 404) return undefined;
        throw error;
      });
      const network =
        state?.Config.Labels?.[prefix + "network"] ?? networks.get(worker.id);
      if (!network) {
        if (state) throw Error("browser_worker_ownership_mismatch");
        return;
      }
      networks.set(worker.id, network);
      if (state) await removeOwned(worker.id, expected, signal);
      await removeNetwork(network, expected, signal);
      networks.delete(worker.id);
    },
  };
}

export function createDockerClient(socketPath = "/var/run/docker.sock") {
  return (
    path: string,
    method = "GET",
    body?: any,
    signal = AbortSignal.timeout(30000),
  ): Promise<any> =>
    new Promise((resolve, reject) => {
      const request = httpRequest(
        {
          socketPath,
          path: `/v1.47${path}`,
          method,
          headers: { "content-type": "application/json" },
          signal,
        },
        (response) => {
          const chunks: Buffer[] = [];
          let size = 0;
          response.on("data", (chunk) => {
            size += chunk.length;
            if (size > 1048576) request.destroy();
            else chunks.push(chunk);
          });
          response.on("error", () => reject(Error("browser_docker_unavailable")));
          response.on("end", () => {
            if (response.statusCode < 200 || response.statusCode >= 300) {
              reject(
                Object.assign(Error("browser_docker_operation_failed"), {
                  statusCode: response.statusCode,
                }),
              );
              return;
            }
            try {
              resolve(size ? JSON.parse(Buffer.concat(chunks).toString()) : {});
            } catch {
              reject(Error("browser_docker_response_invalid"));
            }
          });
        },
      );
      request.on("error", () => reject(Error("browser_docker_unavailable")));
      request.end(body ? JSON.stringify(body) : undefined);
    });
}

export async function readDockerSeccomp(
  path = "/opt/browser-sandbox/seccomp.json",
): Promise<object> {
  return JSON.parse(await readFile(path, "utf8"));
}
