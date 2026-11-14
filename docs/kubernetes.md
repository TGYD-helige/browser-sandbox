# Kubernetes backend

The Kubernetes backend runs a single controller and lazily creates one bare worker Pod per execution. Hosts use `createControllerRuntime` with `createBrowserSessions`; agents use the returned controller CDP URL. Worker Pod addresses stay private. Persistent Profiles, restart binding recovery and multi-controller operation are outside this delivery slice.

## Install

Build or select one browser-sandbox image and resolve its immutable manifest digest. The same image runs the controller and all new workers. Provision the image's generated `/opt/browser-sandbox/seccomp.json` as `browser-sandbox.json` in the kubelet seccomp directory on every eligible worker node; this profile is architecture-specific. Set `worker.securityContext.seccompProfile.localhostProfile` if your provisioned filename differs. The chart creates no node installer or worker Deployment. An administrative Job runs only during uninstall, using the same pinned image without browser privileges.

```sh
helm install browser charts/browser-sandbox --namespace browsers --create-namespace \
  --set image.repository=amaster/browser-sandbox \
  --set image.digest="${BROWSER_IMAGE_DIGEST}" \
  --set 'networkPolicy.apiServerCidrs[0]=10.43.0.1/32' \
  --set 'networkPolicy.apiServerCidrs[1]=192.168.1.10/32'
```

Replace the example API addresses with every API endpoint address used by your cluster, including its Service IP, control-plane addresses and public endpoints. Add any public Pod, Service or node ranges to `networkPolicy.deniedCidrs`. A missing API CIDR list fails chart rendering. Verify policy enforcement in the actual CNI before admitting browser workloads; additional NetworkPolicies are additive and can broaden these permissions.

The agent Service defaults to ClusterIP. A separate, always-ClusterIP `RELEASE-browser-host` Service serves trusted host control on `hostControl.port` (default 8082). Only Pods matching `hostControl.allowedPeers` may reach it; by default these are same-namespace Pods labelled `browser-sandbox.io/host-control: "true"`. Apply that label only to trusted host workload templates, and keep untrusted agents in separate Pods. Hosts connect `createControllerRuntime` to this private Service; returned browser endpoints use the agent origin. Namespace writers and label assignment remain trusted. `controller.publicOrigin` defaults to the controller Service's in-cluster HTTP origin; set it to the internal URL reachable by your hosts and agents when using another entry point. Optional `ingress.enabled`, `ingress.host`, `ingress.className` and `ingress.tls` expose only the controller. Host control retains its authentication hook but currently has no credential authentication. Keep the host Service private; the optional Ingress routes only agent traffic.

## Worker configuration

Operators configure one Helm release and one image digest. `runtime` accepts only `TIMEOUT`, `TZ`, `BROWSER_STARTUP_TIMEOUT` and `LANG`; the chart rejects every other key. Controller-owned image, role, identity, credentials and listener settings cannot be overridden through this shared environment. Workers inherit the same allowlisted settings. Controller credentials, backend settings and ServiceAccount tokens are excluded. The controller reads `worker-template.json` from its configuration ConfigMap and projects only these worker settings:

| Value | Worker behavior |
|---|---|
| `worker.resources` | Container requests and limits |
| `worker.securityContext` | Browser-required capabilities and provisioned Localhost seccomp profile; privileged mode and privilege escalation are disabled |
| `worker.shmSize` | Memory-backed `/dev/shm` emptyDir limit |
| `worker.terminationGracePeriodSeconds` | Graceful stop budget; also included in the Pod deadline |
| `worker.nodeSelector`, `worker.tolerations`, `worker.affinity` | Placement constraints |
| `imagePullSecrets` | Image pull references shared with the controller; never mounted into workers |
| `worker.extensions.configMap.name` | Optional read-only extension ConfigMap at `/opt/browser-extensions` |
| `worker.extensions.persistentVolumeClaim.claimName` | Alternative read-only extension PVC at the same path |

Extension ConfigMaps/PVCs must contain only extension assets and support the chosen scheduling topology. No controller volume or Kubernetes token is inherited. Remote clients stage files through the controller's HTTP `/files` interface; a shared local execution directory is unavailable.

Workers use `restartPolicy: Never`, no automatic ServiceAccount token, one Browserless execution, no host ports and no Service. `activeDeadlineSeconds` covers `TIMEOUT`, the creation budget and the finalization grace period. No renewal extends this infrastructure backstop. The controller uses a bounded creation deadline and reports Pending timeout, image-pull failure and worker loss explicitly. It never replays browser commands into another Pod. If a creation request loses its acknowledgement and absence cannot be confirmed safely, cleanup is reported as unconfirmed.

Workers do not request `net.ipv4.ip_local_reserved_ports`: there is no co-resident agent sharing their network namespace. Existing worker UID rules continue to protect loopback Browserless/CDP ports. Browser execution still requires the Linux capabilities and generated seccomp profile; controller Pods run without those privileges.

## Network isolation

The worker NetworkPolicy allows ingress only from this release's controller Pods in the same namespace. Egress permits public IPv4 excluding private, reserved, link-local, operator-supplied cluster ranges and API endpoints, plus TCP/UDP DNS to the configured DNS Pods. Worker-to-worker connections, controller management connections, cloud metadata and the Kubernetes API are denied. IPv6 internet egress is not enabled. This slice does not add private storage egress exceptions.

The controller ServiceAccount has namespace-scoped Pod create/get/list/delete permissions and can read its release owner ConfigMap. The uninstall Job uses a separate ServiceAccount with only Pod get/list/delete, the owner ConfigMap read, and get/patch access to the named controller Deployment scale subresource. Neither role has watch permission; workers and agents receive neither management identity. Treat the controller, node administration, namespace writers and label assignment as trusted. NetworkPolicy does not block traffic originating from the worker's resident node and does not protect against privileged/node access; it is not a replacement for a tenant isolation policy. See [Kubernetes NetworkPolicy limitations](https://kubernetes.io/docs/concepts/services-networking/network-policies/).

## Rollout, drain and uninstall

Worker Pods have a controller ownerReference to a stable release-owned ConfigMap, rather than the controller Pod. A normal controller rollout disconnects clients and closes control transports while preserving already-published workers. Incomplete creations are cancelled. This slice rejects unknown execution references after restart; recovering bindings and retained-page reconnects is delivered separately. Do not interpret Pod survival as complete restart recovery.

```sh
kubectl rollout restart deployment/browser-browser -n browsers
kubectl drain WORKER_NODE --ignore-daemonsets --delete-emptydir-data --timeout=120s
helm uninstall browser -n browsers --wait
```

Drain needs no `--force` because workers have a controller ownerReference. Eviction sends SIGTERM and the worker attempts graceful shutdown. A lost worker is reported explicitly and is not replaced. Enable `worker.disruptionBudget.enabled=true` with integer `worker.disruptionBudget.minAvailable` (default `1`) to retain at least that many live workers during voluntary eviction. Remove the budget or set its minimum to zero when intentional eviction is required. Bare Pods have no scalable workload controller, so the budget uses the [integer minimum supported for arbitrary workloads](https://kubernetes.io/docs/tasks/run-application/configure-pdb/#arbitrary-workloads-and-arbitrary-selectors).

Helm uninstall first runs a bounded `pre-delete` administrative Job. It scales the named controller to zero, waits for controller Pods to exit, validates each worker against the stable owner UID, then deletes workers with Pod UID preconditions and waits for their absence. NetworkPolicy remains installed throughout that drain. Failure blocks uninstall and leaves the controller scaled down; inspect the retained hook Job and retry after resolving cleanup. Do not bypass this protection with `--no-hooks`. The stable release owner remains a garbage-collection backstop. Active temporary browser data is disposable; finalize executions before uninstall if you need their results. This chart does not create a Profile store PVC; durable storage and retained-PVC behavior belong to the persistent-storage slice.

## Supported runtime

Run workers on Linux amd64 or arm64 nodes with the architecture-specific seccomp profile installed. Verify NetworkPolicy enforcement and a real sandboxed renderer on the actual cluster before admitting workloads. Pod separation alone does not establish tenant isolation.
