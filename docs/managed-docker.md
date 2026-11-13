# Managed Docker backend

Managed mode is an internal-only development preview. The controller is the only agent-facing endpoint; agent and host authentication are deferred. Do not expose it through a public ingress. The Docker socket grants host-level authority and belongs only in the trusted controller. Verify the applicable Browserless hosted-service license and CloakBrowser binary terms before production or third-party deployment.

## Start the controller

Build an image from a revision containing managed mode, then run one controller:

```sh
docker build -t browser-sandbox:managed .
docker run -d --name browser-controller \
  --cap-drop ALL --security-opt no-new-privileges \
  --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
  --mount type=volume,src=browser-controller-data,dst=/controller-data \
  -p 127.0.0.1:8080:8080 \
  -e BROWSER_INSTALLATION=example \
  -e BROWSER_CONTROLLER_ORIGIN=http://127.0.0.1:8080 \
  browser-sandbox:managed controller
```

Keep the private `browser-controller-data` volume and `BROWSER_INSTALLATION` value across controller replacement; the volume holds the execution journal and may also hold Profile archives.

The startup command selects `local` (default), `controller`, or `worker`. Controller startup does not load Browserless, start Chrome, install a browser firewall, or require worker capabilities. The controller creates workers from its own immutable Docker image ID. An explicit `BROWSER_IMAGE` override must be an image digest or immutable image ID. This Docker adapter supports a containerized controller, which resolves worker names on the private Docker networks. Set `BROWSER_CONTROLLER_CONTAINER` to its Docker ID or name if the container hostname does not identify it.

The controller creates a private bridge per worker, connecting only that worker and the controller. Workers have no published ports, Docker socket, host credentials, or shared Profile volume. Their Chromium runs as the existing internal UID with Chrome sandboxing, Browserless concurrency one, and the bundled seccomp configuration. Worker egress rejects private IPv4 networks, link-local/cloud metadata, IPv6 local networks, and IPv4-mapped IPv6; this also blocks other workers and controller management ports. Reply traffic for controller-initiated connections remains allowed. Docker administrators and the host kernel remain trusted: private networking is not protection from a host administrator. Use separate Docker networks for agents, allow agents to reach only the controller, and do not grant agents Docker access or place them in worker networks.

## Connect a host and agent

The agent listener is port 8080. Host-control `/host` is served separately on loopback port 8082 by default; the agent listener rejects that route. Run the trusted host adapter in the controller network namespace (for example a trusted adapter container using `--network container:browser-controller`), or bind host control to a dedicated management interface protected from agent and browser networks. Publish only the agent port to agents. The host authentication hook remains available on the separate listener; authentication is still deferred.

```js
import {
  createBrowserSessions,
  createControllerRuntime,
} from '@amaster.ai/browser-sandbox';

const runtime = createControllerRuntime('http://127.0.0.1:8082', {
  automation: {
    async pause(executionId, signal) {
      // Resolve only after this execution's automation has stopped.
      await host.pause(executionId, signal);
    },
    async resume(executionId, signal) {
      await host.resume(executionId, signal);
    },
  },
});
const sessions = createBrowserSessions({
  provider: {
    identity: 'managed',
    async acquire() { return runtime; },
    async release() {},
  },
});
const session = await sessions.open();
// Pass session.wsEndpoint to the agent's standard CDP client.
// Managed handles have no executorUid; local handles keep their Linux UID.
try {
  await runAgent(session.wsEndpoint);
} finally {
  await session.finalize();
  runtime.close();
}
```

`register` and `endpoint` return opaque controller URLs shaped as `/execution/<reference>`. Registration and unused session status queries do not create a worker. First CDP, file, or viewer use allocates one worker; HTTP `/session`, `/files/<name>`, and `/auth` use the same controller reference. `browser-session` and `sessionRequest` derive their HTTP route from the returned endpoint as in local mode. CDP clients that require a client-side shared files mount must instead stage files through HTTP `/files`; remote workers do not share the client's filesystem.

Host-control WebSocket `/host` on the separate management listener carries the existing request operations and bounded bidirectional callbacks. `createControllerRuntime` implements `BrowserRuntime`; `attachStorage` transports small `load`/`save`/`commit` messages and `automation` transports pause/resume confirmations. Dropped connections fail pending work explicitly and abort callback signals. Persistent registrations select an attached host storage adapter, or the controller volume store when configured; see [persistent Profile storage](./profile-store.md). A Profile without either fails explicitly. The controller relays only storage control messages; workers transfer host archives directly. Never replay commands or website writes after a disconnect.

`access_create` returns separate CDP and viewer references, with the requested control/resize flags and an expiry of at most 30 minutes. A fresh access reference resolves the same execution and worker. `access_revoke` terminates active connections and rejects old HTTP, CDP, and viewer references. Request a fresh endpoint/access reference after revocation. Session retain/renew/release and takeover operations remain worker-owned; renewal does not extend the hard browser deadline. Keep the host runtime connection open while automation callbacks are required.

Every protected agent route crosses the controller's `authorizeAccess` function before worker lookup, including the viewer authentication message. Every host connection crosses the separate `authenticateHost` hook. Both hooks are initially no-ops. Workers implement neither hook; their execution check detects misrouting, while Docker network isolation restricts callers.

## Configuration and lifecycle

| Setting | Meaning |
|---|---|
| `BROWSER_INSTALLATION` | Installation label namespace; default `default`; use a distinct value per controller |
| `BROWSER_CONTROLLER_PORT`, `BROWSER_CONTROLLER_HOST` | Listener; defaults `8080`, `0.0.0.0` |
| `BROWSER_HOST_CONTROL_PORT`, `BROWSER_HOST_CONTROL_HOST` | Separate trusted-host listener; defaults `8082`, `127.0.0.1`; do not publish it to agents |
| `BROWSER_CONTROLLER_ORIGIN` | Host/agent-reachable controller HTTP(S) origin; set explicitly for containers |
| `BROWSER_VIEWER_ORIGINS` | Comma-separated allowed embedding/browser origins |
| `BROWSER_DOCKER_SOCKET` | Controller Docker API socket; default `/var/run/docker.sock` |
| `BROWSER_IMAGE` | Optional immutable worker image override; otherwise reuse the controller image |
| `TIMEOUT`, `TZ`, `BROWSER_STARTUP_TIMEOUT`, `LANG` | Explicit allowlist inherited by workers |
| `BROWSER_EXTENSIONS_SOURCE` | Optional Docker-host directory mounted read-only as `/browser-extensions` in workers |

Controller management settings and secrets are never copied into worker environments. Browser UID traffic to the worker loopback control listener is also blocked. The adapter overrides role, execution identity, incarnation labels, private origin, `CONCURRENT=1`, and `QUEUED=0`. `createDockerBackend` accepts worker memory and shared-memory sizes (defaults 2 GiB and 512 MiB). Extensions use the same directory permissions and trust requirements as local extensions.

Finalization revokes access, confirms worker runtime finalization, and removes the exact owned container and network. Unused executions finalize without creating infrastructure. Terminal worker-control transport failure force-removes the exact temporary worker and reports `browser_worker_finalization_failed`; failed infrastructure deletion retains the binding for an explicit retry. Worker creation belongs to all current waiters; one disconnect does not cancel other waiters, and the final waiter leaving cancels an unfinished attempt. A failed or cancelled attempt is cleared so later requests can retry after confirmed cleanup. An unconfirmed cleanup failure keeps its binding fenced and finalization reports the failure until explicit recovery; it is not treated as an unused execution. A published running worker is never transparently replaced. Cleanup errors preserve the original operation failure and close the runtime channel even when infrastructure deletion fails. Temporary Profile data is container-local and removed with the worker. The controller currently owns bindings in memory and supports one replica. The in-memory finished-identity window retains the most recent 65,536 finalized executions and evicts older identities. Active registrations have a separate limit; normal finalization does not exhaust a lifetime registration budget. Durable fencing across restarts belongs to the recovery slice. Restart recovery, abandoned-execution TTL, admission/capacity limits, and orphan reconciliation are subsequent slices. If a Docker controller crashes, workers remain until an operator reclaims them; Browserless's browser timeout does not bound the lifetime of an idle worker container. A normal controller SIGTERM attempts finalization; do not assume an orchestrator's short stop grace permits every worker to finish.

Persistent worker finalization failures preserve recovery data and the Profile lease; they do not force-remove the worker. Profile leases recover from worker labels before persistent admission, including stopped resources awaiting cleanup. Unknown discovery stays fenced. Live execution binding recovery remains separate.

Use `GET /health` on the controller for readiness. Workers expose readiness only on their private listener after the existing real-renderer startup probe. Managed integration CI runs both Docker architectures, records first-browser readiness and direct-versus-proxy measurements, and tests standard CDP, files, session/access renewal, sandbox diagnostics, an existing agent-browser harness, egress denial, and owned cleanup. Measurements describe the test environment; they are not production capacity guarantees.

## Persistent Profile volume

Mount a controller-only Docker volume at `/profile-store` and set `BROWSER_PROFILE_STORE_ROOT=/profile-store/profiles`. Set `BROWSER_PROFILE_STORE_ORIGIN=http://<controller-container-name>:8083` to an address resolvable from each worker bridge. Port 8083 is a private archive listener and must not be published to agents. The worker derives the exact storage destination from this origin and grants access only to its trusted runtime UID. Public host-issued storage URLs keep working directly; private host storage endpoints require exact IP and port entries in `BROWSER_STORAGE_EGRESS`.

Do not mount this volume in workers or remove it when an execution finishes. Archive encryption keys live in the volume metadata. See [store limits, deletion, idle expiry and recovery](./profile-store.md) before configuring persistence. A controller crash preserves the last confirmed archive and leaves existing worker Profile leases occupied until their exact worker incarnation is confirmed cleaned up; it does not silently redirect browser commands to a new worker.
