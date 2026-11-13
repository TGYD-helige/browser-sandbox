# Managed Docker backend

Managed mode is an internal-only development preview. The controller is the only agent-facing endpoint; agent and host authentication are deferred. Do not expose it through a public ingress. The Docker socket grants host-level authority and belongs only in the trusted controller. Verify the applicable Browserless hosted-service license and CloakBrowser binary terms before production or third-party deployment.

## Start the controller

Build an image from a revision containing managed mode, then run one controller:

```sh
docker build -t browser-sandbox:managed .
docker run -d --name browser-controller \
  --cap-drop ALL --security-opt no-new-privileges \
  --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
  -p 127.0.0.1:8080:8080 \
  -e BROWSER_INSTALLATION=example \
  -e BROWSER_CONTROLLER_ORIGIN=http://127.0.0.1:8080 \
  browser-sandbox:managed controller
```

The startup command selects `local` (default), `controller`, or `worker`. Controller startup does not load Browserless, start Chrome, install a browser firewall, or require worker capabilities. The controller creates workers from its own immutable Docker image ID. An explicit `BROWSER_IMAGE` override must be an image digest or immutable image ID. A controller running outside Docker must supply `BROWSER_CONTROLLER_CONTAINER` for a container attached to the worker networks, plus an image and seccomp configuration through `createDockerBackend`.

The controller creates a private bridge per worker, connecting only that worker and the controller. Workers have no published ports, Docker socket, host credentials, or shared Profile volume. Their Chromium runs as the existing internal UID with Chrome sandboxing, Browserless concurrency one, and the bundled seccomp configuration. Worker egress rejects private IPv4 networks, link-local/cloud metadata, IPv6 local networks, and IPv4-mapped IPv6; this also blocks other workers and controller management ports. Reply traffic for controller-initiated connections remains allowed. Docker administrators and the host kernel remain trusted: private networking is not protection from a host administrator. Use separate Docker networks for agents, allow agents to reach only the controller, and do not grant agents Docker access or place them in worker networks.

## Connect a host and agent

```js
import {
  createBrowserSessions,
  createControllerRuntime,
} from '@amaster.ai/browser-sandbox';

const runtime = createControllerRuntime('http://127.0.0.1:8080', {
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

Host-control WebSocket `/host` carries the existing request operations and bounded bidirectional callbacks. `createControllerRuntime` implements `BrowserRuntime`; `attachStorage` transports small `load`/`save`/`commit` messages and `automation` transports pause/resume confirmations. Dropped connections fail pending work explicitly and abort callback signals. Persistent Profile registrations are rejected in this slice; the transport is available for persistent-storage adapters in the next slice. Never replay commands or website writes after a disconnect.

`access_create` returns separate CDP and viewer references, with the requested control/resize flags and an expiry of at most 30 minutes. A fresh access reference resolves the same execution and worker. `access_revoke` terminates active connections and rejects old HTTP, CDP, and viewer references. Request a fresh endpoint/access reference after revocation. Session retain/renew/release and takeover operations remain worker-owned; renewal does not extend the hard browser deadline. Keep the host runtime connection open while automation callbacks are required.

Every protected agent route crosses the controller's `authorizeAccess` function before worker lookup, including the viewer authentication message. Every host connection crosses the separate `authenticateHost` hook. Both hooks are initially no-ops. Workers implement neither hook; their execution check detects misrouting, while Docker network isolation restricts callers.

## Configuration and lifecycle

| Setting | Meaning |
|---|---|
| `BROWSER_INSTALLATION` | Installation label namespace; default `default`; use a distinct value per controller |
| `BROWSER_CONTROLLER_PORT`, `BROWSER_CONTROLLER_HOST` | Listener; defaults `8080`, `0.0.0.0` |
| `BROWSER_CONTROLLER_ORIGIN` | Host/agent-reachable controller HTTP(S) origin; set explicitly for containers |
| `BROWSER_VIEWER_ORIGINS` | Comma-separated allowed embedding/browser origins |
| `BROWSER_DOCKER_SOCKET` | Controller Docker API socket; default `/var/run/docker.sock` |
| `BROWSER_IMAGE` | Optional immutable worker image override; otherwise reuse the controller image |
| `TIMEOUT`, `TZ`, `BROWSER_STARTUP_TIMEOUT`, `LANG` | Explicit allowlist inherited by workers |
| `BROWSER_EXTENSIONS_SOURCE` | Optional Docker-host directory mounted read-only as `/browser-extensions` in workers |

Controller management settings and secrets are never copied into worker environments. The adapter overrides role, execution identity, incarnation labels, private origin, `CONCURRENT=1`, and `QUEUED=0`. `createDockerBackend` accepts worker memory and shared-memory sizes (defaults 2 GiB and 512 MiB). Extensions use the same directory permissions and trust requirements as local extensions.

Finalization revokes access, confirms worker runtime finalization, and removes the exact owned container and network. Unused executions finalize without creating infrastructure. Failed infrastructure deletion retains the binding for an explicit retry; no replacement worker is created automatically. Temporary Profile data is container-local and removed with the worker. The controller currently owns bindings in memory and supports one replica. Restart recovery, abandoned-execution TTL, admission/capacity limits, and orphan reconciliation are subsequent slices. If a Docker controller crashes, workers remain until an operator reclaims them; Browserless's browser timeout does not bound the lifetime of an idle worker container. A normal controller SIGTERM attempts finalization; do not assume an orchestrator's short stop grace permits every worker to finish.

Use `GET /health` on the controller for readiness. Workers expose readiness only on their private listener after the existing real-renderer startup probe. Managed integration CI runs both Docker architectures, records first-browser readiness and direct-versus-proxy measurements, and tests standard CDP, files, session/access renewal, sandbox diagnostics, an existing agent-browser harness, egress denial, and owned cleanup. Measurements describe the test environment; they are not production capacity guarantees.
