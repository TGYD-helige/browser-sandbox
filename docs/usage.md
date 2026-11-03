# Runtime and host integration

## Run the browser

The current deployment is a private Linux container/Pod network namespace with a trusted root controller. The runtime listens on loopback, not a public port. Clients share that network namespace and use distinct execution UIDs. The trusted host owns the 0600 control socket and supplies authorized execution/Profile identities; an untrusted process must never mount that socket.

Use a compatible Linux host with user namespaces enabled. Chrome keeps its renderer sandbox; startup fails if a real page and screenshot cannot run safely. The runtime must not be exposed as a public unauthenticated browser port.

```sh
pnpm docker:build
bash .github/scripts/verify-image.sh browser-sandbox:local
```

The verification script creates temporary containers with the required security options. To keep login data across container replacement, mount /browser-data on reliable private storage. Mount the control socket only into your trusted host process. Do not use host networking or grant execution processes root privileges.

## Create and finish an execution

Build the package before running the examples. The host client uses newline-delimited JSON over BROWSER_CONTROL_SOCKET + .auth; it is not a model tool.

```js
import { randomUUID } from 'node:crypto';
import { hostRequest } from '@amaster.ai/browser-sandbox';

const executionId = randomUUID();
const registration = {
  executionId,
  executorUid: 100001,
  profileId: 'authorized-tenant/browser-profile', // Omit for a temporary Profile.
};
const { wsEndpoint } = await hostRequest(socketPath, {
  op: 'register', ...registration,
});
// Inject only wsEndpoint into a process running as executorUid.
// Stop that execution's workers before finalization.
await hostRequest(socketPath, { op: 'finalize', executionId });
```

Use a fresh UUID for each execution. Execution UIDs are in 100000–1999999999 and must not be recycled while old processes or grants survive. UID 20001 is reserved for the runtime's internal renderer probe and legacy internal test paths; production hosts must explicitly provide an execution UID. These are Linux deployment constraints, not business identities.

| Operation | Semantics |
|---|---|
| register | Bind execution/UID and optional profileId without starting Chrome or calling storage |
| endpoint | Query an existing execution; supply the matching profileId for persistent executions |
| quiesce | Close the current browser generation while retaining registration and Profile lease |
| finalize | Close, optionally save and commit remote data, then finish the execution |
| finish | Low-level cleanup; rejects unconfirmed remote saves |
| takeover | Request a human control operation through the trusted host |

Control requests have a 30-second budget, or 180 seconds for finalization. Browser preparation has a 45-second deadline; clients should allow at least 60 seconds for first use. The runtime allows a roughly one-second transport reconnection grace after the last client disconnects. A client maintaining its connection retains the live browser until Browserless's total timeout. Explicit live-session retention is described below.

Cookie checkpoints preserve persistent Cookies and deletion/expiry behavior. Cold restore excludes Session Cookies, old task windows, and sessionStorage. A normal close flushes native Profile databases. Forced termination and crashes cannot guarantee the last writes were saved.

Finish revokes execution access and deletes temporary data. Its credentialsScrubbed field is false for persistent Profiles: keeping login data is intentional. A previous owner receipt after a runtime restart is reported as browser_execution_recovery_required; an orphan Profile receipt is browser_profile_stop_unconfirmed. Investigate and confirm process exit before recovery. The runtime never automatically steals a Profile from an uncertain owner.

## Connect your client

The shared contract is a complete CDP WebSocket URL, not a port number. The caller must run under the registered execution UID in the supported network topology.

| Consumer | Connection configuration |
|---|---|
| Raw CDP | [raw-cdp.mjs](../examples/raw-cdp.mjs) uses the bounded CDP transport directly |
| Playwright | [playwright.mjs](../examples/playwright.mjs) uses chromium.connectOverCDP(wsEndpoint) |
| Pi browser tools | Configure sessionMode: existing and wsEndpoint in the consuming plugin |
| DSH browser plugin | Configure mode: attach and endpoint with the full WebSocket URL |
| agent-browser CLI | Pass the full endpoint through --cdp |
| Other harnesses | Configure their CDP client or browser MCP integration; the runtime does not own their tools |

Use the full execution-scoped WebSocket endpoint. Check readiness through /health and create pages through the CDP client. Validate each consuming harness before switching its browser configuration.

## Keep Profiles in remote storage

Remote storage is optional. Attach a host storage adapter before first CDP connection, keep the channel alive through finalization, and register the Profile with storage: true. Provide these three host operations:

| Method | Host responsibility |
|---|---|
| load(signal) | Read the latest confirmed reference after Profile exclusivity is acquired; return {seed: null} or a current download grant |
| save(signal) | Return {target} with upload URL, optional headers, a 32-byte hex encryption key, objectKey, and optional wrapped keyMaterial |
| commit(result, signal) | Conditionally publish result.archive against result.previousSha256, then return {confirmedSha} |

Use attachStorage(socketPath, registration, adapter) from the host client. load is deferred until first CDP use; unused finalization invokes none of these methods. finalize waits for save and commit confirmation before releasing the Profile. Failed uploads or commits preserve recoverable data for retry. A slow transfer is serialized only with its own execution, not every execution.

A download seed contains schema: browser-sandbox.profile/v1, browserVersion, url, key, sha256, byteSize, iv, and tag. Keep transfer grants and encryption keys in the trusted host; never pass them to the browser client. Archives are encrypted and verified before restoration. The encrypted archive limit is 256 MiB; restored content is limited to 512 MiB and 100,000 entries. Same-brand forward browser versions are permitted; downgrades are rejected.

Publish references conditionally using previousSha256 so an old execution cannot replace newer saved data. Commit must be idempotent for the same archive digest, because a connection can fail after storage accepted a write. Failed finalization leaves data available for a later retry.

## Human takeover and files

Stop the execution's automation workers first. Then submit takeover with an action containing op: acquire, actorId, leaseId, and automationStopped: true. The gateway fences new commands, waits for in-flight CDP replies, disconnects automation peers, and grants a ten-minute lease with an epoch. An unconfirmed stop fails closed.

Use action op: frame to obtain a screenshot, targetId, origin, and frameRevision. pointer, text, and key consume that revision, require the matching target/origin and lease, and reject frames older than 30 seconds or from a changed document. Coordinates must fit the viewport. release returns control to automation; clients reconnect normally. Expired or uncertain control requires safe lifecycle recovery rather than silently resuming automation.

Authenticated GET/PUT requests to the execution endpoint's /files/name suffix transfer files, with the same kernel UID check as CDP. Convert the ws: endpoint scheme to http:. Filenames are single-level ASCII names, uploads cannot overwrite, and files are limited to 16 MiB. PUT returns the browser-side absolute path for a CDP upload operation; downloads configured by the launcher land in this directory. Browser-owned links and non-regular files are rejected. Failed uploads may leave partial files until execution cleanup; use a new name for a retry. Downloads stream with backpressure and stay tracked until the HTTP response finishes or is cancelled. The runtime admits at most 16 simultaneous file transfers, returns 429 when full, and enforces a 30-second transfer deadline. Finalization aborts active transfers before deleting the directory.

Some CDP clients, including Chrome DevTools MCP, treat loopback endpoints as local browsers and validate upload paths in the client filesystem. When these clients run in a separate container, mount only the execution files directory at the same absolute path in both containers; the client mount can be read-only. Keep Profiles, journals, and host-control sockets private. The integration runner exercises this topology alongside HTTP file staging and retrieval.

## Settings

| Variable | Default / role |
|---|---|
| BROWSER_CONTROL_SOCKET | /run/browser-sandbox/control.sock; trusted control uses the .auth suffix |
| CONCURRENT | 2 Browserless instances |
| QUEUED | 4 queued requests |
| TIMEOUT | 1800000 ms total Browserless timeout |
| HOST / PORT | Fixed internal Browserless binding 127.0.0.1:3001 |

The first release intentionally targets the verified Linux topology. Remote TCP control, arbitrary UID allocation policies, Windows/macOS service mode, and a generic browser management UI are outside this implementation.

## Manage a live browser session

A live session keeps the original pages and their running JavaScript. A persistent Profile keeps login and site data across browser processes. Use session retention when waiting for a QR scan, MFA, or another agent; use a Profile when the browser may restart.

The execution process can call the session endpoint using its own CDP capability and Linux UID. It does not need the host control socket. The SDK exports `sessionRequest`, and the package installs the `browser-session` command, which reads `BROWSER_WS_ENDPOINT`:

```sh
browser-session status
browser-session retain 120000
browser-session renew 120000
browser-session release
browser-session stop
```

```js
import { sessionRequest } from '@amaster.ai/browser-sandbox';
await sessionRequest(wsEndpoint, 'retain', 120000);
const status = await sessionRequest(wsEndpoint);
await sessionRequest(wsEndpoint, 'renew', 120000);
await sessionRequest(wsEndpoint, 'release');
```

`status` reports state, browser generation, connected client count, retention expiry, and hard expiry. It does not launch a browser. `retain` requires a live browser and keeps it after the last CDP client disconnects. Repeated retain is idempotent; only explicit `renew` moves the retention deadline. Both are capped by the browser generation's original Browserless timeout, which cannot be extended by this runtime. Heartbeats are unnecessary. Lost or expired sessions cannot be renewed into a replacement browser.

`release` removes retention; connected clients may continue, and the ordinary disconnect grace applies afterward. `stop` disconnects clients and waits for browser exit, preserving the registered execution and persistent Profile. A later CDP connection starts a new generation. It does not revoke the execution or bypass remote archive confirmation; the trusted host still calls finalize at execution end. Application login and credential renewal remain the host application’s responsibility.

Hosts may also issue `{op: 'session', executionId, action, durationMs}` through hostRequest. HTTP clients use GET for status or POST with `{action, durationMs}` at the CDP endpoint's `/session` suffix after converting ws to http (or wss to https).

## Additional browser controls

Human takeover supports status, acquire, renew, frame, pointer, text, key, and release. Status reveals the epoch only to the matching actor. Acquire confirms target scripts and loading have stopped after the host has stopped automation workers. Repeated acquisition by the same holder is idempotent. An expired lease, or explicit replaceExisting by the same actor, requires another confirmed stop before replacement. Release requires the original target and origin and is idempotent for the released lease; origin changes fail closed.

The host can request `{op: 'takeover', executionId, action: {op: 'readback', url}}` while automation owns the browser. It stops concurrent automation, reads bounded JSON through the page's authenticated HTTPS origin in a fresh isolated world, and returns text, targetId, URL, and capture time. Redirects, embedded URL credentials, cross-origin requests, page-generation changes, and non-JSON responses fail. This is a generic browser operation; application login verification stays in the host.

An optional allowedDomains list at registration restricts Chrome's hostname resolution using exact hosts or `*.example.com` patterns. The policy is fixed for that execution. This resolver restriction does not replace a complete egress firewall, host network isolation, or client navigation rules.

## Run the client integration checks

Runtime CI checks Biome, workflow formatting, TypeScript, and Linux tests on Node 22 and 24. Browser integration builds sandboxed AMD64 and ARM64 images and exercises the published Pi browser extension, DSH browser registry plus Chrome DevTools MCP provider, OpenCode CLI with Chrome DevTools MCP, and the agent-browser CLI/daemon against the runtime. Client checks verify a real browser form submission and live-page handoff rather than just a successful connection.

```sh
pnpm build
pnpm docker:build
docker build -f Dockerfile.e2e -t browser-sandbox-e2e:local .
node .github/scripts/e2e.mjs pi
node .github/scripts/e2e.mjs dsh
node .github/scripts/e2e.mjs opencode
node .github/scripts/e2e.mjs agent-browser
```

OpenCode runs its actual CLI and MCP tool loop in both modes. The secret-free tools check uses scripted OpenAI-compatible responses; the model check uses the configured provider. Both verify a zero-client handoff, renewal of the original browser generation, and form submission. Its test-only session MCP server exposes the same runtime session API used by the other harnesses.

Each harness verifies real file upload and download bytes. The agent-browser job also checks screenshots, daemon reuse across CLI calls, daemon exit, and restart; it has no model step.

The model jobs use `OPENAI_BASE_URL`, `OPENAI_API_KEY`, and `OPENAI_MODEL_NAME`. Store the API key in a repository Actions secret; base URL and model name can be Actions variables or secrets. Model tests run on same-repository pull requests, master pushes, or a manual Browser integration dispatch with run_model enabled. Fork pull requests run without model secrets. Missing model configuration fails the model job explicitly. Local model runs use the same environment variables with `E2E_MODE=model`.

For local plugin-contract checks against an installed Chrome, install the separately locked test dependencies with `pnpm --dir .github/scripts/e2e install --frozen-lockfile`, then use Node 24 to run `CHROME_EXECUTABLE=/absolute/path/to/chrome node .github/scripts/e2e-local.mjs`. This tests the current gateway and real Pi/DSH/OpenCode clients; it does not validate the Browserless container or Linux isolation.


## Measure real browser lifecycle costs

Image CI publishes `browser-performance-<runner>` JSON artifacts for AMD64 and ARM64. Each workload has three warmups and 20 measured samples: a temporary browser, a small encrypted remote Profile, and an 8 MiB IndexedDB Profile. The runner verifies restored data and real screenshots. It reports median/P95 and counts for registration, first browser readiness, finalization, and streaming archive HTTP transfers, together with the browser, Node, CPU, architecture, and container limits.

First readiness includes the CDP client, archive restoration, Chrome startup, navigation, and screenshot. Finalization includes checkpoint, browser exit, archive/encryption, upload, and reference commit. Storage runs over local HTTP in the test namespace; these measurements do not establish production storage latency, an optimization speedup, or an agreed production performance threshold. `scripts/benchmark.mjs` remains a separate synthetic gateway diagnostic.

To collect the image benchmark locally, build the image and run `BENCHMARK_OUTPUT=/tmp/browser-performance.json bash .github/scripts/verify-image.sh browser-sandbox:local`. Set `BENCHMARK_SAMPLES` between 10 and 100 to change the measured sample count.
