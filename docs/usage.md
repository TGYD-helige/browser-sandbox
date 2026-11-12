# Runtime and host integration

## Run the browser

The runtime image contains CloakBrowser Chromium as its only browser. Browserless still manages browser startup, concurrency, queueing, and total session timeouts. Its pinned application, Node runtime, system dependencies, fonts, and license notices remain included.

The current deployment is a private Linux container/Pod network namespace with a trusted root controller. The runtime listens on loopback, not a public port. Clients share that network namespace and use distinct execution UIDs. The trusted host owns the 0600 control socket and supplies authorized execution/Profile identities; an untrusted process must never mount that socket.

Use a compatible Linux host with user namespaces enabled. Chrome keeps its renderer sandbox; startup fails if a real page and screenshot cannot run safely. The runtime must not be exposed as a public unauthenticated browser port.

Reserve the internal ports from automatic allocation in the shared network namespace: pass `--sysctl net.ipv4.ip_local_reserved_ports=3001,42000-42999` to the runtime container, or configure the equivalent Pod sysctl while preserving any other host-reserved ranges. The firewall blocks execution UIDs from these ports; without this reservation, a client-side MCP or Session worker using a random listener can intermittently select a blocked port. The reservation preserves the firewall and does not authorize direct internal CDP access.

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

Use a fresh UUID for each execution. Every host-control registration must supply an executorUid in 100000–1999999999; requests without one are rejected with browser_executor_identity_invalid, because the endpoint is bound to the kernel peer UID. Do not recycle a UID while old processes or grants survive. To let several harnesses or sessions share one runtime, give each session its own executionId and executorUid; each then gets an independent browser, endpoint, and Profile lease. UID 20001 is reserved for the runtime's internal renderer probe; in-process createGateway().command registrations without executorUid also use it and skip the peer check, so use them only for trusted local tooling. These are Linux deployment constraints, not business identities.

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

Remote storage is optional. Attach a host storage adapter before first CDP connection, keep the channel alive through finalization, and register the Profile with storage: true. Control requests that pass an inline archive grant are rejected with browser_profile_binding_invalid. Provide these three host operations:

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

Some CDP clients, including Chrome DevTools MCP, treat loopback endpoints as local browsers and validate upload paths in the client filesystem. When these clients run in a separate container, mount only the execution files directory at the same absolute path in both containers; the client mount can be read-only. Keep Profiles, journals, and host-control sockets private. The integration runner exercises this topology alongside HTTP file staging and retrieval. Published clients that expose only the MCP default temporary-directory root use a host-provided alias under /tmp; the browser and client see the same execution files there. The alias does not expose Profiles or journals.

## Settings

| Variable | Default / role |
|---|---|
| BROWSER_CONTROL_SOCKET | /run/browser-sandbox/control.sock; the runtime listens only on this path with the .auth suffix, mode 0600 |
| CONCURRENT | 2 Browserless instances |
| QUEUED | 4 queued requests |
| TIMEOUT | 1800000 ms total Browserless timeout |
| BROWSER_EXTENSIONS_DIR | Optional absolute directory containing unpacked extension subdirectories |
| HOST / PORT | Fixed internal Browserless binding 127.0.0.1:3001 |

Profile preparation (including acquisition and restore), archive saves, and ownership release share a separate work limit of CONCURRENT active operations. Preparation and saves admit twice CONCURRENT waiting operations; ownership cleanup has a reserved queue bounded by the 1,000 live execution slots and runs before waiting preparation. This keeps failure recovery and shutdown available when the ordinary queue is full. QUEUED still controls Browserless browser admission. A full Profile work queue returns browser_profile_work_queue_full; it does not call storage or start a browser for the rejected attempt. Cancellation removes queued work, and failed saves retain the Profile for an explicit lifecycle retry. Registration and unused finalization do not enter this queue.

The gateway accepts the execution WebSocket before cold startup completes. The bundled CDP client allows 60 seconds for commands sent before the first reply, covering the default 45-second startup budget plus the ordinary 15-second command budget. Commands sent after that reply keep the 15-second timeout. Other CDP clients must allow for preparation in their initial command deadline; a successful WebSocket handshake alone does not mean Chrome is ready.

The browser runtime targets the supported Linux topology. Optional authenticated remote access and a lightweight live viewer are described in [live viewing and deployment](./live-view.md). The private host-control socket remains local; native Windows/macOS runtime mode and a general browser-management UI are outside this implementation.

## Load local Chrome extensions

Mount a host directory read-only and set `BROWSER_EXTENSIONS_DIR` to its container path. Each immediate subdirectory must contain an unpacked extension with a readable `manifest.json`; loose files are ignored. An empty directory loads no extensions. For example:

```text
extensions/
  my-extension/
    manifest.json
    content.js
  another-extension/
    manifest.json
    background.js
```

Add these options to your existing sandboxed Docker launch, keeping the security options described above:

```sh
--mount "type=bind,src=$(pwd)/extensions,dst=/browser-extensions,readonly" \
--env BROWSER_EXTENSIONS_DIR=/browser-extensions
```

The equivalent additions to a Compose service are:

```yaml
environment:
  BROWSER_EXTENSIONS_DIR: /browser-extensions
volumes:
  - ./extensions:/browser-extensions:ro
```

The runtime scans the directory on each browser launch, including cold restarts using a persistent Profile. Add or remove extension directories on the host before starting the next browser; existing browsers keep their loaded extensions. Stage new extensions outside the mounted directory, then move complete directories into it. Do not edit extension files while a browser is using them. Extensions that are disabled or incompatible with the pinned Chromium build require diagnosis in Chrome; manifest presence alone does not guarantee activation.

Only the trusted deployment operator configures this directory. Extensions run in every execution browser, including the startup probe, and their declared permissions may grant access to page content and login data. Mount trusted extension code only, keep it unwritable by executions, and make directories traversable and files readable by all execution UIDs (typically modes 0755 and 0644). Do not place credentials or private configuration in this shared directory. Extension state belongs to each browser's Profile; temporary Profiles discard it and persistent Profiles retain it under the existing persistence rules.

The directory must be absolute and contain at most 32 extension subdirectories. Commas, control characters, and symlink entries are rejected because Chrome receives a comma-separated path list. An unreadable directory or missing manifest fails browser startup. This loads unpacked extensions in headless CloakBrowser Chromium; it does not download CRX files or install from Chrome Web Store.

The encoded Browserless launch request target must fit within 12 KiB, leaving space for HTTP headers under its 16 KiB parser limit. The budget includes all launch options, including both copies of the extension paths. Long paths and non-ASCII names consume more of this budget. An oversized configuration fails before opening a connection with `browser_extensions_launch_request_too_large`; shorten directory names or the mounted container path, or load fewer extensions.

Overlapping launches share one in-progress directory scan. Cancelling one launch does not cancel other readers; when all readers cancel, their startup and Profile cleanup proceed without waiting for a stalled deployment mount. At most one underlying scan remains outstanding. New launches return `browser_extensions_scan_busy` until that abandoned scan finishes closing its directory handle. Late results cannot start a browser, and the next scan reads the directory again.

Run `VERIFY_EXTENSIONS=1 bash .github/scripts/verify-image.sh browser-sandbox:local` to check that a mounted Manifest V3 content script runs in real pages under two execution UIDs. Image CI runs this check on AMD64 and ARM64; the ordinary performance benchmark keeps extensions disabled.

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

## Runtime admission and cancellation limits

A runtime retains up to 65,536 finished execution identities to fence delayed registrations. After reaching that budget it rejects new identities with `browser_execution_capacity_exceeded`, while existing registrations and their cleanup remain available. It never evicts a tombstone to admit a new execution. Hosts should drain a runtime before that budget is exhausted, confirm browser shutdown, and replace it through their normal deployment lifecycle. Continue to use fresh execution UUIDs and fence old host requests across replacement; tombstones are process-local, not a durable authorization database.

Archive hashing, streams, directory traversal, window cleanup, and the default browser-version subprocess observe lifecycle cancellation. An already-issued Profile filesystem operation must finish before ownership is released, so a stalled kernel or filesystem can still delay cleanup. Cancelled saves retain the recoverable Profile, remove intermediate archive files, and can be retried without replaying website operations. Cancelled restoration never reports successful preparation or starts Chrome afterward.


## PR labels and release notes

The PR Labeler workflow synchronizes `area/*`, `type/*`, and `size:*` labels for PRs targeting `master`. Areas follow changed paths, types follow Conventional Commit titles, and size excludes lockfiles, snapshots, and generated `dist/` files. Other labels, including manually assigned priorities, are preserved. Maintainers can use the workflow's `pr` input to relabel one PR number or `all` open PRs. Fork PRs are supported without executing their code.

Manual dispatch of Build browser images runs on `master` and accepts an existing Git tag reachable from that workflow commit. It builds and verifies the tag's exact source commit on GitHub-hosted AMD64 and ARM64 runners, then transfers the verified image archives through one-day Actions Artifacts. Registry uploads and multi-architecture publication run on the self-hosted runner selected by `BROWSER_IMAGE_PUBLISH_RUNNER` (default: `arc-simple`). That runner needs Docker CLI for registry login, Skopeo, crane, jq, and standard Linux checksum/timeout utilities; it does not need a Docker daemon or ARM64 execution support. Allow the repository to use the selected runner group before publishing. Archives are checked for integrity and architecture before upload. Version and `sha-*` tags identify the source commit rather than the workflow commit; `latest` is only updated when `publish_latest` is explicitly enabled. Existing source tags are not moved when the publication workflow changes.

Pushing a stable version tag such as `v0.1.2` triggers Build browser images. The tag must point to a commit reachable from `master`; both lightweight and annotated tags are supported. The workflow builds and verifies Linux AMD64/ARM64 images, publishes the versioned images, then publishes a public GitHub Release. Tag-triggered builds do not update the image's `latest` tag. Existing tags do not trigger runs retroactively; use manual dispatch to retry them. Tags pushed using a workflow's `GITHUB_TOKEN` do not trigger another workflow; use manual dispatch or an authorized app or personal token when automating tag pushes.

Both image publication workflows publish public Releases after successful image publication. Release notes contain a model-generated English highlights section followed by GitHub's original change list. On rerun, Build browser images publishes an existing draft while preserving its notes, and leaves an already published Release unchanged. Release browser image retains its immutable-tag checks.

Release summaries reuse the model configuration used by browser integration: `OPENAI_BASE_URL` (an OpenAI-compatible HTTP(S) API base URL, including any `/v1` prefix), `OPENAI_API_KEY`, and `OPENAI_MODEL_NAME`. Store the API key as a repository Actions secret; the URL and model name may be repository variables or secrets, with variables taking precedence. The request uses `/chat/completions` and the configured model. GitHub-generated PR notes are sent to this endpoint, so choose a provider authorized to process repository metadata. The summary generator receives credentials only in release jobs. Missing configuration, model errors, and empty model responses fall back to the GitHub change list. Failure to generate the GitHub source notes fails the release-notes step; images already published remain available.
