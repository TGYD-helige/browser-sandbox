# Live browser viewing and human control

The optional access listener serves a standalone viewer and standard CDP for local or remote clients. The browser still runs in the supported Linux Browserless/CloakBrowser runtime. Browserless tokens, Profile directories, and the trusted host socket stay private. No Kubernetes service, remote storage, frontend framework, or separate access process is required for a single instance.

## Enable access

Set BROWSER_ACCESS_PORT to enable the listener. BROWSER_ACCESS_HOST defaults to 127.0.0.1. For a published container port, set it to 0.0.0.0 and set BROWSER_ACCESS_ORIGIN to the exact externally visible origin, such as https://browser.example.com. Only loopback public origins may use HTTP; remote access requires a TLS reverse proxy. The listener itself speaks HTTP. Never expose the private 9222 gateway or Chrome ports.

The trusted host authorizes an existing execution and issues separate automation and viewer capabilities:

```js
const access = await hostRequest(socketPath, {
  op: 'access_create', executionId, control: true, resize: true, durationMs: 600_000,
});
// access: { executionId, cdpUrl, viewerUrl, expiresAt }
```

Give cdpUrl to the agent's standard CDP client and viewerUrl to the authorized user. Session and file operations use the existing /session and /files/name suffixes on cdpUrl. Access lasts ten minutes by default and at most thirty minutes. Issuing access or opening a viewer does not launch a browser or restore a Profile. Observers do not count as automation clients or retain a browser; use explicit session retention when waiting for a human.

Both URLs are credentials. Do not put them in application logs, tickets, analytics, or public pages. Disable or redact proxy access logging for execution capability paths. The viewer secret is in the URL fragment, is removed from the address bar on load, and authenticates in the first WebSocket message. It never appears in the viewer HTTP request. A viewer grant allows observation; control is separately enabled at issuance. Only one connection may use a control-enabled viewer grant at a time; a read-only grant permits four. Create separate grants for separate viewers.

The host can revoke all remote grants for the execution with access_revoke. Expiry, revocation, quiesce/finalization, and shutdown close existing authorized connections as well as rejecting new ones. A new grant does not resume an old command. Runtime restarts lose grants and require explicit recovery. Pre-start grants bind to the first browser generation. Browser retirement or replacement revokes those grants and disconnects their clients; obtain fresh access for recovery.

## Human takeover

Without a harness integration, control stays disabled until the trusted host stops the execution's browser workers and confirms the stop:

```js
// Retain the live browser before disconnecting workers, within its existing hard deadline.
await hostRequest(socketPath, { op: 'session', executionId, action: 'retain' });
// Stop the actual browser workers next; disconnecting a CDP client is insufficient.
await hostRequest(socketPath, {
  op: 'access_confirm_stopped', executionId, automationStopped: true,
});
```

This confirmation is consumed on acquisition. The browser runtime then fences automation, waits for in-flight commands, and grants its existing ten-minute lease. The web client cannot supply its own actor identity or assert a stop. Do not share the host socket with agents. The host must preserve the paused state until explicit handback, including if the viewer disconnects.

When embedding createGateway, provide access.automation with pause(executionId, signal) and resume(executionId, signal) callbacks. Pause must finish only when browser workers have stopped and cannot send more commands. Honor cancellation; a late pause result must not grant control. Resume must reconnect and inspect the actual page rather than replaying the previous operation. A failed resume is reported separately from a successful control release.

Open the session menu at the right of the address bar and choose Take over browser, click a remote input, and type or paste directly into the page. Chinese IME composition is committed locally before insertion. Input waiting for a previous acknowledgement is bounded and consecutive text is batched; unsubmitted input is discarded on errors, disconnect, or document changes. Alternative text input remains available in a collapsed section. Pointer clicks, scrolling, selection, common navigation/editing keys, and address-bar navigation/back/forward/reload are supported. Escape is forwarded to the page and returns local focus to the address bar, so keyboard focus is not trapped. Human input uses single-use screenshot revisions; the displayed interactive frame is refreshed after each action, at one-second intervals while the controller is idle, or through Refresh view. Inputs are briefly disabled during each refresh so the displayed frame and accepted revision agree. Continuous observation and interactive screenshots have separate revisions. Stale input is rejected, not retried. This is page viewing, not a desktop: native dialogs, audio, drag, and OS shortcuts are outside this implementation.

For a login redirect or a different tab, select the destination and refresh the view before Return to AI in the session menu. Handback explicitly confirms the displayed page and origin using a fresh frame, then releases the lease. Existing strict takeover callers remain unchanged. If the acquired page closed, choose another authorized page before handback. An expired lease or uncertain stop requires explicit host recovery; it never resumes automation silently. Input response loss requires a fresh view and human judgment because the action might already have executed.

## Embed the Web Component

The component fits a host-supplied width and height and has no desktop-shell dependency. Use it in a website, sidebar, modal dialog, or desktop webview. The same compact tab strip and address bar are used in standalone and embedded views; session control and alternative text input live in the address-bar menu. Serve the bundled viewer JavaScript and stylesheet from your frontend origin. Import viewer.js to register browser-viewer, mount the element, and connect after it is attached:

```js
import '@amaster.ai/browser-sandbox/viewer';
import '@amaster.ai/browser-sandbox/viewer.css';

const viewer = document.createElement('browser-viewer');
container.append(viewer);
viewer.connect({ endpoint: 'wss://browser.example.com/view/socket', token });
viewer.addEventListener('browser-viewer-state', ({ detail }) => {
  // detail.connected, detail.control, detail.browser, detail.leasedByYou, detail.leaseExpiresAt
});
// viewer.disconnect() or removing the element closes its connection.
```

Give container an explicit height (for example, 600px or the available flex height). A website can put the same element inside a native dialog, call showModal(), and disconnect it on the dialog's close event. Reconnect with an authorized grant when reopening. The component does not open windows, navigate the host page, or install global keyboard handlers. Pointer coordinates follow the rendered image rather than assuming the popup matches the remote viewport.

The host obtains token from an authorized viewerUrl fragment. The socket permits the configured public origin by default. For a component served by another frontend, explicitly list its exact HTTPS origin in access.viewerOrigins (or the comma-separated BROWSER_VIEWER_ORIGINS environment variable). This allowlist also controls CSP frame-ancestors for embedding the standalone page; foreign origins are rejected by default. The stylesheet scopes component rules to .browser-viewer so it can be loaded by a host application. Do not forward page content as HTML; titles and URLs are rendered as text. Display URLs omit userinfo, query parameters, and fragments to keep callback credentials out of viewer metadata. browser-viewer-result events expose sanitized operation results without the lease or credential. Host applications should supply their own accessible description of the task; a streamed page image does not expose the remote document's accessibility tree.

## Await user authentication without manual handback

Install [browser-sandbox-auth](../skills/browser-sandbox-auth/SKILL.md) in the harness's supported skill directory to teach its model when and how to call this API using existing code execution. The repository exposes it through .agents/skills/browser-sandbox-auth, and the package includes the portable skill under skills/browser-sandbox-auth. Reload skill discovery according to the harness. The skill includes a standalone scripts/auth.mjs that reads non-secret request metadata from stdin and the endpoint from BROWSER_WS_ENDPOINT; it needs Node.js 22.16 or newer, an existing code execution tool, and host automation coordination, with no SDK installation or additional model tool.

Use browserAuthRequest for a specific sign-in step instead of giving the user a general browser lease. The agent inspects the live page, identifies the target and visible main-frame controls, and awaits this request. A control-enabled viewer displays a local sign-in dialog with the exact destination origin. Credential values travel from that dialog over the authenticated viewer socket into a private CDP connection; the awaited result and viewer events contain only non-secret status. This is a runtime API for harness integrations, not an automatically registered model tool.

```js
import { browserAuthRequest } from '@amaster.ai/browser-sandbox';

const result = await browserAuthRequest(cdpUrl, {
  targetId, // The actual page target found through standard CDP.
  origin: 'https://accounts.example.com',
  fields: [
    { id: 'email', label: 'Email', type: 'email', selector: '#email' },
    { id: 'password', label: 'Password', type: 'password', selector: '#password' },
  ],
  submit: { selector: 'button[type="submit"]' },
  timeoutMs: 300_000,
}, abortSignal);
// result: { requestId, status, controlReleased, resumed, selectedOption? }
// Reconnect the CDP client after release and inspect a fresh page snapshot.
```

The same endpoint works on the private UID-authenticated execution address and on an issued remote cdpUrl; HTTP clients may POST the request JSON to its /auth suffix. No credential values belong in the request JSON supplied by the agent. The host must provide access.automation.pause/resume callbacks or stop the actual browser workers and issue access_confirm_stopped before requesting authentication. One confirmation is consumed once across every grant and authentication request for the execution. The request itself retains an already-live browser while waiting, capped by existing retention and hard deadlines; it never starts an unused browser. If it still owns retention, completion leaves at most thirty seconds for reconnection. Existing retention and newer host retention deadlines are preserved.

The runtime fences standard automation connections before accepting input. When the user submits or cancels, the private input connection closes before the control fence is released, including when submission navigates to a different origin. With coordinator callbacks, resume unparks the harness; otherwise the awaiting caller reconnects after checking controlReleased. No menu handback is needed for this scoped flow. A failed pause grants no input; an unconfirmed release keeps automation fenced. A lost HTTP caller cancels the request without automatically resuming its harness. Honor callback cancellation so timed-out work cannot later restart automation.

submitted means that the selected fields were filled and the optional submit control was clicked; it does not mean login succeeded. The agent must inspect fresh page evidence for a success page, validation error, MFA challenge, or another required step. Repeat the request with current controls for each step. Omit submit for a fill-only step. To ask the user to choose a visible login method, use fields: [] with options: [{ id: 'email', label: 'Use email', selector: '#email-method' }]; choices and credential fields are mutually exclusive. The chosen option ID is returned without field values. All fields are required unless required: false is specified.

Other statuses are declined (user cancellation), cancelled (caller/execution cancellation), expired, page_changed, origin_changed, locator_invalid, submission_failed, and unavailable. controlReleased reports runtime fencing separately from resumed, which reports the host callback. Never infer success or replay a submission from a timeout or connection failure. Only one authentication request may wait per execution. Acquire and release/resume transitions share a per-execution lock, including the underlying host callback; a competing transition is rejected until it settles. Viewer reconnection can recover a still-pending prompt; input fields are cleared after sending, closing, or disconnecting. Read-only viewers never receive prompts or submit responses.

Requests require an exact HTTPS origin, a current page target, and selectors identifying one visible, enabled main-frame input or action. Fields are limited to eight and support text, email, password, and code. Method choices are limited to eight. Requests last at most ten minutes and remain within browser lifetime limits. Page navigation while waiting invalidates the prompt; submission rechecks the document and origin before writing. Main-frame CSS selectors are the initial contract; iframe/shadow-root controls, passkeys, QR scanning, and password-manager integration need separate support. Embedded hosts using an iframe must allow native dialogs, and their proxy must allow the bounded /auth wait and propagate client disconnects.

The API prevents values from appearing in its own results; it does not isolate a page from the website receiving its credentials or from a subsequently authorized raw-CDP client. Harnesses must avoid reading credential field values into model context. The viewer and its host website are trusted input surfaces: serve them over TLS, exclude third-party scripts, and do not record the sign-in dialog. For local integration tests only, createGateway accepts access.allowInsecureAuthLoopback; the packaged service does not enable it.

## Local and Docker deployment

Local Linux hosts may enable the loopback access listener directly. macOS and Windows users run the supported Linux container and connect from the local browser/agent; this does not add a native macOS/Windows runtime.

Build an image, extract its generated seccomp policy, and run the Compose example:

```sh
docker build -t browser-sandbox:live-viewer .
container=$(docker create browser-sandbox:live-viewer)
docker cp "$container:/opt/browser-sandbox/seccomp.json" config/seccomp.json
docker rm "$container"
export BROWSER_SECCOMP_PROFILE="$(pwd)/config/seccomp.json"
docker compose -f examples/compose/compose.yaml up -d
# Run only in the trusted container host context; the output contains capability URLs.
docker compose -f examples/compose/compose.yaml exec browser node /opt/browser-sandbox/examples/live-view.mjs create
```

The example publishes only 127.0.0.1:8080 and retains the image's sandbox, capabilities, and reserved-port requirements. To allow remote users, place it behind a TLS reverse proxy, configure the HTTPS public origin, and restrict the origin listener to trusted proxy traffic. The trusted host remains responsible for execution identity allocation and lifecycle. The example leaves the execution alive; explicitly finalize it afterward with live-view.mjs finish EXECUTION_ID. Before disconnecting browser workers, use live-view.mjs retain EXECUTION_ID; after stopping your agent, use live-view.mjs confirm-stopped EXECUTION_ID to enable a manual acquisition.

## Kubernetes and multiple instances

examples/kubernetes/access.yaml illustrates access routing to one specific runtime instance, not a complete Pod security configuration. Enable the same access listener in the runtime and configure the public HTTPS origin. Preserve the documented runtime sandbox, UID, sysctl, and private-control requirements. Configure ingress WebSocket upgrades, timeouts, and TLS for your controller. Never label interchangeable browser Pods behind a randomly balanced Service for an existing execution.

For multiple instances, the host selects an authoritative execution-to-runtime binding before issuing URLs. Use instance-specific origins or a host routing adapter that preserves that binding. Container/Pod replacement invalidates grants; it does not move live pages. This repository does not introduce a Kubernetes registry or a routing database. Network policies must limit access to the runtime-host control plane and internal CDP ports. Graceful shutdown revokes clients before browser finalization.

## Cloudflare Worker front door

adapters/cloudflare-worker is a small HTTP/WebSocket proxy, not a browser backend. Configure BROWSER_ORIGIN to a reachable authenticated HTTPS access origin and BROWSER_ACCESS_ORIGIN on that origin to the Worker's public origin. The Worker forwards Origin unchanged so browser handshakes are validated against the public viewer origin. Execution/viewer grants remain authoritative at the browser origin; the Worker does not keep a second in-memory grant registry. No request is automatically retried and upstream redirects are not followed. Configure log redaction and origin network protection separately.

A Worker does not gain reachability to a laptop, Docker bridge, or cluster-private address by configuration alone. Supply reachable authenticated origin connectivity. Cloudflare Containers requires separate validation of the existing Chrome sandbox, UID/firewall/security options, filesystem durability, and lifecycle requirements. Cloudflare Browser Run is a different managed browser backend; it is not an equivalent deployment or a fallback for the pinned Browserless/CloakBrowser runtime. This change does not claim either backend is validated.

## Limits and failure behavior

The access listener bounds grants, viewers, per-execution CDP peers, input messages, frames, and capture targets. Streaming is outside serialized lifecycle control. Slow viewers are disconnected or skip obsolete frames. One target capture is shared among its observers; independent targets are limited to four per browser. No audio or video frame-rate guarantee is made. Session hard deadlines still apply while a human holds control. An access link's expiry does not finalize or delete a persistent Profile.

After a disconnected or expired link, request a new authorized link or reopen an unexpired one. Commands and input are never replayed automatically. Keep observation, human lease, browser retention, and agent task status separate when reporting outcomes. Verify the intended client/harness and deployment before production use.

## Verification commands

Run pnpm test, pnpm typecheck, and pnpm lint for unit/lifecycle and static checks. With CHROME_EXECUTABLE set to a local Chrome executable, pnpm test:viewer runs the real-renderer viewer workflow. After building browser-sandbox:live-viewer, pnpm test:viewer:image runs the same workflow against the sandboxed container browser; it uses a local Chrome instance only to render and drive the viewer UI. VIEWER_OUTPUT selects a directory for desktop/mobile screenshots. These commands do not deploy or publish anything.

## Follow the viewer size

Issue access with resize: true for the primary viewer to make the remote page viewport follow the component's content area. This causes real page reflow, not a stretched or cropped screenshot. The host grants this presentation permission separately from human input. One unexpired primary viewer grant may own viewport sizing per execution; additional viewers observe the resulting viewport without changing it. Revoke the old grants before transferring sizing authority. The owner connection is private and kept alive because CDP emulation settings are session-scoped. Browser retirement or grant revocation closes it.

ResizeObserver tracks website panels, dialogs, and window changes with a short debounce. Dimensions are limited to 3840 by 2160 CSS pixels and the stream remains bounded by its capture resolution. Input revisions are invalidated when dimensions change, and pointer mapping accounts for letterboxing in secondary viewers. Viewing without resize permission retains the original browser dimensions. Enable following deliberately when the host owns presentation sizing: it changes media queries and can invalidate an automation client's previously measured coordinates.

Choose the size at connection time or change it without reconnecting. The grant must include resize: true; the frontend cannot grant itself presentation permissions. Dimensions are CSS pixels:

```js
viewer.connect({ endpoint, token, viewport: 'follow' }); // Default: track the panel.
viewer.setViewport({ width: 1920, height: 1080 });     // Fixed remote layout.
viewer.setViewport('follow');                        // Resume responsive sizing.
```

A fixed layout is fitted into the local panel without distorting its aspect ratio; following uses the panel dimensions and reflows the page. Changing these settings requires no source edits or browser restart.
