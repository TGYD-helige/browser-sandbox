import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm } from "node:fs/promises";
import { createServer } from "node:http";
import { createServer as createSecureServer } from "node:https";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { chromium } from "playwright-core";
import WebSocket, { WebSocketServer } from "ws";
import { createGateway } from "../../dist/gateway.js";
import { sessionRequest } from "../../dist/session.js";

const executablePath = process.env.CHROME_EXECUTABLE;
if (!executablePath)
  throw Error("Set CHROME_EXECUTABLE for the real-renderer viewer check");
const container = process.env.BROWSER_TEST_CONTAINER;
const exec = promisify(execFile);
const root = await mkdtemp(join(tmpdir(), "live-view-e2e-"));
const output = process.env.VIEWER_OUTPUT ?? root;
await mkdir(output, { recursive: true });
const site = createServer((_request, response) => {
  response.setHeader("content-type", "text/html; charset=utf-8");
  response.end(
    `<!doctype html><html lang="en"><meta charset="utf-8"><title>Live view test</title><style>body{font:20px system-ui;margin:40px;background:#fff}input,button{font:inherit;padding:12px}main{height:1800px}</style><main><h1>Remote browser</h1><label>Name <input id="name"></label><button onclick="location.href='/complete'">Continue</button><p id="clock"></p></main><script>setInterval(()=>document.querySelector('#clock').textContent=new Date().toISOString(),200)</script></html>`,
  );
});
await new Promise((resolve) =>
  site.listen(0, container ? "0.0.0.0" : "127.0.0.1", resolve),
);
await exec(
  "openssl",
  [
    "req",
    "-x509",
    "-newkey",
    "rsa:2048",
    "-nodes",
    "-keyout",
    join(root, "key.pem"),
    "-out",
    join(root, "cert.pem"),
    "-days",
    "1",
    "-subj",
    "/CN=localhost",
  ],
  { maxBuffer: 1024 * 1024 },
);
const authSite = createSecureServer(
  {
    key: await readFile(join(root, "key.pem")),
    cert: await readFile(join(root, "cert.pem")),
  },
  (request, response) => {
    response.setHeader("content-type", "text/html; charset=utf-8");
    const content =
      request.url === "/method"
        ? `<button id="email" onclick="location.href='/login'">Use email</button>`
        : request.url === "/done"
          ? `<h1>Signed in</h1>`
          : `<h1>Sign in</h1><form onsubmit="event.preventDefault(); if(document.querySelector('#email').value==='viewer@example.com' && document.querySelector('#password').value==='fixture-password') {history.pushState({},'', '/done'); document.body.innerHTML='<h1>Signed in</h1>';} else document.querySelector('#error').textContent='Invalid credentials'"><label>Email <input id="email" type="email"></label><label>Password <input id="password" type="password"></label><button id="submit">Sign in</button><p id="error"></p></form>`;
    response.end(
      `<!doctype html><html lang="en"><meta charset="utf-8"><title>Authentication fixture</title><style>body{font:20px system-ui;margin:40px}label{display:block;margin:20px 0}input,button{font:inherit;padding:12px}</style>${content}</html>`,
    );
  },
);
await new Promise((resolve) =>
  authSite.listen(0, container ? "0.0.0.0" : "127.0.0.1", resolve),
);
let gateway, viewerBrowser, agent;
try {
  let launches = 0;
  gateway = container
    ? {
        async command(request) {
          const { stdout } = await exec(
            "docker",
            [
              "exec",
              container,
              "node",
              "--input-type=module",
              "-e",
              'import {hostRequest} from "/opt/browser-sandbox/dist/index.js"; const result=await hostRequest("/run/browser-sandbox/control.sock", JSON.parse(process.argv[1])); console.log(JSON.stringify(result));',
              JSON.stringify(request),
            ],
            { timeout: 180_000, maxBuffer: 1024 * 1024 },
          );
          return JSON.parse(stdout);
        },
        async stop() {},
      }
    : await createGateway({
        WebSocket,
        WebSocketServer,
        port: 0,
        dataRoot: join(root, "data"),
        socketPath: join(root, "control.sock"),
        healthy: async () => true,
        access: { port: 0 },
        async launch({ profileDir }) {
          launches++;
          const context = await chromium.launchPersistentContext(profileDir, {
            executablePath,
            headless: true,
            chromiumSandbox: true,
            viewport: { width: 1280, height: 800 },
            args: ["--remote-debugging-port=0"],
          });
          const [port, path] = (
            await readFile(join(profileDir, "DevToolsActivePort"), "utf8")
          )
            .trim()
            .split("\n");
          return {
            endpoint: `ws://127.0.0.1:${port}${path}`,
            close: () => context.close(),
            closed: new Promise((resolve) => context.once("close", resolve)),
          };
        },
      });
  const executionId = randomUUID();
  await gateway.command({
    op: "register",
    executionId,
    ...(container ? { executorUid: 100001 } : {}),
  });
  const access = await gateway.command(
    { op: "access_create", executionId, control: true, resize: true },
    true,
  );
  viewerBrowser = await chromium.launch({
    executablePath,
    headless: true,
    chromiumSandbox: true,
  });
  const viewer = await viewerBrowser.newPage({
    viewport: { width: 1440, height: 1050 },
  });
  async function waitForFill() {
    await viewer.waitForFunction(() => {
      const image = document.querySelector(".viewport img");
      const viewport = document.querySelector(".viewport").getBoundingClientRect();
      return (
        image.naturalWidth > 0 &&
        Math.abs(
          image.naturalWidth / image.naturalHeight - viewport.width / viewport.height,
        ) < 0.005
      );
    });
  }
  const errors = [];
  async function sessionAction(name) {
    if (!(await viewer.locator(".control-menu").evaluate((menu) => menu.open)))
      await viewer.locator(".control-menu > summary").click();
    await viewer.getByRole("button", { name, exact: true }).click();
  }
  viewer.on("pageerror", (error) => errors.push(error.message));
  await viewer.goto(access.viewerUrl);
  await viewer.getByText("Viewing", { exact: true }).waitFor();
  assert.equal(launches, 0);
  assert.equal(new URL(viewer.url()).hash, "");
  agent = await chromium.connectOverCDP(access.cdpUrl);
  let page = agent.contexts()[0].pages()[0];
  const siteOrigin = `http://${container ? "host.docker.internal" : "127.0.0.1"}:${site.address().port}`;
  await page.goto(`${siteOrigin}/form`);
  await viewer.locator(".viewport img").waitFor({ state: "visible" });
  await viewer.waitForFunction(() =>
    document.querySelector(".url").value.endsWith("/form"),
  );
  assert.equal(await viewer.locator('[data-action="acquire"]').isEnabled(), false);
  await waitForFill();
  await viewer.screenshot({ path: join(output, "viewer-desktop.png"), fullPage: true });
  await viewer.evaluate(() =>
    document.querySelector("browser-viewer").setViewport({ width: 1280, height: 720 }),
  );
  await page.waitForFunction(() => innerWidth === 1280 && innerHeight === 720);
  await viewer.evaluate(() =>
    document.querySelector("browser-viewer").setViewport("follow"),
  );
  await waitForFill();
  await viewer.setViewportSize({ width: 390, height: 844 });
  await waitForFill();
  await viewer.screenshot({ path: join(output, "viewer-mobile.png"), fullPage: true });
  assert(
    await viewer.evaluate(() => document.documentElement.scrollWidth <= innerWidth),
  );
  await viewer.setViewportSize({ width: 1440, height: 1050 });
  await waitForFill();
  const embeddedAccess = await gateway.command(
    { op: "access_create", executionId },
    true,
  );
  const embedded = await viewerBrowser.newPage({
    viewport: { width: 1100, height: 850 },
  });
  await embedded.goto(new URL("/view", access.viewerUrl).href);
  await embedded.evaluate(({ viewerUrl }) => {
    document.querySelector("browser-viewer").remove();
    const dialog = document.createElement("dialog");
    dialog.style.cssText =
      "width:820px;height:640px;padding:16px;border:1px solid #aab9af;border-radius:8px";
    const element = document.createElement("browser-viewer");
    dialog.append(element);
    document.body.append(dialog);
    dialog.showModal();
    const url = new URL(viewerUrl);
    element.connect({
      endpoint: url.origin.replace(/^http/, "ws") + "/view/socket",
      token: url.hash.slice(1),
    });
  }, embeddedAccess);
  await embedded.locator("dialog .viewport img").waitFor({ state: "visible" });
  assert.equal(await embedded.locator("dialog .viewer-heading").isVisible(), false);
  assert(
    await embedded.evaluate(() => {
      const panel = document
        .querySelector("dialog browser-viewer")
        .getBoundingClientRect();
      const picture = document.querySelector("dialog img").getBoundingClientRect();
      const viewport = document
        .querySelector("dialog .viewport")
        .getBoundingClientRect();
      return (
        picture.width <= viewport.width &&
        picture.height <= viewport.height &&
        viewport.height <= panel.height
      );
    }),
  );
  await embedded.screenshot({
    path: join(output, "viewer-embedded-dialog.png"),
    fullPage: true,
  });
  await embedded.evaluate(() => {
    document.querySelector("browser-viewer").remove();
    document.querySelector("dialog").close();
  });
  await embedded.close();
  await viewer.waitForFunction(() => {
    const image = document.querySelector(".viewport img").getBoundingClientRect();
    const viewport = document.querySelector(".viewport").getBoundingClientRect();
    return image.width >= viewport.width - 2 && image.height >= viewport.height - 2;
  });
  const inputBox = await page.locator("#name").boundingBox();
  const viewport = await page.evaluate(() => ({
    width: innerWidth,
    height: innerHeight,
  }));
  // This test controls the only automation connection; stop it before host confirmation.
  await gateway.command(
    { op: "session", executionId, action: "retain", durationMs: 120_000 },
    true,
  );
  await agent.close();
  agent = undefined;
  await gateway.command(
    { op: "access_confirm_stopped", executionId, automationStopped: true },
    true,
  );
  await sessionAction("Take over browser");
  await viewer.getByText("You have control", { exact: true }).waitFor();
  const image = viewer.locator(".viewport img");
  // Use the actual fixture viewport so the same check works on the pinned browser image.
  const box = await image.boundingBox();
  await image.click({
    position: {
      x: ((inputBox.x + inputBox.width / 2) / viewport.width) * box.width,
      y: ((inputBox.y + inputBox.height / 2) / viewport.height) * box.height,
    },
  });
  await viewer.keyboard.insertText("你好，browser");
  await viewer.keyboard.type(" live", { delay: 10 });
  await sessionAction("Send Tab key");
  await sessionAction("Return to AI");
  await viewer
    .getByText("Control returned. Resume or reconnect your agent through the host.", {
      exact: true,
    })
    .waitFor();
  agent = await chromium.connectOverCDP(access.cdpUrl);
  page = agent.contexts()[0].pages()[0];
  assert.equal(await page.locator("#name").inputValue(), "你好，browser live");
  assert.equal(await page.evaluate(() => document.activeElement.tagName), "BUTTON");
  await page.goto(`${siteOrigin}/continued`);
  assert.equal(await page.title(), "Live view test");
  const authOrigin = `https://${container ? "host.docker.internal" : "127.0.0.1"}:${authSite.address().port}`;
  const authSession = await page.context().newCDPSession(page);
  // The fixture finishes sign-in with an SPA transition: CDP certificate bypass is connection-scoped.
  // Only this generated local fixture certificate is bypassed in the test browser.
  await authSession.send("Security.setIgnoreCertificateErrors", { ignore: true });
  await page.goto(`${authOrigin}/method`);
  const { targetInfo } = await authSession.send("Target.getTargetInfo");
  await authSession.detach();
  const authSpec = {
    targetId: targetInfo.targetId,
    origin: authOrigin,
    fields: [
      { id: "email", label: "Email", type: "email", selector: "#email" },
      { id: "password", label: "Password", type: "password", selector: "#password" },
    ],
    submit: { selector: "#submit" },
  };
  async function requestAuth(spec, interact) {
    await gateway.command(
      { op: "session", executionId, action: "retain", durationMs: 120_000 },
      true,
    );
    await agent.close();
    agent = undefined;
    await gateway.command(
      { op: "access_confirm_stopped", executionId, automationStopped: true },
      true,
    );
    const running = exec(
      process.execPath,
      [
        fileURLToPath(
          new URL(
            "../../skills/browser-sandbox-auth/scripts/auth.mjs",
            import.meta.url,
          ),
        ),
      ],
      {
        env: { ...process.env, BROWSER_WS_ENDPOINT: access.cdpUrl },
        timeout: (spec.timeoutMs ?? 300_000) + 25_000,
        maxBuffer: 8192,
      },
    );
    running.child.stdin.end(JSON.stringify(spec));
    const resultPromise = running.then(({ stdout }) => JSON.parse(stdout));
    if (interact) {
      await viewer.locator(".auth-dialog").waitFor({ state: "visible" });
      await interact();
    }
    const result = await resultPromise;
    await viewer.locator(".auth-dialog").waitFor({ state: "hidden" });
    assert.equal(result.controlReleased, true, JSON.stringify(result));
    assert.equal(JSON.stringify(result).includes("fixture-password"), false);
    agent = await chromium.connectOverCDP(access.cdpUrl);
    page = agent.contexts()[0].pages()[0];
    return result;
  }
  const method = await requestAuth(
    {
      ...authSpec,
      fields: [],
      submit: undefined,
      options: [{ id: "email", label: "Use email", selector: "#email" }],
    },
    () => viewer.getByRole("button", { name: "Use email", exact: true }).click(),
  );
  assert.equal(method.status, "submitted");
  assert.equal(method.selectedOption, "email");
  await page.waitForURL(`${authOrigin}/login`);
  assert.equal(
    (await requestAuth(authSpec, () => viewer.locator(".auth-cancel").click())).status,
    "declined",
  );
  // Exercise expiry while the user prompt is waiting, after control has been acquired.
  assert.equal(
    (await requestAuth({ ...authSpec, timeoutMs: 2000 }, async () => {})).status,
    "expired",
  );
  assert.equal(
    (
      await requestAuth({
        ...authSpec,
        fields: [{ ...authSpec.fields[0], selector: "#missing" }],
        submit: undefined,
      })
    ).status,
    "locator_invalid",
  );
  await viewer.evaluate(() => {
    window.authResults = [];
    document
      .querySelector("browser-viewer")
      .addEventListener("browser-viewer-result", (event) =>
        window.authResults.push(event.detail),
      );
  });
  const invalid = await requestAuth(authSpec, async () => {
    await viewer.locator('.auth-dialog input[name="email"]').fill("viewer@example.com");
    await viewer
      .locator('.auth-dialog input[name="password"]')
      .fill("wrong-fixture-password");
    await viewer.locator(".auth-submit").click();
  });
  assert.equal(invalid.status, "submitted");
  assert.equal(await page.locator("#error").textContent(), "Invalid credentials");
  assert.equal(page.url(), `${authOrigin}/login`);
  const submitted = await requestAuth(authSpec, async () => {
    await viewer.locator('.auth-dialog input[name="email"]').fill("viewer@example.com");
    await viewer
      .locator('.auth-dialog input[name="password"]')
      .fill("fixture-password");
    await viewer.screenshot({
      path: join(output, "viewer-authentication.png"),
      fullPage: true,
    });
    await viewer.locator(".auth-submit").click();
  });
  assert.equal(submitted.status, "submitted");
  await page.waitForURL(`${authOrigin}/done`, { waitUntil: "domcontentloaded" });
  assert.equal(await page.locator("h1").textContent(), "Signed in");
  assert.equal(await viewer.locator(".auth-dialog input").count(), 0);
  assert.equal(
    await viewer.evaluate(() =>
      JSON.stringify(window.authResults).includes("fixture-password"),
    ),
    false,
  );
  // Submitted only confirms delivery. The harness above checks the resulting page separately.
  const realSites = [];
  for (const url of (process.env.VIEWER_SITE_URLS ?? "").split(",").filter(Boolean)) {
    const sitePage = await agent.contexts()[0].newPage();
    let status, error;
    try {
      status = (
        await sitePage.goto(url, { waitUntil: "domcontentloaded", timeout: 60_000 })
      )?.status();
    } catch (failure) {
      error = /ERR_[A-Z_]+/.exec(failure.message)?.[0] ?? "navigation_failed";
    }
    const title = await sitePage.title().catch(() => "");
    if (error) {
      realSites.push({ url, title, status, error });
      continue;
    }
    const session = await sitePage.context().newCDPSession(sitePage);
    const { targetInfo } = await session.send("Target.getTargetInfo");
    await session.detach();
    const tab = viewer.locator(`.tabs button[data-target-id="${targetInfo.targetId}"]`);
    await tab.waitFor();
    await tab.click();
    await viewer.waitForFunction(
      (expected) => document.querySelector(".url").value.startsWith(expected),
      new URL(sitePage.url()).origin,
    );
    await waitForFill();
    const hostname = new URL(url).hostname;
    await sitePage.waitForLoadState("load", { timeout: 15_000 }).catch(() => {});
    await sitePage.screenshot({ path: join(output, `raw-${hostname}.png`) });
    await viewer.locator(".viewport img").evaluate(async (image) => {
      if (!image.complete)
        await new Promise((resolve) => {
          image.addEventListener("load", resolve, { once: true });
          image.addEventListener("error", resolve, { once: true });
        });
      await new Promise((resolve) =>
        requestAnimationFrame(() => requestAnimationFrame(resolve)),
      );
    });
    await viewer.locator(".viewport img").waitFor({ state: "visible" });
    await viewer.screenshot({
      path: join(output, `site-${hostname}.png`),
      fullPage: true,
    });
    realSites.push({ url, title, status, error });
  }
  await gateway.command({ op: "access_revoke", executionId }, true);
  await viewer.getByText("Disconnected", { exact: true }).waitFor();
  assert.equal(await viewer.locator("form.input button").isEnabled(), false);
  assert.deepEqual(errors, []);
  const stopAccess = await gateway.command({ op: "access_create", executionId }, true);
  assert.equal((await sessionRequest(stopAccess.cdpUrl, "stop")).state, "stopped");
  await assert.rejects(sessionRequest(stopAccess.cdpUrl));
  await gateway.command({ op: "finalize", executionId }, true);
  console.log(
    JSON.stringify({
      passed: true,
      browser: container ? "container" : "local Chrome",
      checks: [
        "lazy view",
        "standard CDP",
        "real frames",
        "desktop/mobile layout",
        "fixed viewport and runtime follow switch",
        "confirmed takeover",
        "direct Chinese text and queued typing",
        "remote Tab key",
        "handback",
        "reconnect",
        "revocation",
        "remote stop response before generation-bound connection close",
        "authentication method selection, cancellation, timeout, invalid locator",
        "private credential submission and fresh login-success inspection",
        "submitted does not imply login success",
      ],
      screenshots: output,
      realSites,
    }),
  );
} finally {
  await agent?.close().catch(() => {});
  await viewerBrowser?.close();
  await gateway?.stop();
  await new Promise((resolve) => site.close(resolve));
  await new Promise((resolve) => authSite.close(resolve));
  await rm(root, { recursive: true, force: true });
}
