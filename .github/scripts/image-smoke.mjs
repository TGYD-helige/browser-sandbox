import assert from "node:assert/strict";
import { fork } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { createServer } from "node:http";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { connectCdp } from "../../dist/cdp.js";
import { hostRequest } from "../../dist/host-client.js";
import { sessionRequest } from "../../dist/session.js";

if (process.argv[2] === "worker") {
  const endpoint = process.env.BROWSER_WS_ENDPOINT;
  const connection = connectCdp(endpoint);
  connection.catch(() => {});
  process.on("message", async ({ id, method, params = {} }) => {
    try {
      const upstream = await connection;
      const cdp = {
        async call(method, ...args) {
          try {
            return await upstream.call(method, ...args);
          } catch {
            throw Error("browser_smoke_cdp_" + method.replaceAll(".", "_"));
          }
        },
      };
      let result;
      if (method === "session")
        result = await sessionRequest(endpoint, params.action, params.durationMs);
      else if (method === "deny") {
        await assert.rejects(connectCdp(params.endpoint), /connect_failed|closed/);
        result = true;
      } else if (method === "files") {
        const url = endpoint.replace(/^ws:/, "http:") + "/files/";
        const uploaded = await fetch(url + "upload.txt", {
          method: "PUT",
          body: "browser upload",
        });
        assert.equal(uploaded.status, 201, "browser_smoke_upload_denied");
        const { path } = await uploaded.json();
        const { targetId } = await cdp.call("Target.createTarget", {
          url: "about:blank",
        });
        const { sessionId } = await cdp.call("Target.attachToTarget", {
          targetId,
          flatten: true,
        });
        await cdp.call("Page.enable", {}, sessionId);
        await cdp.call("Page.navigate", { url: params.url + "/upload" }, sessionId);
        for (let i = 0; i < 100; i++) {
          try {
            const ready = await cdp.call(
              "Runtime.evaluate",
              { expression: "!!document.querySelector('input')", returnByValue: true },
              sessionId,
            );
            if (ready.result.value) break;
          } catch {
            /* A read can cross the initial document commit. */
          }
          await delay(25);
        }
        const { root } = await cdp.call("DOM.getDocument", {}, sessionId);
        const { nodeId } = await cdp.call(
          "DOM.querySelector",
          { nodeId: root.nodeId, selector: "input" },
          sessionId,
        );
        assert(nodeId, "browser_smoke_upload_input_missing");
        await cdp.call("DOM.setFileInputFiles", { nodeId, files: [path] }, sessionId);
        const value = await cdp.call(
          "Runtime.evaluate",
          {
            expression: "document.querySelector('input').files[0].text()",
            awaitPromise: true,
            returnByValue: true,
          },
          sessionId,
        );
        assert.equal(
          value.result.value,
          "browser upload",
          "browser_smoke_upload_content_mismatch",
        );
        await cdp.call(
          "Runtime.evaluate",
          { expression: "document.querySelector('a').click()", userGesture: true },
          sessionId,
        );
        let downloaded;
        for (let i = 0; i < 100; i++) {
          const response = await fetch(url + "download.txt");
          if (response.ok) {
            downloaded = await response.text();
            break;
          }
          await delay(50);
        }
        assert.equal(downloaded, "browser download", "browser_smoke_download_missing");
        result = true;
      } else result = await cdp.call(method, params);
      process.send({ id, result });
    } catch (error) {
      process.send({
        id,
        error:
          error.code === "ERR_ASSERTION"
            ? (/^browser_smoke_[a-z_]+/.exec(error.message)?.[0] ??
              "browser_smoke_assertion_failed")
            : error.message,
      });
    }
  });
  process.on("disconnect", () => {
    connection.then(
      (cdp) => cdp.close(),
      () => {},
    );
    process.exit(0);
  });
} else {
  const socket = process.env.BROWSER_CONTROL_SOCKET;
  const workers = new Set(),
    executions = new Set();
  const site = createServer((req, res) => {
    if (req.url === "/download")
      res
        .writeHead(200, {
          "content-type": "application/octet-stream",
          "content-disposition": 'attachment; filename="download.txt"',
        })
        .end("browser download");
    else
      res
        .writeHead(200, { "content-type": "text/html" })
        .end('<input type="file"><a href="/download">Download</a>');
  });
  // This fixture owns the container network namespace; avoid protected CDP ports.
  site.listen(18080, "127.0.0.1");
  await once(site, "listening");
  const create = async (uid, profileId) => {
    const executionId = randomUUID();
    executions.add(executionId);
    const { wsEndpoint } = await hostRequest(socket, {
      op: "register",
      executionId,
      executorUid: uid,
      ...(profileId ? { profileId } : {}),
    });
    const child = fork(fileURLToPath(import.meta.url), ["worker"], {
      uid,
      gid: uid,
      env: { PATH: process.env.PATH, BROWSER_WS_ENDPOINT: wsEndpoint },
      stdio: ["ignore", "ignore", "pipe", "ipc"],
    });
    workers.add(child);
    let sequence = 0;
    const pending = new Map();
    child.on("message", ({ id, result, error }) => {
      const call = pending.get(id);
      if (call) {
        pending.delete(id);
        clearTimeout(call.timer);
        error ? call.reject(Error(error)) : call.resolve(result);
      }
    });
    child.on("exit", () => {
      workers.delete(child);
      for (const call of pending.values()) {
        clearTimeout(call.timer);
        call.reject(Error("browser_smoke_worker_closed"));
      }
      pending.clear();
    });
    const call = (method, params) =>
      new Promise((resolve, reject) => {
        const id = ++sequence;
        const timer = setTimeout(() => {
          pending.delete(id);
          reject(Error("browser_smoke_timeout"));
        }, 55000);
        pending.set(id, { resolve, reject, timer });
        child.send({ id, method, params });
      });
    const ready = call("Browser.getVersion");
    const finish = async () => {
      if (child.connected) {
        const exited = once(child, "exit");
        child.disconnect();
        await exited;
      }
      const result = await hostRequest(socket, { op: "finalize", executionId });
      executions.delete(executionId);
      return result;
    };
    return { wsEndpoint, ready, call, finish };
  };
  try {
    const a = await create(100011),
      b = await create(100012);
    await Promise.all([a.ready, b.ready]);
    await a.call("Storage.setCookies", {
      cookies: [{ name: "isolation", value: "a", domain: "example.com", path: "/" }],
    });
    assert(
      !(await b.call("Storage.getCookies")).cookies.some(
        (cookie) => cookie.name === "isolation",
      ),
    );
    await a.call("deny", { endpoint: b.wsEndpoint });
    const c = await create(100013);
    let queued = true;
    c.ready.then(
      () => {
        queued = false;
      },
      () => {},
    );
    await delay(300);
    assert(queued, "The third browser must queue at CONCURRENT=2");
    await a.finish();
    await c.ready;
    await c.call("files", { url: `http://127.0.0.1:${site.address().port}` });
    await Promise.all([b.finish(), c.finish()]);
    const profileId = "smoke/" + randomUUID();
    const first = await create(100014, profileId);
    await first.ready;
    await first.call("Storage.setCookies", {
      cookies: [
        {
          name: "persistent",
          value: "saved",
          domain: "example.com",
          path: "/",
          expires: Date.now() / 1000 + 3600,
        },
        { name: "session_only", value: "temporary", domain: "example.com", path: "/" },
      ],
    });
    await first.finish();
    const second = await create(100015, profileId);
    await second.ready;
    let cookies = (await second.call("Storage.getCookies")).cookies;
    assert(cookies.some((cookie) => cookie.name === "persistent"));
    assert(!cookies.some((cookie) => cookie.name === "session_only"));
    await second.call("Storage.clearCookies");
    await second.finish();
    const third = await create(100016, profileId);
    await third.ready;
    cookies = (await third.call("Storage.getCookies")).cookies;
    assert(!cookies.some((cookie) => cookie.name === "persistent"));
    await third.finish();
    console.log(
      "Image lifecycle: UID/Cookie isolation, concurrency queue, upload/download, persistent Cookie restore/deletion, and Session Cookie exclusion passed",
    );
  } finally {
    for (const child of workers) child.kill("SIGTERM");
    for (const executionId of executions)
      await hostRequest(socket, { op: "finalize", executionId }).catch(() => {});
    await new Promise((resolve) => site.close(resolve));
  }
}
