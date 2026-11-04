import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { readFile, rm } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";

export async function fixture() {
  const challenge = randomUUID();
  const uploadBody = `upload-${randomUUID()}`;
  const downloadBody = `download-${randomUUID()}`;
  const downloadName = `download-${challenge}.txt`;
  let completed = false;
  let uploaded = false;
  const server = createServer(async (req, res) => {
    if (req.url === "/download") {
      res
        .writeHead(200, {
          "content-type": "application/octet-stream",
          "content-disposition": `attachment; filename="${downloadName}"`,
        })
        .end(downloadBody);
      return;
    }
    if (req.url === "/upload" && req.method === "POST") {
      let body = "";
      for await (const chunk of req) {
        body += chunk;
        if (body.length > 4096) {
          res.writeHead(413).end();
          return;
        }
      }
      uploaded = body === uploadBody;
      res.writeHead(uploaded ? 200 : 400).end();
      return;
    }
    if (req.url === "/complete" && req.method === "POST") {
      let input = "";
      for await (const chunk of req) {
        input += chunk;
        if (input.length > 4096) {
          res.writeHead(413).end();
          return;
        }
      }
      completed = new URLSearchParams(input).get("answer") === challenge;
      res.writeHead(completed ? 200 : 400, { "content-type": "text/html" });
      res.end(completed ? "<h1>Completed</h1>" : "<h1>Wrong answer</h1>");
      return;
    }
    res.writeHead(200, { "content-type": "text/html", "cache-control": "no-store" });
    res.end(
      `<h1>Browser challenge</h1><p id="challenge">${challenge}</p><input type="file" aria-label="Upload test file" onchange="window.uploadDone=fetch('/upload',{method:'POST',body:this.files[0]}).then(r=>r.ok)"><a id="download" href="/download">Download test file</a><form action="/complete" method="post"><label>Answer<input name="answer" id="answer"></label><button>Finish</button></form><script>window.handoffMarker=crypto.randomUUID();window.ticks=0;setInterval(()=>window.ticks++,50)</script>`,
    );
  });
  // Linux may allocate ephemeral ports inside the runtime's protected CDP range.
  for (let attempt = 0; attempt < 10; attempt++) {
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const port = server.address().port;
    if (port < 42000 || port >= 43000) break;
    await new Promise((resolve) => server.close(resolve));
  }
  assert(server.listening, "No unprotected fixture port available");
  return {
    url: `http://127.0.0.1:${server.address().port}/challenge`,
    uploadBody,
    downloadBody,
    downloadName,
    async verifyFiles() {
      for (let i = 0; i < 100 && !uploaded; i++)
        await new Promise((resolve) => setTimeout(resolve, 25));
      assert(uploaded, "The browser did not upload the expected file bytes");
    },
    async verify() {
      for (let i = 0; i < 100 && !completed; i++)
        await new Promise((resolve) => setTimeout(resolve, 25));
      assert(completed, "The browser did not submit the server-generated challenge");
    },
    close: () => new Promise((resolve) => server.close(resolve)),
  };
}
export const complete =
  "() => { document.querySelector('#answer').value = document.querySelector('#challenge').textContent; document.querySelector('form').requestSubmit(); return 'submitted'; }";
export const inspect =
  "async () => { for (let i = 0; i < 100 && !window.handoffMarker; i++) await new Promise(resolve => setTimeout(resolve, 50)); return { marker: window.handoffMarker, ticks: window.ticks }; }";
export const text = (result) =>
  (result.content ?? [])
    .filter((item) => item.type === "text")
    .map((item) => item.text)
    .join("\n");
export function pageId(result, url) {
  const line = text(result)
    .split("\n")
    .find((line) => line.includes(url));
  const id = Number(/^\s*(\d+):/.exec(line ?? "")?.[1]);
  assert(Number.isInteger(id), "Missing browser page ID");
  return id;
}
export function evaluated(result) {
  const match = /```json\s*([\s\S]*?)\s*```/.exec(text(result));
  assert(match, "Missing browser evaluation result");
  return JSON.parse(match[1]);
}

export const HANDOFF_MS = 180000;

export function uploadUid(snapshot) {
  const line = text(snapshot)
    .split("\n")
    .find((line) => line.includes("Upload test file"));
  const uid = /uid=([^\s]+)/.exec(line ?? "")?.[1];
  assert(uid, "Missing upload control in browser snapshot");
  return uid;
}

export async function prepareFiles(site, endpoint) {
  const base = endpoint.replace(/^ws:/, "http:") + "/files/";
  const response = await fetch(base + "upload.txt", {
    method: "PUT",
    body: site.uploadBody,
  });
  assert.equal(response.status, 201);
  const { path } = await response.json();
  return {
    path: process.env.BROWSER_FILES_DIRECTORY
      ? join(process.env.BROWSER_FILES_DIRECTORY, basename(path))
      : path,
    async verify() {
      await site.verifyFiles();
      for (let i = 0; i < 100; i++) {
        const response = await fetch(base + site.downloadName);
        if (response.ok) {
          assert.equal(await response.text(), site.downloadBody);
          return;
        }
        assert.equal(response.status, 404);
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
      assert.fail("The browser did not download the expected file bytes");
    },
  };
}

export async function transferFiles(call, page, files) {
  const filePath = join(tmpdir(), `browser-screenshot-${randomUUID()}.png`);
  try {
    const screenshot = await call("take_screenshot", {
      pageId: page,
      format: "png",
      filePath,
    });
    assert(!screenshot.isError, "Browser screenshot tool failed");
    assertPng(await readFile(filePath));
  } finally {
    await rm(filePath, { force: true });
  }
  const snapshot = await call("take_snapshot", { pageId: page });
  const uploaded = await call("upload_file", {
    pageId: page,
    uid: uploadUid(snapshot),
    filePaths: [files.path],
  });
  assert(!uploaded.isError, "Browser file upload tool failed");
  const downloaded = await call("evaluate_script", {
    pageId: page,
    function:
      "() => { document.querySelector('#download').click(); return 'downloading'; }",
  });
  assert(!downloaded.isError, "Browser download interaction failed");
  await files.verify();
}

export function assertPng(bytes) {
  assert(bytes.length > 100, "Screenshot is empty");
  assert.equal(bytes.subarray(0, 8).toString("hex"), "89504e470d0a1a0a");
  assert(bytes.readUInt32BE(16) > 0 && bytes.readUInt32BE(20) > 0);
}
