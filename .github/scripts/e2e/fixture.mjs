import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { createServer } from "node:http";

export async function fixture() {
  const challenge = randomUUID();
  let completed = false;
  const server = createServer(async (req, res) => {
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
      `<h1>Browser challenge</h1><p id="challenge">${challenge}</p><form action="/complete" method="post"><label>Answer<input name="answer" id="answer"></label><button>Finish</button></form><script>window.handoffMarker=crypto.randomUUID();window.ticks=0;setInterval(()=>window.ticks++,50)</script>`,
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
