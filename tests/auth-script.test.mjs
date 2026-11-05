import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { cp, mkdtemp, rm, symlink } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { promisify } from "node:util";

const exec = promisify(execFile);
const spec = {
  targetId: "page",
  origin: "https://example.com",
  fields: [
    { id: "password", label: "Password", type: "password", selector: "#password" },
  ],
  timeoutMs: 1000,
};
const result = {
  requestId: "request",
  status: "submitted",
  controlReleased: true,
  resumed: false,
};
async function fixture(t, respond) {
  const root = await mkdtemp(join(tmpdir(), "auth-script-"));
  const skill = join(root, "skill");
  await cp(new URL("../skills/browser-sandbox-auth", import.meta.url), skill, {
    recursive: true,
  });
  await symlink(
    skill,
    join(root, "linked-skill"),
    process.platform === "win32" ? "junction" : "dir",
  );
  const calls = [];
  const server = createServer(async (request, response) => {
    let body = "";
    for await (const chunk of request) body += chunk;
    calls.push({ path: request.url, body: JSON.parse(body) });
    respond(response);
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(async () => {
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
    await rm(root, { recursive: true, force: true });
  });
  const endpoint = `ws://127.0.0.1:${server.address().port}/execution/${"a".repeat(64)}`;
  return {
    calls,
    run(input = spec, url = endpoint) {
      const running = exec(
        process.execPath,
        [join(root, "linked-skill/scripts/auth.mjs")],
        {
          cwd: root,
          env: { ...process.env, BROWSER_WS_ENDPOINT: url },
          timeout: 5000,
          maxBuffer: 16384,
        },
      );
      running.child.stdin.end(
        typeof input === "string" ? input : JSON.stringify(input),
      );
      return running;
    },
  };
}

test("the copied skill runs through a symlink without SDK dependencies and emits only status", async (t) => {
  const f = await fixture(t, (res) =>
    res.end(JSON.stringify({ ...result, credentials: "never-emit-this" })),
  );
  const output = await f.run();
  assert.deepEqual(JSON.parse(output.stdout), result);
  assert.equal(output.stderr, "");
  assert.deepEqual(f.calls, [
    { path: `/execution/${"a".repeat(64)}/auth`, body: spec },
  ]);
});

test("the script rejects malformed, oversized, and credential-bearing requests before forwarding", async (t) => {
  const f = await fixture(t, (res) => res.end(JSON.stringify(result)));
  for (const input of [
    "{",
    " ".repeat(8193),
    { ...spec, values: { password: "private-test-value" } },
    { ...spec, fields: [{ ...spec.fields[0], value: "private-test-value" }] },
    { ...spec, timeoutMs: 600001 },
  ]) {
    await assert.rejects(f.run(input), (error) => {
      assert.equal(error.code, 1);
      assert.equal(error.stdout, "");
      assert.deepEqual(JSON.parse(error.stderr), {
        error: "browser_auth_request_invalid",
      });
      return true;
    });
  }
  assert.equal(f.calls.length, 0);
});

test("the script sanitizes endpoint and server failures", async (t) => {
  const f = await fixture(t, (res) => {
    res.writeHead(400);
    res.end(JSON.stringify({ error: "private diagnostic with credential" }));
  });
  await assert.rejects(
    f.run(spec, "ws://user:private-test-value@example.com/"),
    (error) => {
      assert.equal(error.stdout, "");
      assert.deepEqual(JSON.parse(error.stderr), {
        error: "browser_auth_endpoint_invalid",
      });
      return true;
    },
  );
  await assert.rejects(f.run(), (error) => {
    assert.equal(error.stdout, "");
    assert.deepEqual(JSON.parse(error.stderr), {
      error: "browser_auth_request_failed",
    });
    return true;
  });
});

test("the script rejects oversized responses and preserves cancellation status", async (t) => {
  const large = await fixture(t, (res) => res.end(" ".repeat(8193)));
  await assert.rejects(large.run(), (error) => {
    assert.deepEqual(JSON.parse(error.stderr), {
      error: "browser_auth_response_invalid",
    });
    return true;
  });
  const cancelled = { ...result, status: "cancelled", controlReleased: false };
  const normal = await fixture(t, (res) => res.end(JSON.stringify(cancelled)));
  assert.deepEqual(JSON.parse((await normal.run()).stdout), cancelled);
});

test("the script does not follow redirects or retry a request", async (t) => {
  const f = await fixture(t, (res) => {
    res.writeHead(307, { location: "/another-origin" });
    res.end();
  });
  await assert.rejects(f.run(), (error) => {
    assert.deepEqual(JSON.parse(error.stderr), {
      error: "browser_auth_request_failed",
    });
    return true;
  });
  assert.equal(f.calls.length, 1);
});
