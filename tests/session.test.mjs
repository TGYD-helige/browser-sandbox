import assert from "node:assert/strict";
import test from "node:test";
import { BrowserSession, sessionRequest } from "../dist/session.js";

test("explicit renewal never extends a browser generation's hard deadline", () => {
  const original = Date.now;
  let now = original();
  Date.now = () => now;
  const session = new BrowserSession(() => {}, 1000);
  try {
    assert.equal(session.status(0).state, "unused");
    session.starting();
    session.ready(now + 100);
    session.control("retain", 500);
    const first = session.status(0);
    assert.equal(first.expiresAt, now + 100);
    now += 50;
    session.control("renew", 1000);
    assert.equal(session.status(0).expiresAt, first.expiresAt);
    now += 51;
    assert.equal(session.status(0).state, "expired");
    assert.throws(() => session.control("renew", 500), /not_live/);
    session.ended();
    session.control("release");
    assert.equal(session.status(0).state, "expired");
    assert.throws(() => session.control("retain", 500), /not_live/);
  } finally {
    session.ended();
    Date.now = original;
  }
});

test("session clients reject malformed endpoints before making requests", async () => {
  for (const value of [
    "http://example.com",
    "ws://example.com/other",
    `ws://user:secret@example.com/execution/${"a".repeat(64)}`,
    `ws://example.com/execution/${"a".repeat(64)}?token=other`,
  ])
    await assert.rejects(sessionRequest(value), /endpoint_invalid/);
});
