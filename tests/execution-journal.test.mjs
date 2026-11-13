import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { createExecutionJournal } from "../dist/execution-journal.js";

async function fixture(t, maxRecords = 10) {
  const directory = await mkdtemp(join(tmpdir(), "execution-journal-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const options = { directory, installationId: "test-installation", maxRecords };
  return { directory, options, journal: await createExecutionJournal(options) };
}

test("finished and never-started execution identities survive controller restart", async (t) => {
  const { journal, options } = await fixture(t);
  const id = randomUUID();
  const unused = randomUUID();
  await journal.create(id, { state: "registered" });
  await journal.create(unused, { state: "registered" });
  await journal.update(id, 1, { state: "finished" });
  const restarted = await createExecutionJournal(options);
  await assert.rejects(
    restarted.create(id, { state: "registered" }),
    /browser_execution_revoked/,
  );
  assert.deepEqual(await restarted.read(id), {
    executionId: id,
    revision: 2,
    data: { state: "finished" },
  });
  assert.deepEqual((await restarted.read(unused)).data, { state: "registered" });
});

test("stale cleanup cannot overwrite a newer worker binding", async (t) => {
  const { journal } = await fixture(t);
  const id = randomUUID();
  await journal.create(id, { incarnation: "old", state: "active" });
  const [current, stale] = await Promise.allSettled([
    journal.update(id, 1, { incarnation: "new", state: "active" }),
    journal.update(id, 1, { incarnation: "old", state: "finished" }),
  ]);
  assert.equal(current.status, "fulfilled");
  assert.equal(stale.status, "rejected");
  assert.match(stale.reason.message, /browser_execution_record_changed/);
  assert.deepEqual((await journal.read(id)).data, {
    incarnation: "new",
    state: "active",
  });
});

test("corruption or installation mismatch rejects recovery without replacing the record", async (t) => {
  const { journal, options, directory } = await fixture(t);
  const id = randomUUID();
  await journal.create(id, { state: "active", profileId: "recoverable" });
  const path = join(directory, `${id}.json`);
  const original = await readFile(path, "utf8");
  await assert.rejects(
    createExecutionJournal({ ...options, installationId: "other" }),
    /browser_execution_journal_corrupt/,
  );
  assert.equal(await readFile(path, "utf8"), original);
  await writeFile(path, "{broken");
  await assert.rejects(createExecutionJournal(options));
  assert.equal(await readFile(path, "utf8"), "{broken");
});

test("fence capacity rejects new identities rather than evicting finished ones", async (t) => {
  const { journal, options } = await fixture(t, 1);
  const id = randomUUID();
  await journal.create(id, { state: "finished" });
  await assert.rejects(
    journal.create(randomUUID(), { state: "registered" }),
    /browser_execution_journal_full/,
  );
  const restarted = await createExecutionJournal(options);
  await assert.rejects(
    restarted.create(id, { state: "registered" }),
    /browser_execution_revoked/,
  );
});

test("invalid and oversized writes preserve the last confirmed record", async (t) => {
  const { journal } = await fixture(t);
  const id = randomUUID();
  await journal.create(id, { state: "active" });
  await assert.rejects(
    journal.update(id, 1, { value: "x".repeat(65536) }),
    /browser_execution_record_too_large/,
  );
  assert.deepEqual((await journal.read(id)).data, { state: "active" });
  await assert.rejects(journal.update(id, 1, null), /browser_execution_record_invalid/);
  assert.deepEqual((await journal.read(id)).data, { state: "active" });
});
