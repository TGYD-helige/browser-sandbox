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
  await journal.create(id, { phase: "registered" });
  await journal.create(unused, { phase: "registered" });
  await journal.update(id, 1, { phase: "finished" });
  const restarted = await createExecutionJournal(options);
  await assert.rejects(
    restarted.create(id, { phase: "registered" }),
    /browser_execution_revoked/,
  );
  assert.deepEqual(await restarted.read(id), {
    executionId: id,
    revision: 2,
    data: { phase: "finished" },
  });
  assert.deepEqual((await restarted.read(unused)).data, { phase: "registered" });
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

test("finished fences compact without evicting active execution records", async (t) => {
  const { journal, options } = await fixture(t, 2);
  const active = randomUUID(),
    oldest = randomUUID(),
    recent = randomUUID();
  await journal.create(active, { phase: "active" });
  await journal.create(oldest, { phase: "finished" });
  await journal.create(recent, { phase: "finished" });
  await assert.rejects(journal.read(oldest), { code: "ENOENT" });
  assert.deepEqual((await journal.read(active)).data, { phase: "active" });
  await assert.rejects(
    journal.create(recent, { phase: "registered" }),
    /browser_execution_revoked/,
  );
  const restarted = await createExecutionJournal(options);
  await restarted.create(randomUUID(), { phase: "registered" });
  assert.deepEqual((await restarted.read(active)).data, { phase: "active" });
  await assert.rejects(
    restarted.create(randomUUID(), { phase: "registered" }),
    /browser_execution_capacity_exceeded/,
  );
  assert.equal(restarted.ids().length, 2);
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

test("concurrent admissions compact finished fences without spurious capacity errors", async (t) => {
  const { journal } = await fixture(t, 2);
  await journal.create(randomUUID(), { phase: "finished" });
  await journal.create(randomUUID(), { phase: "finished" });
  const ids = [randomUUID(), randomUUID()];
  await Promise.all(ids.map((id) => journal.create(id, { phase: "registered" })));
  for (const id of ids)
    assert.deepEqual((await journal.read(id)).data, { phase: "registered" });
  assert.equal(journal.ids().length, 2);
});

test("restart removes abandoned pending files and preserves confirmed records", async (t) => {
  const { journal, options, directory } = await fixture(t);
  const id = randomUUID();
  await journal.create(id, { phase: "active", profileId: "recoverable" });
  const pending = join(directory, `.pending-${randomUUID()}`);
  await writeFile(pending, "incomplete write");
  const restarted = await createExecutionJournal(options);
  assert.deepEqual((await restarted.read(id)).data, {
    phase: "active",
    profileId: "recoverable",
  });
  await assert.rejects(readFile(pending), { code: "ENOENT" });
});

test("a v1 finished record without a timestamp restores and compacts safely", async (t) => {
  const { directory, options } = await fixture(t, 2);
  const old = randomUUID(),
    active = randomUUID();
  await writeFile(
    join(directory, `${old}.json`),
    JSON.stringify({
      protocol: "browser-sandbox.execution/v1",
      installationId: options.installationId,
      executionId: old,
      revision: 2,
      data: { phase: "finished" },
    }),
  );
  const restarted = await createExecutionJournal(options);
  assert.deepEqual((await restarted.read(old)).data, { phase: "finished" });
  await restarted.create(active, { phase: "active", profileId: "recoverable" });
  await restarted.create(randomUUID(), { phase: "active" });
  assert.deepEqual((await restarted.read(active)).data, {
    phase: "active",
    profileId: "recoverable",
  });
  await assert.rejects(restarted.read(old), { code: "ENOENT" });
});

test("compaction cannot delete a record whose serialized update still needs cleanup", async (t) => {
  const { journal } = await fixture(t, 1);
  const id = randomUUID();
  await journal.create(id, { phase: "finished" });
  const [updated, admission] = await Promise.allSettled([
    journal.update(id, 1, { phase: "cleanup-pending" }),
    journal.create(randomUUID(), { phase: "registered" }),
  ]);
  assert.equal(updated.status, "fulfilled");
  assert.equal(admission.status, "rejected");
  assert.match(admission.reason.message, /browser_execution_capacity_exceeded/);
  assert.deepEqual((await journal.read(id)).data, { phase: "cleanup-pending" });
});

test("startup streams validated records to recovery without a retained body cache", async (t) => {
  const { journal, options } = await fixture(t);
  const id = randomUUID();
  await journal.create(id, { phase: "active", profileId: "original" });
  const recovered = [];
  await createExecutionJournal({
    ...options,
    async onRecord(record) {
      await Promise.resolve();
      recovered.push(record);
    },
  });
  assert.deepEqual(recovered, [
    { executionId: id, revision: 1, data: { phase: "active", profileId: "original" } },
  ]);
});
