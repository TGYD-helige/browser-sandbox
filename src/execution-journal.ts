import { randomUUID } from "node:crypto";
import { constants } from "node:fs";
import { chmod, link, mkdir, open, opendir, rename, unlink } from "node:fs/promises";
import { join } from "node:path";

const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const MAX_RECORD_BYTES = 65536;
export interface ExecutionRecord {
  executionId: string;
  revision: number;
  data: Record<string, unknown>;
}

/** Single-controller durable identities with bounded recent finished fences. */
export async function createExecutionJournal({
  directory,
  installationId,
  maxRecords = 65536,
  onRecord,
}: {
  directory: string;
  installationId: string;
  maxRecords?: number;
  onRecord?: (record: ExecutionRecord) => Promise<void> | void;
}) {
  if (
    !installationId ||
    !Number.isSafeInteger(maxRecords) ||
    maxRecords < 1 ||
    maxRecords > 65536
  )
    throw Error("browser_execution_journal_invalid");
  await mkdir(directory, { recursive: true, mode: 0o700 });
  await chmod(directory, 0o700);
  const ids = new Set<string>();
  let finished = new Map<string, number>();
  const mutations = new Map<string, Promise<unknown>>();
  const filename = (id: string) => {
    if (!uuid.test(id)) throw Error("browser_execution_id_invalid");
    return join(directory, `${id}.json`);
  };
  const readRecord = async (id: string) => {
    const file = await open(
      filename(id),
      constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK,
    );
    try {
      const stat = await file.stat();
      if (!stat.isFile() || stat.size > MAX_RECORD_BYTES)
        throw Error("browser_execution_journal_corrupt");
      const body = JSON.parse(await file.readFile("utf8"));
      if (
        body.protocol !== "browser-sandbox.execution/v1" ||
        body.installationId !== installationId ||
        body.executionId !== id ||
        !Number.isSafeInteger(body.revision) ||
        body.revision < 1 ||
        !body.data ||
        typeof body.data !== "object" ||
        Array.isArray(body.data)
      )
        throw Error("browser_execution_journal_corrupt");
      const finishedAt =
        body.data.phase === "finished"
          ? (body.finishedAt ?? Math.floor(stat.mtimeMs))
          : undefined;
      if (
        body.finishedAt !== undefined &&
        (body.data.phase !== "finished" ||
          !Number.isSafeInteger(body.finishedAt) ||
          body.finishedAt < 0)
      )
        throw Error("browser_execution_journal_corrupt");
      return {
        record: {
          executionId: id,
          revision: body.revision,
          data: body.data,
        } as ExecutionRecord,
        finishedAt: finishedAt as number | undefined,
      };
    } finally {
      await file.close();
    }
  };
  const read = async (id: string): Promise<ExecutionRecord> =>
    (await readRecord(id)).record;
  const syncDirectory = async () => {
    const folder = await open(directory, constants.O_RDONLY | constants.O_DIRECTORY);
    try {
      await folder.sync();
    } finally {
      await folder.close();
    }
  };
  const write = async (record: ExecutionRecord, create: boolean) => {
    const target = filename(record.executionId);
    if (
      !record.data ||
      typeof record.data !== "object" ||
      Array.isArray(record.data) ||
      !Number.isSafeInteger(record.revision) ||
      record.revision < 1
    )
      throw Error("browser_execution_record_invalid");
    const finishedAt =
      record.data.phase === "finished"
        ? (finished.get(record.executionId) ?? Date.now())
        : undefined;
    const body = JSON.stringify({
      protocol: "browser-sandbox.execution/v1",
      installationId,
      ...record,
      finishedAt,
    });
    if (Buffer.byteLength(body) > MAX_RECORD_BYTES)
      throw Error("browser_execution_record_too_large");
    const temporary = join(directory, `.pending-${randomUUID()}`);
    const file = await open(temporary, "wx", 0o600);
    try {
      await file.writeFile(body);
      await file.sync();
    } finally {
      await file.close();
    }
    try {
      if (create) await link(temporary, target);
      else await rename(temporary, target);
      await syncDirectory();
    } finally {
      await unlink(temporary).catch((error) => {
        if (error.code !== "ENOENT") throw error;
      });
    }
    if (finishedAt !== undefined) finished.set(record.executionId, finishedAt);
    else finished.delete(record.executionId);
  };
  const serialize = <T>(id: string, action: () => Promise<T>): Promise<T> => {
    const work = (mutations.get(id) ?? Promise.resolve()).then(action);
    const settled = work
      .catch(() => {})
      .finally(() => {
        if (mutations.get(id) === settled) mutations.delete(id);
      });
    mutations.set(id, settled);
    return work;
  };
  let compacting: Promise<unknown> = Promise.resolve();
  const compact = (reserve: number) => {
    const work = compacting.then(async () => {
      let removed = 0;
      while (ids.size + reserve > maxRecords && finished.size) {
        const id = finished.keys().next().value;
        await serialize(id, async () => {
          if (!finished.has(id)) return;
          await unlink(filename(id));
          await syncDirectory();
          finished.delete(id);
          ids.delete(id);
          removed++;
        });
      }
      return removed;
    });
    compacting = work.catch(() => {});
    return work;
  };
  let removedPending = false;
  const entries = await opendir(directory);
  for await (const entry of entries) {
    if (entry.name.startsWith(".pending-") && uuid.test(entry.name.slice(9))) {
      await unlink(join(directory, entry.name));
      removedPending = true;
      continue;
    }
    const id = entry.name.slice(0, -5);
    if (!entry.name.endsWith(".json") || !uuid.test(id))
      throw Error("browser_execution_journal_corrupt");
    if (ids.size >= 65536) throw Error("browser_execution_capacity_exceeded");
    const loaded = await readRecord(id);
    ids.add(id);
    if (loaded.finishedAt !== undefined) finished.set(id, loaded.finishedAt);
    await onRecord?.(loaded.record);
  }
  if (removedPending) await syncDirectory();
  finished = new Map([...finished].sort((a, b) => a[1] - b[1]));
  await compact(0);
  return {
    read,
    ids: () => [...ids],
    has: (id: string) => ids.has(id),
    finishedIds: () => [...finished.keys()],
    async create(id: string, data: Record<string, unknown>) {
      filename(id);
      if (ids.has(id)) throw Error("browser_execution_revoked");
      while (ids.size >= maxRecords) {
        const removed = await compact(1);
        if (!removed && ids.size >= maxRecords)
          throw Error("browser_execution_capacity_exceeded");
      }
      if (ids.has(id)) throw Error("browser_execution_revoked");
      // A failed durable write remains fenced until restart resolves its actual on-disk outcome.
      ids.add(id);
      return serialize(id, async () => {
        const record = { executionId: id, revision: 1, data };
        await write(record, true);
        return record;
      });
    },
    update(id: string, revision: number, data: Record<string, unknown>) {
      return serialize(id, async () => {
        const previous = await read(id);
        if (previous.revision !== revision)
          throw Error("browser_execution_record_changed");
        const record = { executionId: id, revision: revision + 1, data };
        await write(record, false);
        return record;
      });
    },
  };
}
