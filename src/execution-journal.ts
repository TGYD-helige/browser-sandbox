import { randomUUID } from "node:crypto";
import { constants } from "node:fs";
import { link, mkdir, open, opendir, rename, unlink } from "node:fs/promises";
import { join } from "node:path";

const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const MAX_RECORD_BYTES = 65536;
export interface ExecutionRecord {
  executionId: string;
  revision: number;
  data: Record<string, unknown>;
}

/** Single-controller durable identities. Finished records are fences, never automatic GC candidates. */
export async function createExecutionJournal({
  directory,
  installationId,
  maxRecords = 65536,
}: {
  directory: string;
  installationId: string;
  maxRecords?: number;
}) {
  if (!installationId || !Number.isSafeInteger(maxRecords) || maxRecords < 1)
    throw Error("browser_execution_journal_invalid");
  await mkdir(directory, { recursive: true, mode: 0o700 });
  const ids = new Set<string>();
  const mutations = new Map<string, Promise<unknown>>();
  const filename = (id: string) => {
    if (!uuid.test(id)) throw Error("browser_execution_id_invalid");
    return join(directory, `${id}.json`);
  };
  const read = async (id: string): Promise<ExecutionRecord> => {
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
      return { executionId: id, revision: body.revision, data: body.data };
    } finally {
      await file.close();
    }
  };
  const entries = await opendir(directory);
  for await (const entry of entries) {
    if (entry.name.startsWith(".pending-")) continue;
    const id = entry.name.slice(0, -5);
    if (!entry.name.endsWith(".json") || !uuid.test(id))
      throw Error("browser_execution_journal_corrupt");
    if (ids.size >= maxRecords) throw Error("browser_execution_journal_full");
    await read(id);
    ids.add(id);
  }
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
    const body = JSON.stringify({
      protocol: "browser-sandbox.execution/v1",
      installationId,
      ...record,
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
      const folder = await open(directory, constants.O_RDONLY | constants.O_DIRECTORY);
      try {
        await folder.sync();
      } finally {
        await folder.close();
      }
    } finally {
      await unlink(temporary).catch((error) => {
        if (error.code !== "ENOENT") throw error;
      });
    }
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
  return {
    read,
    ids: () => [...ids],
    create(id: string, data: Record<string, unknown>) {
      filename(id);
      if (ids.has(id)) return Promise.reject(Error("browser_execution_revoked"));
      if (ids.size >= maxRecords)
        return Promise.reject(Error("browser_execution_journal_full"));
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
