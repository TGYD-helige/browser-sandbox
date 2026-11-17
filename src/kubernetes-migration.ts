import { constants } from "node:fs";
import { open, opendir } from "node:fs/promises";
import { join } from "node:path";
import { isDeepStrictEqual } from "node:util";
import type { ExecutionRecord } from "./execution-journal.js";
import type { KubernetesOwnership } from "./kubernetes-ownership.js";

/** Read every old entry before publishing any state; never rewrite, compact, or chmod the source. */
export async function importExecutionJournal(options: {
  directory: string;
  installationId: string;
  ownership: KubernetesOwnership;
  validate(data: unknown): void;
}) {
  const entries: ExecutionRecord[] = [];
  let directory: Awaited<ReturnType<typeof opendir>>;
  try {
    directory = await opendir(options.directory);
  } catch (error) {
    if (error.code === "ENOENT") return 0;
    throw error;
  }
  for await (const entry of directory) {
    if (
      !/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\.json$/.test(
        entry.name,
      ) ||
      entries.length >= 65536
    )
      throw Error("browser_execution_journal_corrupt");
    const file = await open(
      join(options.directory, entry.name),
      constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK,
    );
    try {
      const stat = await file.stat();
      if (!stat.isFile() || stat.size > 65536)
        throw Error("browser_execution_journal_corrupt");
      const value = JSON.parse(await file.readFile("utf8"));
      if (
        value.protocol !== "browser-sandbox.execution/v1" ||
        value.installationId !== options.installationId ||
        value.executionId !== entry.name.slice(0, -5) ||
        !Number.isSafeInteger(value.revision) ||
        value.revision < 1 ||
        (value.finishedAt !== undefined &&
          (value.data?.phase !== "finished" ||
            !Number.isSafeInteger(value.finishedAt) ||
            value.finishedAt < 0))
      )
        throw Error("browser_execution_journal_corrupt");
      options.validate(value.data);
      entries.push({
        executionId: value.executionId,
        revision: value.revision,
        data: value.data,
      });
    } finally {
      await file.close();
    }
  }
  for (const record of entries) {
    const owner = await options.ownership.resolve(record.executionId, true);
    if (!options.ownership.owns(record.executionId) || !owner)
      throw Error("browser_controller_owner_moved");
    const previous = await options.ownership.journal.read(record.executionId);
    if (previous) {
      // A completed import may have subsequently advanced through ordinary lifecycle writes.
      if (
        previous.revision < record.revision ||
        (previous.revision === record.revision &&
          !isDeepStrictEqual(previous.data, record.data))
      )
        throw Error("browser_execution_migration_conflict");
      await options.ownership.indexLegacy(previous);
      continue;
    }
    await options.ownership.importRecord(record);
    await options.ownership.indexLegacy(record);
    if (record.data.profileId && record.data.phase !== "finished")
      await options.ownership.acquireProfile(
        record.executionId,
        String(record.data.profileId),
      );
  }
  return entries.length;
}
