import { opendir } from "node:fs/promises";
import { isDeepStrictEqual } from "node:util";
import {
  type ExecutionRecord,
  isPendingExecutionFile,
  readExecutionRecord,
} from "./execution-journal.js";
import type { KubernetesOwnership } from "./kubernetes-ownership.js";

/** Read every old entry before publishing any state; never rewrite, compact, or chmod the source. */
export async function importExecutionJournal(options: {
  directory: string;
  installationId: string;
  ownership: KubernetesOwnership;
  validate(data: unknown): void;
}) {
  if (await options.ownership.migrationComplete()) return 0;
  const entries: ExecutionRecord[] = [];
  let directory: Awaited<ReturnType<typeof opendir>>;
  try {
    directory = await opendir(options.directory);
  } catch (error) {
    if (error.code === "ENOENT") return 0;
    throw error;
  }
  for await (const entry of directory) {
    if (isPendingExecutionFile(entry.name)) continue;
    if (
      !/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\.json$/.test(
        entry.name,
      ) ||
      entries.length >= 65536
    )
      throw Error("browser_execution_journal_corrupt");
    const { record } = await readExecutionRecord(
      options.directory,
      options.installationId,
      entry.name.slice(0, -5),
    );
    options.validate(record.data);
    entries.push(record);
  }
  for (const record of entries) {
    const previous = await options.ownership.journal.read(record.executionId);
    if (previous) {
      // A completed import may have subsequently advanced through ordinary lifecycle writes.
      if (
        previous.revision < record.revision ||
        (previous.revision === record.revision &&
          !isDeepStrictEqual(previous.data, record.data))
      )
        throw Error("browser_execution_migration_conflict");
      options.validate(previous.data);
      if (options.ownership.owns(record.executionId))
        await options.ownership.indexLegacy(previous);
      continue;
    }
    const owner = await options.ownership.resolve(record.executionId, true);
    if (!options.ownership.owns(record.executionId) || !owner)
      throw Error("browser_controller_owner_moved");
    await options.ownership.importRecord(record);
    await options.ownership.indexLegacy(record);
    if (record.data.profileId && record.data.phase !== "finished")
      await options.ownership.acquireProfile(
        record.executionId,
        String(record.data.profileId),
      );
  }
  await options.ownership.finishMigration();
  return entries.length;
}
