import { Database } from "./database";
import { Logger } from "./logger";

const BATCH_SIZE = 1000;
const MAX_ITERATIONS_PER_RUN = 50;

export function createCleanup(database: Database, logger: Logger, options: CleanupOptions) {
  const { retentionMs, partitionKey } = options;

  return {
    async cleanup() {
      let totalDeleted = 0;
      let iterations = 0;

      while (iterations < MAX_ITERATIONS_PER_RUN) {
        const deleted = await database.deleteCompletedJobsOlderThan(partitionKey, retentionMs, BATCH_SIZE);
        totalDeleted += deleted;
        iterations += 1;

        if (deleted < BATCH_SIZE) break;
      }

      if (totalDeleted > 0) {
        logger.info({ batchCount: iterations, totalDeleted }, "cleanup.completedJobsDeleted");
      } else {
        logger.debug("cleanup.noCompletedJobsToDelete");
      }
    },
  };
}

interface CleanupOptions {
  retentionMs: number;
  partitionKey: string;
}
