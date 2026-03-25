import { Database } from "./database";
import { Logger } from "./logger";

const BATCH_SIZE = 1000;
const MAX_ITERATIONS_PER_RUN = 50;

export function createCleanup(database: Database, logger: Logger, options: CleanupOptions) {
  const { retentionDays, partitionKey } = options;

  return {
    async cleanup() {
      let totalDeleted = 0;
      let iterations = 0;
      let deleted: number;

      do {
        deleted = await database.deleteCompletedJobsOlderThan(partitionKey, retentionDays, BATCH_SIZE);
        totalDeleted += deleted;
        iterations += 1;
      } while (iterations < MAX_ITERATIONS_PER_RUN && deleted >= BATCH_SIZE);

      if (totalDeleted > 0) {
        logger.info({ batchCount: iterations, totalDeleted }, "cleanup.completedJobsDeleted");
      } else {
        logger.debug("cleanup.noCompletedJobsToDelete");
      }
    },
  };
}

interface CleanupOptions {
  retentionDays: number;
  partitionKey: string;
}
