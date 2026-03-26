import { Database } from "./database";
import { Logger } from "./logger";

export function createCleanup(database: Database, logger: Logger, options: CleanupOptions) {
  const { retentionDays, partitionKey, batchSize, maxIterationsForRun } = options;

  return {
    async cleanup() {
      let totalDeleted = 0;
      let iterations = 0;
      let deleted: number;

      do {
        deleted = await database.deleteCompletedJobsOlderThan(partitionKey, retentionDays, batchSize);
        totalDeleted += deleted;
        iterations += 1;
      } while (iterations < maxIterationsForRun && deleted >= batchSize);

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
  batchSize: number;
  maxIterationsForRun: number;
}
