import { Database } from "./database";
import { Logger } from "./logger";
import { RowDataPacket } from "mysql2/promise";

export function createRescuer(database: Database, logger: Logger, options: RescuerOptions) {
  const { batchSize, rescueAfterMs } = options;

  return {
    async rescue() {
      const stuckHorizon = new Date(Date.now() - rescueAfterMs);

      await database.runWithPoolConnection(async (connection) => {
        const [stuckJobs] = await connection.query<RowDataPacket[]>(
          `SELECT id, queueId FROM ${database.jobsTable()} WHERE status = 'running' AND runningAt < ? ORDER BY id LIMIT ?;`,
          [stuckHorizon, batchSize],
        );
        if (stuckJobs.length === 0) {
          logger.debug("rescuer.noStuckJobsFound");
          return;
        }

        const jobsByQueue = stuckJobs.reduce(
          (acc, job) => {
            if (!acc[job.queueId]) acc[job.queueId] = [];
            acc[job.queueId].push(job.id);
            return acc;
          },
          {} as Record<string, string[]>,
        );

        logger.debug({ jobsCount: stuckJobs.length, queuesCount: Object.keys(jobsByQueue).length }, "rescuer.foundStuckJobs");

        for (const [queueId, jobIds] of Object.entries(jobsByQueue)) {
          const queue = await database.getQueueById(connection, queueId);
          if (!queue) throw new Error("Queue not found");

          await database.failJobs(connection, jobIds, queue.maxRetries, queue.minDelayMs, queue.backoffMultiplier, {
            message: "Job stuck in running state and was rescued",
            name: "RescuerError",
          });
          logger.debug({ jobIds, queueId, queueName: queue.name }, "rescuer.failedJobs");
        }
      });
    },
  };
}

interface RescuerOptions {
  rescueAfterMs: number;
  batchSize: number;
}
