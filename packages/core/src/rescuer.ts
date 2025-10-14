import { Database } from "./database";
import { errorToJson } from "./utils";
import { Logger } from "./logger";
import { RowDataPacket } from "mysql2/promise";

export function createRescuer(database: Database, logger: Logger) {
  let timeout: NodeJS.Timeout;
  let nextRun: Date;

  const stuckHorizonMs = 30_000_000;

  return {
    dispose() {
      clearTimeout(timeout);
    },
    getNextRun() {
      return nextRun;
    },
    initialize() {
      scheduleNextRun();
    },
    rescue,
  };

  async function rescue() {
    const stuckHorizon = new Date(Date.now() - stuckHorizonMs);
    await database.runWithPoolConnection(async (connection) => {
      const query = `SELECT id, queueId
           FROM ${database.jobsTable()}
           WHERE status = 'running'
             AND runningAt < ?
           ORDER BY id
           LIMIT ?`;
      const [stuckJobs] = await connection.query<RowDataPacket[]>(query, [stuckHorizon, 100]);

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
  }

  function scheduleNextRun() {
    const now = new Date();
    const nextHour = new Date(now);
    nextHour.setHours(now.getHours() + 1, 0, 0, 0);
    const delay = nextHour.getTime() - now.getTime();
    nextRun = nextHour;
    logger.debug({ delayInMinutes: Math.round(delay / 60000), nextHour }, "rescuer.nextRunScheduled");

    timeout = setTimeout(() => {
      rescue()
        .catch((e) => {
          logger.error({ ...errorToJson(e) }, "rescuer.error");
        })
        .finally(scheduleNextRun);
    }, delay);
  }
}
