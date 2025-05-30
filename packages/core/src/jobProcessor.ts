import { errorToJson, truncateStr } from "./utils";
import { Job, Queue, WorkerCallback } from "./types";
import { Database } from "./database";
import { Logger } from "./logger";
import { PoolConnection } from "mysql2/promise";
import { randomUUID } from "node:crypto";

export function JobProcessor(database: Database, logger: Logger, queue: Queue, callback: WorkerCallback) {
  return {
    async processBatch(batchSize = 1, workerAbortSignal: AbortSignal) {
      if (workerAbortSignal?.aborted) {
        logger.warn("jobProcessor.processBatch.abortedBeforeFetching");
        return;
      }

      await database.runWithPoolConnection(async (connection) => {
        const transactionId = randomUUID();
        try {
          await connection.beginTransaction();
          const jobs = (await database.getPendingJobs(connection, queue.id, batchSize)) as Job[];
          if (jobs.length === 0) {
            await connection.commit();
            return;
          }
          const jobIds = jobs.map((job) => job.id);
          const jobCount = jobs.length;
          logger.debug({ jobCount, jobIds, transactionId }, `jobProcessor.processBatch.pendingJobsFound`);

          const BATCH_SIZE = 10;
          for (let i = 0; i < jobs.length; i += BATCH_SIZE) {
            const batch = jobs.slice(i, i + BATCH_SIZE);
            await Promise.all(batch.map((job) => executeCallbackAndHandleStatusUpdate(job, workerAbortSignal, connection)));
          }

          await connection.commit();
          logger.debug({ jobCount, jobIds, transactionId }, `jobProcessor.processBatch.commited`);
        } catch (error: unknown) {
          await connection.rollback();
          const typedError = error as Error;
          logger.error({ error: errorToJson(typedError), transactionId }, `jobProcessor.processBatch.error`);
          throw error;
        }
      });
    },
  };

  async function executeCallbackAndHandleStatusUpdate(job: Job, workerAbortSignal: AbortSignal, connection: PoolConnection) {
    try {
      await executeCallbackWithTimeout(connection, job, workerAbortSignal);
    } catch (error: unknown) {
      await handleCallbackError(error, connection, job, workerAbortSignal);
    }
  }

  async function executeCallbackWithTimeout(connection: PoolConnection, job: Job, workerAbortSignal: AbortSignal) {
    const callbackAbortController = new AbortController();
    let timeoutId: NodeJS.Timeout;

    function onWorkerAbort() {
      callbackAbortController.abort();
      if (timeoutId) clearTimeout(timeoutId);
      logger.warn({ jobId: job.id }, `jobProcessor.process.abortedDueWorkerAbort`);
    }
    workerAbortSignal.addEventListener("abort", onWorkerAbort);

    const callbackPromise = callback(job, callbackAbortController.signal, connection);
    const timeoutPromise = new Promise((_, reject) => {
      timeoutId = setTimeout(() => {
        callbackAbortController.abort();
        reject(new Error(`Job execution exceed the timeout of ${queue.maxDurationMs}`));
        logger.warn({ jobId: job.id }, `jobProcessor.process.abortedDueTimeout`);
      }, queue.maxDurationMs);
    });

    await Promise.race([callbackPromise, timeoutPromise]).finally(() => {
      clearTimeout(timeoutId);
      workerAbortSignal.removeEventListener("abort", onWorkerAbort);
    });
    await database.markJobAsCompleted(connection, job.id, job.attempts);
    logger.debug({ jobId: job.id }, `jobProcessor.process.markedJobAsCompleted`);
  }

  async function handleCallbackError(error: unknown, connection: PoolConnection, job: Job, workerAbortSignal: AbortSignal) {
    if (workerAbortSignal.aborted) return;
    const typedError = error as Error;
    if (job.attempts < queue.maxRetries - 1) {
      const now = Date.now();
      const startAfter = queue.backoffMultiplier
        ? new Date(now + queue.minDelayMs * Math.pow(queue.backoffMultiplier, job.attempts))
        : new Date(now + queue.minDelayMs);
      await database.incrementJobAttempts(connection, job.id, truncateStr(typedError.message, 85), job.attempts, startAfter);
      logger.warn(
        {
          error: errorToJson(typedError),
          jobId: job.id,
          retryInSeconds: Math.floor((startAfter.getTime() - now) / 1000),
          startAfter,
        },
        `jobProcessor.process.incrementedAttempts`,
      );
    } else {
      await database.markJobAsFailed(connection, job.id, truncateStr(typedError.message, 85), job.attempts);
      logger.error({ error: errorToJson(typedError), jobId: job.id }, `jobProcessor.process.markedAsFailed`);
    }
  }
}
