import { errorToJson, truncateStr } from "./utils";
import { Job, JobWithQueueName, Queue, Session, WorkerCallback } from "./types";
import { Database } from "./database";
import { Logger } from "./logger";
import { PoolConnection } from "mysql2/promise";
import { randomUUID } from "node:crypto";

export function JobProcessor(
  database: Database,
  logger: Logger,
  queue: Queue,
  callback: WorkerCallback,
  onJobProcessed?: (job: JobWithQueueName) => void,
  onJobFailed?: (error: Error, job: { id: string; queueName: string }) => void,
) {
  return {
    async processBatch(batchSize = 1, workerAbortSignal: AbortSignal) {
      if (workerAbortSignal?.aborted) {
        logger.warn("jobProcessor.processBatch.abortedBeforeFetching");
        return;
      }

      const callbackAbortControllers = new Map<string, AbortController>();

      function onWorkerAbort() {
        logger.warn("jobProcessor.processBatch.workerAborted");
        for (const controller of callbackAbortControllers.values()) {
          controller.abort();
        }
        callbackAbortControllers.clear();
      }

      workerAbortSignal.addEventListener("abort", onWorkerAbort);

      try {
        if (workerAbortSignal.aborted) return;
        await database.runWithPoolConnection(async (connection) => {
          const transactionId = randomUUID();
          const start = Date.now();
          try {
            await connection.query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
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
              await Promise.all(
                batch.map((job) => executeCallbackAndHandleStatusUpdate(job, workerAbortSignal, connection, callbackAbortControllers)),
              );
            }

            await connection.commit();
            const elapsedSeconds = (Date.now() - start) / 1000;
            logger.debug({ elapsedSeconds, jobCount, jobIds, transactionId }, `jobProcessor.processBatch.committed`);
            jobs.forEach((j) => onJobProcessed?.({ ...j, queueName: queue.name }));
          } catch (error: unknown) {
            const typedError = error as Error;
            logger.error({ error: errorToJson(typedError), transactionId }, `jobProcessor.processBatch.error`);
            await connection.rollback();
            throw error;
          }
        });
      } finally {
        workerAbortSignal.removeEventListener("abort", onWorkerAbort);
      }
    },
  };

  async function executeCallbackAndHandleStatusUpdate(
    job: Job,
    workerAbortSignal: AbortSignal,
    connection: PoolConnection,
    callbackAbortControllers: Map<string, AbortController>,
  ) {
    try {
      await executeCallbackWithTimeout(connection, job, workerAbortSignal, callbackAbortControllers);
    } catch (error: unknown) {
      await handleCallbackError(error, connection, job, workerAbortSignal);
    }
  }

  async function executeCallbackWithTimeout(
    connection: PoolConnection,
    job: Job,
    workerAbortSignal: AbortSignal,
    callbackAbortControllers: Map<string, AbortController>,
  ) {
    const callbackAbortController = new AbortController();
    let timeoutId: NodeJS.Timeout;

    callbackAbortControllers.set(job.id, callbackAbortController);

    const callbackPromise = callback(job, callbackAbortController.signal, createSessionWrapper(connection));
    const timeoutPromise = new Promise((_, reject) => {
      timeoutId = setTimeout(() => {
        callbackAbortController.abort();
        reject(new Error(`Job execution exceed the timeout of ${queue.maxDurationMs}`));
        logger.warn({ jobId: job.id }, `jobProcessor.process.abortedDueTimeout`);
      }, queue.maxDurationMs);
    });

    await Promise.race([callbackPromise, timeoutPromise]).finally(() => {
      clearTimeout(timeoutId);
      callbackAbortControllers.delete(job.id);
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
      onJobFailed?.(typedError, { id: job.id, queueName: queue.name });
    }
  }
}

function createSessionWrapper(connection: PoolConnection): Session {
  return {
    execute: async (sql: string, parameters: unknown[]) => {
      const [result] = await connection.execute(sql, parameters);
      return [result as { affectedRows: number }];
    },
    query: async <TRow = unknown>(sql: string, parameters: unknown[]) => {
      const [rows] = await connection.query(sql, parameters);
      return rows as TRow[];
    },
  };
}
