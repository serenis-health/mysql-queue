import { Context, Job, JobWithQueueName, Queue, Session, WorkerCallback } from "./types";
import { Database } from "./database";
import { errorToJson } from "./utils";
import { Logger } from "./logger";
import { PoolConnection } from "mysql2/promise";

export function JobProcessor(
  database: Database,
  logger: Logger,
  queue: Queue,
  callback: WorkerCallback,
  workerAbortSignal: AbortSignal,
  batchSize = 1,
  onJobProcessed?: (job: JobWithQueueName) => void,
  onJobFailed?: (error: Error, job: { id: string; queueName: string }) => void,
) {
  return {
    async processBatch() {
      if (workerAbortSignal.aborted) return;
      if (await database.isQueuePaused(queue.id)) return;

      const start = Date.now();

      await database.runWithPoolConnection(async (connection) => {
        const jobs = await database.runTransaction(async (trx) => {
          const jobs = (await database.getPendingJobs(trx, queue.id, batchSize)) as Job[];
          if (jobs.length === 0) return;

          const jobIds = jobs.map((job) => job.id);
          await database.markJobsAsRunning(trx, jobIds);
          logger.debug({ jobIds }, `jobProcessor.processBatch.jobsMarkedAsRunning`);
          return jobs;
        }, connection);

        if (!jobs) return;
        const jobIds = jobs.map((job) => job.id);
        const jobCount = jobs.length;

        try {
          await executeCallbackWithTimeout(connection, jobs, jobIds);
        } catch (error: unknown) {
          const typedError = error as Error;
          await database.failJobs(connection, jobIds, queue.maxRetries, queue.minDelayMs, queue.backoffMultiplier, typedError);
          logger.error({ error: errorToJson(typedError), jobIds }, `jobProcessor.process.jobsFailHandled`);
          jobs
            .filter((j) => j.attempts >= queue.maxRetries - 1)
            .forEach((j) => onJobFailed?.(typedError, { id: j.id, queueName: queue.name }));
        }
        const elapsedSeconds = (Date.now() - start) / 1000;
        logger.debug({ elapsedSeconds, jobCount, jobIds }, `jobProcessor.processBatch.jobsRun`);
        jobs.forEach((j) => onJobProcessed?.({ ...j, queueName: queue.name }));
      });
    },
  };

  async function executeCallbackWithTimeout(connection: PoolConnection, jobs: Job[], jobIds: string[]) {
    const callbackAbortController = new AbortController();
    workerAbortSignal.addEventListener("abort", onWorkerAbort);

    let shouldMarkAsCompleted = true;
    try {
      const context = await createCallbackContext();

      const callbackPromise = callback(jobs, callbackAbortController.signal, context);
      let timeoutId: NodeJS.Timeout;
      const timeoutPromise = new Promise((_, reject) => {
        timeoutId = setTimeout(() => {
          callbackAbortController.abort();
          reject(new Error(`Job execution exceed the timeout of ${queue.maxDurationMs}`));
          logger.warn({ jobIds }, `jobProcessor.process.abortedDueTimeout`);
        }, queue.maxDurationMs);
      });

      await Promise.race([callbackPromise, timeoutPromise]).finally(() => {
        clearTimeout(timeoutId);
      });

      if (shouldMarkAsCompleted) {
        await database.markJobsAsCompleted(createSessionWrapper(connection), jobIds);
        logger.debug({ jobIds }, `jobProcessor.process.markedJobAsCompleted`);
      }
    } finally {
      workerAbortSignal.removeEventListener("abort", onWorkerAbort);
    }

    async function createCallbackContext() {
      return {
        async markJobsAsCompleted(session: Session) {
          shouldMarkAsCompleted = false;
          await database.markJobsAsCompleted(session, jobIds);
          logger.debug({ jobIds }, `jobProcessor.process.markedJobAsCompletedWithSession`);
        },
      } satisfies Context;
    }

    function onWorkerAbort() {
      callbackAbortController.abort();
    }
  }
}

function createSessionWrapper(connection: PoolConnection): Session {
  return {
    execute: async (sql: string, parameters: unknown[]) => {
      const [result] = await connection.query(sql, parameters);
      return [result as { affectedRows: number }];
    },
    query: async <TRow = unknown>(sql: string, parameters: unknown[]) => {
      const [rows] = await connection.query(sql, parameters);
      return rows as TRow[];
    },
  };
}
