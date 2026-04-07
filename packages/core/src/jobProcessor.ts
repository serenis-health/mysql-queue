import { CallbackContext, Job, JobWithQueueName, Queue, Session, WorkerCallback } from "./types";
import { Database } from "./database";
import { errorToJson } from "./utils";
import { executeJobsConcurrently } from "./concurrentJobProcessor";
import { Logger } from "./logger";
import { PoolConnection } from "mysql2/promise";

export function JobProcessor(
  database: Database,
  logger: Logger,
  queue: Queue,
  callback: WorkerCallback,
  workerAbortSignal: AbortSignal,
  options: JobProcessorOptions,
) {
  return {
    async process(): Promise<boolean> {
      if (workerAbortSignal.aborted) return false;
      if (await database.isQueuePaused(queue.id)) return false;
      const start = Date.now();

      const jobs = await claimJobsForProcessing();
      if (!jobs) return false;

      jobs.forEach(async (job) => {
        try {
          await options.onJobClaimed?.({ ...job, queueName: queue.name });
        } catch (error) {
          logger.error({ error: errorToJson(error as Error), jobId: job.id }, `jobProcessor.onJobClaimed.error`);
        }
      });

      const result = await executeJobsConcurrently(jobs, options.callbackBatchSize, workerAbortSignal, executeCallbackWithTimeout);

      await persistResults(result.successful.ids, result.failed);

      logger.debug({ elapsedSeconds: (Date.now() - start) / 1000, jobCount: jobs.length }, `jobProcessor.processed`);
      jobs.forEach(async (job) => {
        try {
          await options.onJobProcessed?.({ ...job, queueName: queue.name });
        } catch (error) {
          logger.error({ error: errorToJson(error as Error), jobId: job.id }, `jobProcessor.onJobProcessed.error`);
        }
      });
      return true;
    },
  };

  async function claimJobsForProcessing() {
    return await database.runWithPoolConnection(async (connection) => {
      return await database.runTransaction(async (trx) => {
        const jobs = (await database.getPendingJobs(trx, queue.id, options.pollingBatchSize)) as Job[];
        if (jobs.length === 0) return;

        const jobIds = jobs.map((job) => job.id);
        await database.markJobsAsRunning(trx, jobIds);
        logger.debug({ jobIds }, `jobProcessor.jobsClaimed`);
        return jobs;
      }, connection);
    });
  }

  async function executeCallbackWithTimeout(
    jobs: Job[],
    jobIds: string[],
    callbackAbortController: AbortController,
  ): Promise<{ shouldMarkAsCompleted: boolean }> {
    let shouldMarkAsCompleted = true;
    const context = createCallbackContext();

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

    return { shouldMarkAsCompleted };

    function createCallbackContext() {
      return {
        async markJobsAsCompleted(session: Session) {
          shouldMarkAsCompleted = false;
          const affectedRows = await database.markJobsAsCompleted(session, jobIds);
          if (affectedRows < jobIds.length) throw new Error(`Jobs may have already timed out or been cancelled`);
          logger.debug({ jobIds }, `jobProcessor.process.markedJobAsCompletedWithSession`);
        },
      } satisfies CallbackContext;
    }
  }

  async function persistResults(successfulJobIds: string[], failedJobsData: Array<{ ids: string[]; error: Error; jobs: Job[] }>) {
    await database.runWithPoolConnection(async (connection) => {
      await database.runTransaction(async (trx) => {
        if (successfulJobIds.length > 0) {
          await database.markJobsAsCompleted(connectionToSession(trx), successfulJobIds);
          logger.debug({ jobIds: successfulJobIds }, `jobProcessor.jobsMarkedAsCompleted`);
        }

        if (failedJobsData.length > 0) {
          for (const { error, ids, jobs: chunkJobs } of failedJobsData) {
            await database.failJobs(trx, ids, queue.maxRetries, queue.minDelayMs, queue.backoffMultiplier, errorToJson(error));
            logger.error({ error: errorToJson(error), jobIds: ids }, `jobProcessor.processBatch.jobsChunkMarkedAsFailed`);
            chunkJobs
              .filter((j) => j.attempts + 1 >= queue.maxRetries)
              .forEach(async (j) => {
                try {
                  await options.onJobFailed?.(error, { id: j.id, queueName: queue.name });
                } catch (hookError) {
                  logger.error({ error: errorToJson(hookError as Error), jobId: j.id }, `jobProcessor.onJobFailed.error`);
                }
              });
          }
        }
      }, connection);
    });
  }
}

export function connectionToSession(connection: PoolConnection): Session {
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

export type JobProcessorOptions = {
  callbackBatchSize: number;
  onJobClaimed?: (job: JobWithQueueName) => void;
  onJobFailed?: (error: Error, job: { id: string; queueName: string }) => void;
  onJobProcessed?: (job: JobWithQueueName) => void;
  pollingBatchSize: number;
  pollingIntervalMs: number;
};
