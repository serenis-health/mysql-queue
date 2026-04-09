import { CallbackContext, Job, JobWithQueueName, Queue, Session, WorkerCallback } from "./types";
import { Database } from "./database";
import { errorToJson } from "./utils";
import { executeJobsConcurrently } from "./concurrentJobProcessor";
import { Logger } from "./logger";
import { Metrics } from "./metrics";
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
      const claimedAt = Date.now();

      for (const job of jobs) {
        const eligibleAt = Math.max(job.createdAt.getTime(), job.startAfter.getTime());
        options.metrics.jobQueueWaitTime(queue.name, job.name, (claimedAt - eligibleAt) / 1000);
      }
      for (const [jobName, count] of countByName(jobs)) {
        options.metrics.jobsClaimed(queue.name, jobName, count);
      }

      await Promise.all(
        jobs.map(async (job) => {
          try {
            await options.onJobClaimed?.({ ...job, queueName: queue.name });
          } catch (error) {
            logger.error({ error: errorToJson(error as Error), jobId: job.id }, `jobProcessor.onJobClaimed.error`);
          }
        }),
      );

      const executionStart = Date.now();
      const result = await executeJobsConcurrently(jobs, options.callbackBatchSize, workerAbortSignal, executeCallbackWithTimeout);
      const executionSeconds = (Date.now() - executionStart) / 1000;

      await persistResults(result.successful, result.failed);

      for (const [jobName, count] of countByName(result.manuallyCompleted.jobs)) {
        options.metrics.jobsCompleted(queue.name, jobName, count);
      }

      const allProcessedJobs = [...result.successful.jobs, ...result.manuallyCompleted.jobs, ...result.failed.flatMap((f) => f.jobs)];
      for (const job of allProcessedJobs) {
        options.metrics.jobExecutionTime(queue.name, job.name, executionSeconds);
      }

      const elapsedSeconds = (Date.now() - start) / 1000;
      options.metrics.jobProcessingDuration(queue.name, elapsedSeconds);
      logger.debug({ elapsedSeconds, jobCount: jobs.length }, `jobProcessor.processed`);

      await Promise.all(
        jobs.map(async (job) => {
          try {
            await options.onJobProcessed?.({ ...job, queueName: queue.name });
          } catch (error) {
            logger.error({ error: errorToJson(error as Error), jobId: job.id }, `jobProcessor.onJobProcessed.error`);
          }
        }),
      );

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

  async function persistResults(
    successful: { ids: string[]; jobs: Job[] },
    failedJobsData: Array<{ ids: string[]; error: Error; jobs: Job[] }>,
  ) {
    const terminalJobsToNotify: Array<{ error: Error; job: Job }> = [];
    const retriedJobs: Job[] = [];

    await database.runWithPoolConnection(async (connection) => {
      await database.runTransaction(async (trx) => {
        if (successful.ids.length > 0) {
          await database.markJobsAsCompleted(connectionToSession(trx), successful.ids);
          logger.debug({ jobIds: successful.ids }, `jobProcessor.jobsMarkedAsCompleted`);
        }

        if (failedJobsData.length > 0) {
          for (const { error, ids, jobs: chunkJobs } of failedJobsData) {
            await database.failJobs(trx, ids, queue.maxRetries, queue.minDelayMs, queue.backoffMultiplier, errorToJson(error));
            logger.error({ error: errorToJson(error), jobIds: ids }, `jobProcessor.processBatch.jobsChunkMarkedAsFailed`);
            const terminalJobs = chunkJobs.filter((j) => j.attempts + 1 >= queue.maxRetries);
            retriedJobs.push(...chunkJobs.filter((j) => j.attempts + 1 < queue.maxRetries));
            terminalJobs.forEach((j) => {
              terminalJobsToNotify.push({ error, job: j });
            });
          }
        }
      }, connection);
    });

    for (const [jobName, count] of countByName(successful.jobs)) {
      options.metrics.jobsCompleted(queue.name, jobName, count);
    }
    for (const [jobName, count] of countByName(terminalJobsToNotify.map((t) => t.job))) {
      options.metrics.jobsFailed(queue.name, jobName, count);
    }
    for (const [jobName, count] of countByName(retriedJobs)) {
      options.metrics.jobsRetried(queue.name, jobName, count);
    }

    // Call onJobFailed hooks after transaction is closed
    await Promise.all(
      terminalJobsToNotify.map(async ({ error, job }) => {
        try {
          await options.onJobFailed?.(error, { id: job.id, queueName: queue.name });
        } catch (hookError) {
          logger.error({ error: errorToJson(hookError as Error), jobId: job.id }, `jobProcessor.onJobFailed.error`);
        }
      }),
    );
  }
}

function countByName(jobs: Job[]): Map<string, number> {
  const counts = new Map<string, number>();
  for (const job of jobs) counts.set(job.name, (counts.get(job.name) ?? 0) + 1);
  return counts;
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
  metrics: Metrics;
  onJobClaimed?: (job: JobWithQueueName) => void | Promise<void>;
  onJobFailed?: (error: Error, job: { id: string; queueName: string }) => void | Promise<void>;
  onJobProcessed?: (job: JobWithQueueName) => void | Promise<void>;
  pollingBatchSize: number;
  pollingIntervalMs: number;
};
