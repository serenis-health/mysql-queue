import { connectionToSession } from "./jobProcessor";
import { CronExpressionParser } from "cron-parser";
import { Database } from "./database";
import { Logger } from "./logger";
import { MysqlQueue } from "./index";
import { PeriodicJob } from "./types";

export function createPeriodic(logger: Logger, enqueue: MysqlQueue["enqueue"], database: Database) {
  const jobs = new Map<string, PeriodicJobInternal>();
  let isRunning = false;
  const pendingExecutions = new Map<string, Promise<void>>();

  return {
    list(): PeriodicJob[] {
      return Array.from(jobs.values()).map((job) => ({
        catchUpStrategy: job.catchUpStrategy,
        cronExpression: job.cronExpression,
        includeScheduledTime: job.includeScheduledTime,
        jobTemplate: job.jobTemplate,
        maxCatchUp: job.maxCatchUp,
        name: job.name,
        nextRunAt: job.nextRunAt,
        targetQueue: job.targetQueue,
      }));
    },
    async register(job: PeriodicJob): Promise<void> {
      CronExpressionParser.parse(job.cronExpression);

      const existingJob = jobs.get(job.name);
      if (existingJob?.timer) clearTimeout(existingJob.timer);

      const state = await database.getPeriodicJobByName(job.name);
      if (state?.lastRunAt) {
        const missedRuns = calculateMissedRuns(job.cronExpression, state.lastRunAt);
        if (missedRuns.length > 0) await handleMissedRuns(job, missedRuns);
      }

      const nextRunAt = getNextRun(job.cronExpression);
      await database.runWithPoolConnection(async (connection) => {
        await database.upsertPeriodicJob(job.name, null, nextRunAt, connection, job);
      });
      const internalJob: PeriodicJobInternal = { ...job, nextRunAt };
      jobs.set(job.name, internalJob);
      logger.debug({ job }, "periodic.jobRegistered");
      scheduleNextOrExecute(internalJob);
    },
    async remove(name: string): Promise<boolean> {
      const job = jobs.get(name);
      if (!job) return false;
      if (job.timer) clearTimeout(job.timer);
      jobs.delete(name);
      await database.deletePeriodicJob(name);
      logger.info({ cronJobName: name }, "periodic.jobRemoved");
      return true;
    },
    start(): void {
      if (isRunning) return;
      isRunning = true;
      logger.info({ jobCount: jobs.size }, "periodic.started");
      for (const job of jobs.values()) scheduleNextOrExecute(job);
    },
    stop(): void {
      if (!isRunning) return;
      isRunning = false;
      for (const job of jobs.values()) {
        if (job.timer) {
          clearTimeout(job.timer);
          job.timer = undefined;
        }
      }
      logger.info("periodic.stopped");
    },
    async waitForPendingExecutions() {
      await Promise.all(pendingExecutions.values());
    },
  };

  async function executeJob(job: PeriodicJobInternal, scheduledTime: Date): Promise<void> {
    if (!isRunning) return;
    if (!jobs.has(job.name)) return;
    await database.runWithPoolConnection((connection) =>
      database.runTransaction(async (connection) => {
        const jobParams = {
          ...job.jobTemplate,
          idempotentKey: generateIdempotentKey(job.name, scheduledTime),
        };

        if (job.includeScheduledTime) {
          jobParams.payload = {
            ...(typeof job.jobTemplate.payload === "object" && job.jobTemplate.payload !== null ? job.jobTemplate.payload : {}),
            _periodic: {
              scheduledTime: scheduledTime.toISOString(),
            },
          };
        }

        await enqueue(job.targetQueue, jobParams, connectionToSession(connection));
        await database.upsertPeriodicJob(job.name, scheduledTime, job.nextRunAt, connection, job);
        logger.info({ cronJobName: job.name, scheduledTime: scheduledTime.toISOString() }, "periodic.jobEnqueued");
      }, connection),
    );
  }

  function scheduleNextOrExecute(job: PeriodicJobInternal): void {
    if (!isRunning || !jobs.has(job.name)) return;

    const delay = job.nextRunAt.getTime() - Date.now();

    if (delay < 0) {
      logger.warn({ cronJobName: job.name, nextRunAt: job.nextRunAt.toISOString() }, "periodic.jobOverdue");
      executeJobAndScheduleNext();
    } else {
      job.timer = setTimeout(executeJobAndScheduleNext, delay);
    }

    function executeJobAndScheduleNext() {
      const execution = executeJob(job, job.nextRunAt)
        .then(() => handleJobResult())
        .catch((error: Error) => handleJobResult(error))
        .finally(() => pendingExecutions.delete(job.name));

      pendingExecutions.set(job.name, execution);
    }

    function handleJobResult(error?: Error) {
      if (error) {
        logger.error({ cronJobName: job.name, error }, "periodic.executionFailed");
      }
      job.nextRunAt = getNextRun(job.cronExpression, job.nextRunAt);
      scheduleNextOrExecute(job);
    }
  }

  function calculateMissedRuns(cronExpression: string, lastRunAt: Date): Date[] {
    const now = new Date();
    const missedRuns: Date[] = [];
    let currentRun = getNextRun(cronExpression, lastRunAt);
    while (currentRun <= now) {
      missedRuns.push(currentRun);
      currentRun = getNextRun(cronExpression, currentRun);
    }
    return missedRuns;
  }

  function getNextRun(cronExpression: string, fromDate: Date = new Date()): Date {
    const interval = CronExpressionParser.parse(cronExpression, { currentDate: fromDate });
    return interval.next().toDate();
  }

  function generateIdempotentKey(jobName: string, scheduledTime: Date): string {
    const roundedTime = new Date(Math.floor(scheduledTime.getTime() / 1000) * 1000);
    return `periodic:${jobName}:${roundedTime.toISOString()}`;
  }

  async function handleMissedRuns(job: PeriodicJob, missedRuns: Date[]): Promise<void> {
    logger.info(
      { catchUpStrategy: job.catchUpStrategy, cronJobName: job.name, missedCount: missedRuns.length },
      "periodic.missedRunsDetected",
    );

    switch (job.catchUpStrategy) {
      case "all":
        await handleCatchUpAll(job, missedRuns);
        break;
      case "latest":
        await handleCatchUpLatest(job, missedRuns);
        break;
      case "none":
        logger.debug({ cronJobName: job.name, missedCount: missedRuns.length }, "periodic.skippedMissedRuns");
        break;
    }
  }

  async function handleCatchUpAll(job: PeriodicJob, missedRuns: Date[]): Promise<void> {
    const limit = Math.min(missedRuns.length, job.maxCatchUp || 100);

    await database.runWithPoolConnection((connection) =>
      database.runTransaction(async (connection) => {
        await enqueue(
          job.targetQueue,
          missedRuns.slice(0, limit).map((missedRun) => {
            const jobParams = {
              ...job.jobTemplate,
              idempotentKey: generateIdempotentKey(job.name, missedRun),
            };

            if (job.includeScheduledTime) {
              jobParams.payload = {
                ...(typeof job.jobTemplate.payload === "object" && job.jobTemplate.payload !== null ? job.jobTemplate.payload : {}),
                _periodic: {
                  scheduledTime: missedRun.toISOString(),
                },
              };
            }

            return jobParams;
          }),
          connectionToSession(connection),
        );

        if (missedRuns.length > limit) {
          logger.warn({ cronJobName: job.name, maxCatchUp: limit, missedCount: missedRuns.length }, "periodic.hitMaxCatchUpLimit");
        }
        await database.upsertPeriodicJob(job.name, missedRuns[limit - 1], getNextRun(job.cronExpression), connection, job);
      }, connection),
    );
  }

  async function handleCatchUpLatest(job: PeriodicJob, missedRuns: Date[]): Promise<void> {
    const latestMissed = missedRuns[missedRuns.length - 1];

    await database.runWithPoolConnection((connection) =>
      database.runTransaction(async (connection) => {
        await enqueue(
          job.targetQueue,
          {
            ...job.jobTemplate,
            idempotentKey: generateIdempotentKey(job.name, latestMissed),
            payload: {
              ...job.jobTemplate.payload,
              ...(job.includeScheduledTime && {
                _periodic: {
                  scheduledTime: latestMissed.toISOString(),
                },
              }),
            },
          },
          connectionToSession(connection),
        );
        await database.upsertPeriodicJob(job.name, latestMissed, getNextRun(job.cronExpression), connection, job);
      }, connection),
    );
  }
}

interface PeriodicJobInternal extends PeriodicJob {
  nextRunAt: Date;
  timer?: NodeJS.Timeout;
}
