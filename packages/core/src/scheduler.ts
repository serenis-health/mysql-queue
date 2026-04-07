import { CronExpressionParser } from "cron-parser";
import { errorToJson } from "./utils";
import { Logger } from "./logger";

export function createScheduler(task: () => Promise<void>, logger: Logger, options: SchedulerOptions) {
  const { intervalMs, runOnStart, taskName } = options;

  let interval: NodeJS.Timeout | null = null;
  let nextRun: Date | null = null;
  let running = false;
  let taskInProgress = false;

  function stop() {
    if (interval) clearInterval(interval);
    interval = null;
    nextRun = null;
    running = false;
    taskInProgress = false;
    logger.trace({ taskName }, "scheduler.stopped");
  }

  function getNextRun() {
    return nextRun;
  }

  function updateNextRun() {
    nextRun = new Date(Date.now() + intervalMs);
  }

  async function runSafely() {
    if (taskInProgress) {
      logger.trace({ taskName }, "scheduler.skipRunTaskInProgress");
      return;
    }
    taskInProgress = true;
    try {
      logger.trace({ taskName }, "scheduler.runStarted");
      await task();
      logger.trace({ taskName }, "scheduler.runCompleted");
    } catch (err) {
      logger.error({ taskName, ...errorToJson(err as Error) }, "scheduler.runError");
    } finally {
      taskInProgress = false;
    }
  }

  function start() {
    if (running) {
      logger.trace({ taskName }, "scheduler.alreadyRunning");
      return;
    }
    running = true;
    logger.debug({ intervalMs, taskName }, "scheduler.started");
    updateNextRun();
    interval = setInterval(() => {
      updateNextRun();
      void runSafely();
    }, intervalMs);
    if (runOnStart) void runSafely();
  }

  return {
    getNextRun,
    start,
    stop,
  };
}

interface SchedulerOptions {
  taskName: string;
  intervalMs: number;
  runOnStart: boolean;
}

export function createCronScheduler(task: () => Promise<void>, logger: Logger, options: CronSchedulerOptions) {
  const { cronExpression, taskName } = options;

  let timeout: NodeJS.Timeout | null = null;
  let nextRun: Date | null = null;
  let running = false;
  let epoch = 0;

  function stop() {
    if (timeout) clearTimeout(timeout);
    timeout = null;
    nextRun = null;
    running = false;
    epoch++;
    logger.trace({ taskName }, "scheduler.stopped");
  }

  function getNextRun() {
    return nextRun;
  }

  function getNextCronDate(): Date {
    return CronExpressionParser.parse(cronExpression, { tz: "UTC" }).next().toDate();
  }

  async function runSafely(myEpoch: number) {
    try {
      logger.trace({ taskName }, "scheduler.runStarted");
      await task();
      logger.trace({ taskName }, "scheduler.runCompleted");
    } catch (err) {
      logger.error({ taskName, ...errorToJson(err as Error) }, "scheduler.runError");
    } finally {
      if (myEpoch === epoch) scheduleNext();
    }
  }

  function scheduleNext() {
    if (!running) return;
    nextRun = getNextCronDate();
    scheduleSafe(epoch);
  }

  function scheduleSafe(myEpoch: number) {
    if (!running || myEpoch !== epoch) return;
    const delay = nextRun!.getTime() - Date.now();
    if (delay <= 0) {
      void runSafely(myEpoch);
      return;
    }
    // Node.js setTimeout uses a signed 32-bit integer for the delay (~24.8 days max).
    // For longer delays, we reschedule in chunks to avoid immediate fire.
    const safeDelay = Math.min(delay, MAX_TIMEOUT_MS);
    timeout = setTimeout(() => scheduleSafe(myEpoch), safeDelay);
  }

  function start() {
    if (running) {
      logger.trace({ taskName }, "scheduler.alreadyRunning");
      return;
    }
    running = true;
    logger.debug({ cronExpression, taskName }, "scheduler.started");
    scheduleNext();
  }

  return {
    getNextRun,
    start,
    stop,
  };
}

interface CronSchedulerOptions {
  taskName: string;
  cronExpression: string;
}

const MAX_TIMEOUT_MS = 2_147_483_647;
