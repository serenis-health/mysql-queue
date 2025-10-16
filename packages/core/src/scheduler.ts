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
