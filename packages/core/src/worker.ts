import { JobWithQueueName, Queue, WorkerCallback } from "./types";
import { Database } from "./database";
import { errorToJson } from "./utils";
import { JobProcessor } from "./jobProcessor";
import { Logger } from "./logger";
import { randomUUID } from "node:crypto";

export function WorkersFactory(logger: Logger, database: Database) {
  const jobExecutionTrackers: Record<string, { remaining: number; promise: (job: JobWithQueueName) => void }> = {};
  const workers: Worker[] = [];

  return {
    create(callback: WorkerCallback, pollingIntervalMs = 500, batchSize = 1, queue: Queue) {
      const worker = Worker(callback, pollingIntervalMs, batchSize, logger, database, queue, (job) => {
        const tracker = jobExecutionTrackers[queue.name];
        if (tracker) {
          tracker.remaining -= 1;
          logger.debug({ queueName: queue.name, remaining: tracker.remaining }, "workers.jobExecutionPromiseTick");
          if (tracker.remaining <= 0) {
            tracker.promise(job);
            logger.debug({ queueName: queue.name }, "workers.jobExecutionPromiseResolved");
          }
        }
      });
      workers.push(worker);
      return worker;
    },
    getJobExecutionPromise(queueName: string, count = 1) {
      let resolvePromise: () => void;
      const promise = new Promise<void>((resolve) => {
        resolvePromise = resolve;
      });
      jobExecutionTrackers[queueName] = { promise: resolvePromise!, remaining: count };
      return promise;
    },
    stopAll() {
      return Promise.all(workers.map((worker) => worker.stop()));
    },
  };
}

export type Worker = ReturnType<typeof Worker>;

export function Worker(
  callback: WorkerCallback,
  pollingIntervalMs = 500,
  batchSize = 1,
  logger: Logger,
  database: Database,
  queue: Queue,
  onJobProcessed?: (job: JobWithQueueName) => void,
) {
  const workerId = randomUUID();
  const wLogger = logger.child({ workerId });

  const jobProcessor = JobProcessor(database, wLogger, queue, callback, onJobProcessed);

  const controller = new AbortController();
  const { signal } = controller;

  let stopPromiseResolve: (() => void) | null = null;
  const stopPromise = new Promise<void>((resolve) => {
    stopPromiseResolve = resolve;
  });

  return {
    async start() {
      wLogger.info({ batchSize, pollingIntervalMs }, `worker.starting`);

      while (!signal.aborted) {
        try {
          await jobProcessor.processBatch(batchSize, signal);
          await sleep(pollingIntervalMs);
        } catch (error) {
          const typedError = error as Error;
          wLogger.error({ error: errorToJson(typedError) }, `worker.loop.error`);
        }
      }
      stopPromiseResolve?.();
      wLogger.debug(`worker.aborted`);
    },
    async stop() {
      wLogger.debug(`worker.stopping`);
      controller.abort();
      await stopPromise;
      wLogger.info(`worker.stopped`);
    },
  };
}

async function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
