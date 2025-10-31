import { JobProcessor, JobProcessorOptions } from "./jobProcessor";
import { JobWithQueueName, Queue, WorkerCallback } from "./types";
import { Database } from "./database";
import { errorToJson } from "./utils";
import { Logger } from "./logger";
import { randomUUID } from "node:crypto";

export function WorkersFactory(logger: Logger, database: Database) {
  const jobExecutionTrackers: Record<string, { remaining: number; promise: (job: JobWithQueueName) => void }> = {};
  const workers: Worker[] = [];

  return {
    create(
      callback: WorkerCallback,
      queue: Queue,
      options: {
        pollingIntervalMs: number;
        callbackBatchSize: number;
        pollingBatchSize: number;
        onJobFailed?: (error: Error, job: { id: string; queueName: string }) => void;
      },
    ) {
      const worker = Worker(database, callback, queue, logger, {
        ...options,
        onJobProcessed: (job) => {
          const tracker = jobExecutionTrackers[queue.name];
          if (tracker) {
            tracker.remaining -= 1;
            logger.debug({ queueName: queue.name, remaining: tracker.remaining }, "workers.jobExecutionPromiseTick");
            if (tracker.remaining <= 0) {
              tracker.promise(job);
              logger.debug({ queueName: queue.name }, "workers.jobExecutionPromiseResolved");
            }
          }
        },
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

export function Worker(database: Database, callback: WorkerCallback, queue: Queue, logger: Logger, options: JobProcessorOptions) {
  const workerId = randomUUID();
  const wLogger = logger.child({ workerId });

  const controller = new AbortController();
  const { signal } = controller;
  const jobProcessor = JobProcessor(database, wLogger, queue, callback, signal, options);

  let stopPromiseResolve: (() => void) | null = null;
  const stopPromise = new Promise<void>((resolve) => {
    stopPromiseResolve = resolve;
  });

  return {
    async start() {
      wLogger.info(
        {
          callbackBatchSize: options.callbackBatchSize,
          pollingBatchSize: options.pollingBatchSize,
          pollingIntervalMs: options.pollingIntervalMs,
        },
        `worker.starting`,
      );

      while (!signal.aborted) {
        try {
          await jobProcessor.process();
          await sleep(options.pollingIntervalMs);
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
