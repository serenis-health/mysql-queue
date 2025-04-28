import { Job, Queue, WorkerCallback } from "./types";
import { Database } from "./database";
import { JobProcessor } from "./jobProcessor";
import { Logger } from "./logger";
import { randomUUID } from "node:crypto";

export function WorkersFactory(logger: Logger, database: Database) {
  const jobExecutionPromises: Record<string, (job: Job) => void> = {};
  const workers: Worker[] = [];

  return {
    create(callback: WorkerCallback, pollingIntervalMs = 500, batchSize = 1, queue: Queue) {
      const wrappedCallback = createWrappedCallback(callback, queue);
      const worker = Worker(wrappedCallback, pollingIntervalMs, batchSize, logger, database, queue);
      workers.push(worker);
      return worker;
    },
    getJobExecutionPromise(queueName: string) {
      let resolvePromise: () => void;
      const promise = new Promise<void>((resolve) => {
        resolvePromise = resolve;
      });
      jobExecutionPromises[queueName] = resolvePromise!;
      return promise;
    },
    stopAll() {
      return Promise.all(workers.map((worker) => worker.stop()));
    },
  };

  function createWrappedCallback(callback: WorkerCallback, queue: Queue) {
    return async function (...args: Parameters<typeof callback>): Promise<void> {
      try {
        await callback(...args);
      } finally {
        if (jobExecutionPromises[queue.name]) {
          jobExecutionPromises[queue.name](args[0]);
          logger.debug({ queueName: queue.name }, "workers.jobExecutionPromiseResolved");
        }
      }
    };
  }
}

export type Worker = ReturnType<typeof Worker>;

export function Worker(callback: WorkerCallback, pollingIntervalMs = 500, batchSize = 1, logger: Logger, database: Database, queue: Queue) {
  const workerId = randomUUID();
  const wLogger = logger.child({ workerId });

  const jobProcessor = JobProcessor(database, wLogger, queue, callback);

  const controller = new AbortController();
  const { signal } = controller;

  let stopPromiseResolve: (() => void) | null = null;
  const stopPromise = new Promise<void>((resolve) => {
    stopPromiseResolve = resolve;
  });

  return {
    async process(jobId: string) {
      const job = (await database.getJobById(jobId)) as Job;

      await jobProcessor.process(job, signal);
    },
    async start() {
      wLogger.info({ batchSize, pollingIntervalMs }, `worker.starting`);

      while (!signal.aborted) {
        await jobProcessor.processBatch(batchSize, signal);
        await sleep(pollingIntervalMs);
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
