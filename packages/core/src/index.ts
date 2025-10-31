import {
  EnqueueParams,
  JobForInsert,
  Options,
  Queue,
  RetrieveQueueParams,
  Session,
  UpsertQueueParams,
  WorkerCallback,
  WorkOptions,
} from "./types";
import { createRescuer } from "./rescuer";
import { createScheduler } from "./scheduler";
import { Database } from "./database";
import { Logger } from "./logger";
import { randomUUID } from "node:crypto";
import { WorkersFactory } from "./worker";

export function MysqlQueue(_options: Options) {
  const options = applyOptionsDefault(_options);
  const logger = Logger({
    level: options.loggingLevel,
    prettyPrint: options.loggingPrettyPrint,
  });
  const database = Database(logger, {
    tablesPrefix: options.tablesPrefix,
    uri: options.dbUri,
  });
  const workersFactory = WorkersFactory(logger, database);
  const rescuer = createRescuer(database, logger, { batchSize: options.rescuerBatchSize, rescueAfterMs: options.rescuerRescueAfterMs });
  const rescuerScheduler = createScheduler(rescuer.rescue, logger, {
    intervalMs: options.rescuerIntervalMs,
    runOnStart: options.rescuerRunOnStart,
    taskName: "rescuer",
  });

  return {
    __internal: { getRescuerNextRun: rescuerScheduler.getNextRun, rescue: rescuer.rescue },
    async countJobs(queueName: string) {
      return database.countJobs(queueName, options.partitionKey);
    },
    async dispose() {
      logger.debug("disposing");
      rescuerScheduler.stop();
      await workersFactory.stopAll();
      await database.endPool();
      logger.info("disposed");
      logger.flush();
    },
    async enqueue(queueName: string, params: EnqueueParams, session?: Session) {
      const now = new Date();
      const jobsForInsert: JobForInsert[] = (Array.isArray(params) ? params : [params]).map((p) => {
        const payloadStr = JSON.stringify(p.payload);
        const byteLength = new TextEncoder().encode(payloadStr).length;
        if (byteLength > (options.maxPayloadSizeKb || 16) * 1024) throw new Error(`Payload size exceeds maximum allowed size`);
        return {
          createdAt: now,
          id: randomUUID(),
          idempotentKey: p.idempotentKey,
          name: p.name,
          payload: payloadStr,
          pendingDedupKey: p.pendingDedupKey,
          priority: p.priority || 0,
          startAfter: p.startAfter || now,
          status: "pending",
        };
      });

      const affectedRows = await database.addJobs(queueName, jobsForInsert, options.partitionKey, session);
      logger.debug({ jobCount: affectedRows, jobs: jobsForInsert }, "enqueue.jobsAddedToQueue");
      logger.info({ jobCount: affectedRows }, "enqueue.jobsAddedToQueue");
      return { jobIds: jobsForInsert.map((j) => j.id) };
    },
    async getJobById(id: string) {
      return await database.getJobById(id);
    },
    getJobExecutionPromise: workersFactory.getJobExecutionPromise,
    async globalDestroy() {
      logger.debug("destroying");
      await database.removeAllTables();
      logger.info("destroyed");
    },
    async globalInitialize() {
      logger.debug("starting");
      await database.runMigrations();
      rescuerScheduler.start();
      logger.info("started");
    },
    jobsTable: database.jobsTable,
    migrationTable: database.migrationsTable,
    async pauseQueue(queueName: string) {
      logger.debug({ partitionKey: options.partitionKey, queueName }, "pausingQueue");
      await database.pauseQueue(queueName, options.partitionKey);
      logger.info({ partitionKey: options.partitionKey, queueName }, "queuePaused");
    },
    async purge() {
      logger.debug({ partitionKey: options.partitionKey }, "purging");

      await workersFactory.stopAll();
      await database.deleteQueuesByPartition(options.partitionKey);
      logger.info({ partitionKey: options.partitionKey }, "purged");
    },
    queuesTable: database.queuesTable,
    async resumeQueue(queueName: string) {
      logger.debug({ partitionKey: options.partitionKey, queueName }, "resumingQueue");
      await database.resumeQueue(queueName, options.partitionKey);
      logger.info({ partitionKey: options.partitionKey, queueName }, "queueResumed");
    },
    retrieveQueue,
    async upsertQueue(name: string, params: UpsertQueueParams = {}) {
      const existingQueue = await database.getQueueByName(name, options.partitionKey);
      const baseQueueParams: Omit<Queue, "id"> = {
        backoffMultiplier: params.backoffMultiplier && params.backoffMultiplier > 0 ? params.backoffMultiplier : 2,
        maxDurationMs: params.maxDurationMs || 5000,
        maxRetries: params.maxRetries || 3,
        minDelayMs: params.minDelayMs || 1000,
        name,
        partitionKey: options.partitionKey,
        paused: false,
      };
      if (existingQueue) {
        const queue: Queue = { ...baseQueueParams, id: existingQueue.id, paused: existingQueue.paused };
        await database.updateQueue(queue);
        logger.debug({ queue }, "queueUpdated");
        return queue;
      }
      const queue: Queue = { ...baseQueueParams, id: randomUUID() };
      await database.createQueue(queue);
      logger.debug({ queue }, "queueCreated");
      return queue;
    },
    async work(queueName: string, callback: WorkerCallback, _options: WorkOptions = {}) {
      const options = applyWorkOptionsDefault(_options);
      const queue = await retrieveQueue({ name: queueName });
      return workersFactory.create(callback, queue, options);
    },
  };

  async function retrieveQueue(params: RetrieveQueueParams) {
    const queue = await database.getQueueByName(params.name, options.partitionKey);
    if (!queue) throw new Error(`Queue with name ${params.name}:${options.partitionKey} not found`);

    return queue as Queue;
  }
}

export type MysqlQueue = ReturnType<typeof MysqlQueue>;

export { CallbackContext, Session, Job, PurgePartitionParams } from "./types";

function applyOptionsDefault(options: Options) {
  return {
    ...options,
    partitionKey: options.partitionKey ?? "default",
    rescuerBatchSize: options.rescuerBatchSize ?? 100,
    rescuerIntervalMs: options.rescuerIntervalMs ?? 1_800_000, //30m
    rescuerRescueAfterMs: options.rescuerRescueAfterMs ?? 3_600_000, //1h
    rescuerRunOnStart: options.rescuerRunOnStart ?? false,
  };
}

function applyWorkOptionsDefault(options?: WorkOptions) {
  return {
    ...options,
    callbackBatchSize: options?.callbackBatchSize ?? 1,
    pollingBatchSize: options?.pollingBatchSize ?? 1,
    pollingIntervalMs: options?.pollingIntervalMs ?? 500,
  };
}
