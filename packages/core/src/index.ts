import { EnqueueParams, JobForInsert, Options, Queue, RetrieveQueueParams, Session, UpsertQueueParams, WorkerCallback } from "./types";
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

  return {
    async countJobs(queueName: string) {
      return database.countJobs(queueName, options.partitionKey);
    },
    async dispose() {
      logger.debug("disposing");
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
    getJobExecutionPromise: workersFactory.getJobExecutionPromise,
    async globalDestroy() {
      logger.debug("destroying");
      await database.removeAllTables();
      logger.info("destroyed");
    },
    async globalInitialize() {
      logger.debug("starting");
      await database.runMigrations();
      logger.info("started");
    },
    jobsTable: database.jobsTable,
    migrationTable: database.migrationsTable,
    async purge() {
      logger.debug({ partitionKey: options.partitionKey }, "purging");

      await workersFactory.stopAll();
      await database.deleteQueuesByPartition(options.partitionKey);
      logger.info({ partitionKey: options.partitionKey }, "purged");
    },
    queuesTable: database.queuesTable,
    retrieveQueue,
    async upsertQueue(name: string, params: UpsertQueueParams = {}) {
      const queueWithoutId: Omit<Queue, "id"> = {
        backoffMultiplier: params.backoffMultiplier && params.backoffMultiplier > 0 ? params.backoffMultiplier : 2,
        maxDurationMs: params.maxDurationMs || 5000,
        maxRetries: params.maxRetries || 3,
        minDelayMs: params.minDelayMs || 1000,
        name,
        partitionKey: options.partitionKey,
      };

      let id: string;
      const existingQueue = await database.getQueueIdByName(name, options.partitionKey);
      if (existingQueue) {
        id = existingQueue.id;
        await database.updateQueue({ id, ...queueWithoutId });
        logger.debug({ queue: { id, ...queueWithoutId } }, "queueUpdated");
      } else {
        id = randomUUID();
        await database.createQueue({ id, ...queueWithoutId });
        logger.debug({ queue: { id, ...queueWithoutId } }, "queueCreated");
      }

      const queue: Queue = { id, ...queueWithoutId };
      return queue;
    },
    async work(
      queueName: string,
      callback: WorkerCallback,
      pollingIntervalMs = 500,
      batchSize = 1,
      onJobFailed?: (error: Error, job: { id: string; queueName: string }) => void,
    ) {
      const queue = await retrieveQueue({ name: queueName });
      return workersFactory.create(callback, pollingIntervalMs, batchSize, queue, onJobFailed);
    },
  };

  async function retrieveQueue(params: RetrieveQueueParams) {
    const queue = await database.getQueueByName(params.name, options.partitionKey);
    if (!queue) throw new Error(`Queue with name ${params.name}:${options.partitionKey} not found`);

    return queue as Queue;
  }
}

export type MysqlQueue = ReturnType<typeof MysqlQueue>;

export { Session, Job, PurgePartitionParams } from "./types";

function applyOptionsDefault(options: Options) {
  return {
    ...options,
    partitionKey: options.partitionKey ?? "default",
  };
}
