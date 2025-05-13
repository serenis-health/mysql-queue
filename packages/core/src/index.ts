import { EnqueueParams, JobForInsert, Options, Queue, RetrieveQueueParams, UpsertQueueParams, WorkerCallback } from "./types";
import { Connection } from "mysql2/promise";
import { Database } from "./database";
import { Logger } from "./logger";
import { randomUUID } from "node:crypto";
import { WorkersFactory } from "./worker";

export function MysqlQueue(options: Options) {
  const logger = Logger({
    level: options.loggingLevel,
    prettyPrint: options.loggingPrettyPrint,
  });
  const database = Database(logger, {
    tablesPrefix: options.tablesPrefix,
    uri: options.dbUri,
  });
  const workersFactory = WorkersFactory(logger, database);

  async function retrieveQueue(params: RetrieveQueueParams) {
    const queue = await database.getQueueByName(params.name);
    if (!queue) throw new Error(`Queue with name ${params.name} not found`);

    return queue as Queue;
  }

  return {
    async destroy() {
      logger.debug("destroying");
      await database.removeAllTables();
      logger.info("destroyed");
    },
    async dispose() {
      logger.debug("disposing");
      await workersFactory.stopAll();
      await database.endPool();
      logger.info("disposed");
      logger.flush();
    },

    async enqueue(queueName: string, params: EnqueueParams, connection?: Connection) {
      const jobsForInsert: JobForInsert[] = (Array.isArray(params) ? params : [params]).map((p) => ({
        id: randomUUID(),
        name: p.name,
        payload: JSON.stringify(p.payload),
        priority: p.priority || 0,
        startAfter: p.startAfter || null,
        status: "pending",
      }));

      await database.addJobs(queueName, jobsForInsert, connection);
      logger.info({ jobs: jobsForInsert }, "jobsAddedToQueue");
      return { jobIds: jobsForInsert.map((j) => j.id) };
    },
    getEnqueueRawSql(queueName: string, params: EnqueueParams) {
      const jobsForInsert: JobForInsert[] = (Array.isArray(params) ? params : [params]).map((p) => ({
        id: randomUUID(),
        name: p.name,
        payload: JSON.stringify(p.payload),
        priority: p.priority || 0,
        startAfter: p.startAfter || null,
        status: "pending",
      }));
      if (jobsForInsert.length === 0) return "SELECT NULL LIMIT 0;";

      const values = jobsForInsert
        .map(
          (job) =>
            `SELECT '${job.id}', '${job.name}', '${job.payload}', '${job.status}', '${job.priority}', ${
              job.startAfter ? `'${job.startAfter.toISOString().slice(0, 19).replace("T", " ")}'` : "NULL"
            }`,
        )
        .join(" UNION ALL ");

      return `INSERT INTO ${database.jobsTable()} (id, name, payload, status, priority, startAfter, queueId) SELECT j.id, j.name, j.payload, j.status, j.priority, j.startAfter, q.id FROM (${values}) AS j(id, name, payload, status, priority, startAfter) JOIN ${database.queuesTable()} q ON q.name = '${queueName}'`;
    },
    getJobExecutionPromise: workersFactory.getJobExecutionPromise,
    async initialize() {
      logger.debug("starting");
      await database.runMigrations();
      logger.info("started");
    },
    jobsTable: database.jobsTable,
    migrationTable: database.migrationsTable,
    queuesTable: database.queuesTable,
    async upsertQueue(name: string, params: UpsertQueueParams = {}) {
      const queueWithoutId: Omit<Queue, "id"> = {
        backoffMultiplier: params.backoffMultiplier !== undefined ? params.backoffMultiplier : 2,
        maxDurationMs: params.maxDurationMs || 5000,
        maxRetries: params.maxRetries || 3,
        minDelayMs: params.minDelayMs || 1000,
        name,
      };

      let id: string;
      const existingQueue = await database.getQueueIdByName(name);
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
    async work(queueName: string, callback: WorkerCallback, pollingIntervalMs = 500, batchSize = 1) {
      const queue = await retrieveQueue({ name: queueName });
      return workersFactory.create(callback, pollingIntervalMs, batchSize, queue);
    },
  };
}

export type MysqlQueue = ReturnType<typeof MysqlQueue>;
