import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { MysqlQueue, Session } from "../src";
import { QueryDatabase } from "./utils/queryDatabase";
import { randomUUID } from "node:crypto";
import { RowDataPacket } from "mysql2";
import { sleep } from "../src/utils";

const dbUri = "mysql://root:password@localhost:3306/serenis";

describe("mysqlQueue", () => {
  const queryDatabase = QueryDatabase({ dbUri });
  const instance = MysqlQueue({
    dbUri,
    loggingLevel: "fatal",
    tablesPrefix: `${randomUUID().slice(-4)}_`,
  });

  describe("initialize", () => {
    it("should apply migrations", async () => {
      await instance.globalInitialize();

      const rows = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${instance.migrationTable()};`);
      expect(rows).toEqual([
        {
          applied_at: expect.any(Date),
          id: 1,
          name: "create-queues-table",
        },
        {
          applied_at: expect.any(Date),
          id: 2,
          name: "create-jobs-table",
        },
        {
          applied_at: expect.any(Date),
          id: 3,
          name: "add-partition-key",
        },
        {
          applied_at: expect.any(Date),
          id: 4,
          name: "add-idempotent-key",
        },
        {
          applied_at: expect.any(Date),
          id: 5,
          name: "add-pending-dedup-key",
        },
        {
          applied_at: expect.any(Date),
          id: 6,
          name: "add-paused-column",
        },
        {
          applied_at: expect.any(Date),
          id: 7,
          name: "add-running-status-and-errors",
        },
        {
          applied_at: expect.any(Date),
          id: 8,
          name: "create-periodic-jobs-state-table",
        },
        {
          applied_at: expect.any(Date),
          id: 9,
          name: "create-leader-election-table",
        },
        {
          applied_at: expect.any(Date),
          id: 10,
          name: "extend-pending-dedup-to-running",
        },
        {
          applied_at: expect.any(Date),
          id: 11,
          name: "add-sequential",
        },
      ]);
    });

    it("should create tables", async () => {
      await instance.globalInitialize();

      const rows = await queryDatabase.query<RowDataPacket[]>("SHOW TABLES;");
      const tableNames = rows.map((row) => row.Tables_in_serenis);
      expect(tableNames).toEqual(expect.arrayContaining([instance.jobsTable(), instance.migrationTable(), instance.queuesTable()]));

      await instance.globalDestroy();
    });
  });

  describe("destroy", () => {
    beforeEach(async () => {
      await instance.globalInitialize();
      await queryDatabase.query(`CREATE TABLE IF NOT EXISTS another_table (id INT AUTO_INCREMENT PRIMARY KEY);`);
    });

    afterEach(async () => {
      await queryDatabase.query("DROP TABLE IF EXISTS another_table;");
    });

    it("should remove all tables", async () => {
      await instance.globalDestroy();

      const rows = await queryDatabase.query<RowDataPacket[]>("SHOW TABLES;");
      const tableNames = rows.map((row) => row.Tables_in_serenis);
      expect(tableNames).toEqual(expect.arrayContaining(["another_table"]));
    });
  });

  describe("upsertQueue", () => {
    beforeAll(async () => {
      await instance.globalInitialize();
    });

    afterAll(async () => {
      await instance.globalDestroy();
    });

    it("should create a row in mysql_queue_queues", async () => {
      const queueName = "test_queue";
      await instance.upsertQueue(queueName);

      const [row] = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${instance.queuesTable()};`);

      expect(isValidUUID(row.id)).toBeTruthy();
      expect(row).toEqual({
        backoffMultiplier: 2,
        id: expect.any(String),
        maxDurationMs: 5000,
        maxRetries: 3,
        minDelayMs: 1000,
        name: "test_queue",
        partitionKey: "default",
        paused: 0,
        sequential: 0,
      });
    });

    it("should update the row in mysql_queue_queues case already created", async () => {
      const queueName = "test_queue";
      await instance.upsertQueue(queueName);

      await instance.upsertQueue(queueName, {
        backoffMultiplier: 3,
        maxDurationMs: 10000,
        maxRetries: 5,
        minDelayMs: 2000,
      });

      const [row] = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${instance.queuesTable()};`);

      expect(row).toEqual({
        backoffMultiplier: 3,
        id: expect.any(String),
        maxDurationMs: 10000,
        maxRetries: 5,
        minDelayMs: 2000,
        name: "test_queue",
        partitionKey: "default",
        paused: 0,
        sequential: 0,
      });
    });

    it("should set backoffMultiplier to 2 if 0 is passed", async () => {
      const queueName = "test_queue";
      await instance.upsertQueue(queueName, { backoffMultiplier: 0 });

      const [row] = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${instance.queuesTable()};`);

      expect(isValidUUID(row.id)).toBeTruthy();
      expect(row.backoffMultiplier).toEqual(2);
    });

    it("should set backoffMultiplier to 2 if -1 is passed", async () => {
      const queueName = "test_queue";
      await instance.upsertQueue(queueName, { backoffMultiplier: -1 });

      const [row] = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${instance.queuesTable()};`);

      expect(isValidUUID(row.id)).toBeTruthy();
      expect(row.backoffMultiplier).toEqual(2);
    });

    it("should set backoffMultiplier to 3 if 3 is passed", async () => {
      const queueName = "test_queue";
      await instance.upsertQueue(queueName, { backoffMultiplier: 3 });

      const [row] = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${instance.queuesTable()};`);

      expect(isValidUUID(row.id)).toBeTruthy();
      expect(row.backoffMultiplier).toEqual(3);
    });
  });

  describe("enqueue", () => {
    const queueName = "test_queue";

    beforeEach(async () => {
      await instance.globalInitialize();
      await instance.upsertQueue(queueName);
    });

    afterEach(async () => {
      await instance.globalDestroy();
    });

    describe("without session", () => {
      it("should create a row in mysql_queue_jobs", async () => {
        const { jobIds } = await instance.enqueue(queueName, {
          name: "test_job",
          payload: { message: "Hello, world!" },
        });

        const [row] = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${instance.jobsTable()};`);

        expect(isValidUUID(jobIds[0])).toBeTruthy();
        expect(row).toEqual({
          attempts: 0,
          completedAt: null,
          createdAt: expect.any(Date),
          errors: null,
          failedAt: null,
          id: expect.any(String),
          idempotentKey: null,
          name: "test_job",
          payload: { message: "Hello, world!" },
          pendingDedupKey: null,
          priority: 0,
          queueId: expect.any(String),
          runningAt: null,
          sequentialKey: null,
          startAfter: expect.any(Date),
          status: "pending",
        });
        expect(row.startAfter.getTime()).toBe(row.createdAt.getTime());
      });

      it("should throw case queue not exists", async () => {
        const unknownQueueName = "anotherQueue";

        await expect(
          instance.enqueue(unknownQueueName, {
            name: "test_job",
            payload: { message: "Hello, world!" },
          }),
        ).rejects.toThrowError("Unable to add jobs, maybe queue does not exist");
      });

      it("should not throw case 0 jobs params passed", async () => {
        await instance.enqueue(queueName, []);
      });

      it("should fire the worker callback", async () => {
        const promise = instance.getJobExecutionPromise(queueName);

        const workerCbMock = vi.fn();
        const worker = await instance.work(queueName, workerCbMock);

        const { jobIds } = await instance.enqueue(queueName, {
          name: "test_job",
          payload: { message: "Hello, world!" },
        });

        void worker.start();
        await promise;

        await worker.stop();
        expect(workerCbMock).toHaveBeenCalledWith([expect.objectContaining({ id: jobIds[0] })], expect.anything(), expect.anything());
      });

      it("should fire the worker callback two jobs", async () => {
        const promise = instance.getJobExecutionPromise(queueName, 2);

        const workerCbMock = vi.fn();
        const worker = await instance.work(queueName, workerCbMock);

        await instance.enqueue(queueName, [
          { name: "test_job", payload: {} },
          { name: "test_job", payload: {} },
        ]);

        void worker.start();
        await promise;

        await worker.stop();
        expect(workerCbMock).toHaveBeenCalledTimes(2);
      });

      it("should throw case payload size exceed limit", async () => {
        await expect(() =>
          instance.enqueue(queueName, [{ name: "test_job", payload: { data: "a".repeat(1024 * 1024) } }]),
        ).rejects.toThrowError("Payload size exceeds maximum allowed size");
      });
    });

    describe("with session", () => {
      const session: Session = {
        async execute(sql, parameters) {
          const connection = await queryDatabase.pool.getConnection();
          const result = await connection.query(sql, parameters);
          connection.release();
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          return [{ affectedRows: (result[0] as any).affectedRows }];
        },
        async query(sql, parameters) {
          const connection = await queryDatabase.pool.getConnection();
          const [result] = await connection.query(sql, parameters);
          connection.release();
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          return result as any[];
        },
      };

      it("should create a row in mysql_queue_jobs", async () => {
        const { jobIds } = await instance.enqueue(
          queueName,
          {
            name: "test_job",
            payload: { message: "Hello, world!" },
          },
          session,
        );

        const [row] = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${instance.jobsTable()};`);

        expect(isValidUUID(jobIds[0])).toBeTruthy();
        expect(row).toEqual({
          attempts: 0,
          completedAt: null,
          createdAt: expect.any(Date),
          errors: null,
          failedAt: null,
          id: expect.any(String),
          idempotentKey: null,
          name: "test_job",
          payload: { message: "Hello, world!" },
          pendingDedupKey: null,
          priority: 0,
          queueId: expect.any(String),
          runningAt: null,
          sequentialKey: null,
          startAfter: expect.any(Date),
          status: "pending",
        });
      });

      it("should throw case queue not exists", async () => {
        const unknownQueueName = "anotherQueue";

        await expect(
          instance.enqueue(
            unknownQueueName,
            {
              name: "test_job",
              payload: { message: "Hello, world!" },
            },
            session,
          ),
        ).rejects.toThrowError("Unable to add jobs, maybe queue does not exist");
      });

      it("should throw case session not return affectedRows", async () => {
        const wrongSession: Session = {
          async execute(sql, parameters) {
            const connection = await queryDatabase.pool.getConnection();
            await connection.query(sql, parameters);
            connection.release();
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            return [{} as any];
          },
          async query(sql, parameters) {
            const connection = await queryDatabase.pool.getConnection();
            const [result] = await connection.query(sql, parameters);
            connection.release();
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            return result as any[];
          },
        };

        await expect(
          instance.enqueue(
            queueName,
            {
              name: "test_job",
              payload: { message: "Hello, world!" },
            },
            wrongSession,
          ),
        ).rejects.toThrowError("Session did not return affected rows");
      });
    });
  });

  describe("work", () => {
    const queueName = "test_queue";

    beforeEach(async () => {
      await instance.globalInitialize();
      await instance.upsertQueue(queueName, { maxRetries: 1 });
    });

    afterEach(async () => {
      await instance.globalDestroy();
    });

    it("should fail job after attempts", async () => {
      const promise = instance.getJobExecutionPromise(queueName);

      const errorMessage = "a".repeat(120);

      const workerCbMock = vi.fn().mockImplementation(() => {
        throw new Error(errorMessage);
      });
      const worker = await instance.work(queueName, workerCbMock);

      await instance.enqueue(queueName, {
        name: "test_job",
        payload: {},
      });

      void worker.start();
      await promise;

      await new Promise<void>((r) => setTimeout(() => r(), 500));
      const [row] = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${instance.jobsTable()}`);
      expect(row).toMatchObject({
        attempts: 1,
        errors: [
          {
            at: expect.any(String),
            attempt: 1,
            error: expect.stringContaining(errorMessage),
          },
        ],
        failedAt: expect.any(Date),
        status: "failed",
      });

      await worker.stop();
    });
  });

  describe("Idempotency & pending deduplication", () => {
    const queueName = "test_queue";

    beforeEach(async () => {
      await instance.globalInitialize();
      await instance.upsertQueue(queueName, { maxRetries: 2 });
    });

    afterEach(async () => {
      await instance.globalDestroy();
    });

    it("should prevent duplicate jobs with same idempotentKey and name", async () => {
      await instance.enqueue(queueName, {
        idempotentKey: "user-123-welcome",
        name: "welcome-email",
        payload: { userId: 123 },
      });
      await instance.enqueue(queueName, {
        idempotentKey: "user-123-welcome",
        name: "welcome-email",
        payload: { userId: 123 },
      });

      const jobs = await queryDatabase.query<RowDataPacket[]>(
        `SELECT id, name, idempotentKey, status FROM ${instance.jobsTable()} ORDER BY createdAt`,
      );
      expect(jobs).toHaveLength(1);
    });

    it("should allow different job names with same idempotentKey", async () => {
      await instance.enqueue(queueName, {
        idempotentKey: "user-123",
        name: "welcome-email",
        payload: { userId: 123 },
      });
      await instance.enqueue(queueName, {
        idempotentKey: "user-123",
        name: "notification-email",
        payload: { userId: 123 },
      });

      const jobs = await queryDatabase.query<RowDataPacket[]>(`SELECT name, idempotentKey FROM ${instance.jobsTable()} ORDER BY name`);
      expect(jobs).toHaveLength(2);
    });

    it("should prevent duplicate pending jobs with same pendingDedupKey and name", async () => {
      await instance.enqueue(queueName, {
        name: "send-to-sts",
        payload: { invoiceId: 456 },
        pendingDedupKey: "invoice-456",
      });
      await instance.enqueue(queueName, {
        name: "send-to-sts",
        payload: { invoiceId: 456 },
        pendingDedupKey: "invoice-456",
      });

      const jobs = await queryDatabase.query<RowDataPacket[]>(
        `SELECT id, name, pendingDedupKey, status FROM ${instance.jobsTable()} ORDER BY createdAt`,
      );
      expect(jobs).toHaveLength(1);
    });

    it("should prevent duplicate jobs with same pendingDedupKey while running", async () => {
      let resolveJob: () => void;
      const jobPromise = new Promise<void>((resolve) => {
        resolveJob = resolve;
      });

      let resolveHandlerCalled: () => void;
      const handlerCalledPromise = new Promise<void>((resolve) => {
        resolveHandlerCalled = resolve;
      });

      const worker = await instance.work(queueName, async () => {
        resolveHandlerCalled();
        await jobPromise;
      });
      void worker.start();

      await instance.enqueue(queueName, {
        name: "send-to-sts",
        payload: { invoiceId: 789 },
        pendingDedupKey: "invoice-789",
      });

      await handlerCalledPromise;

      await instance.enqueue(queueName, {
        name: "send-to-sts",
        payload: { duplicate: true, invoiceId: 789 },
        pendingDedupKey: "invoice-789",
      });

      const jobs = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${instance.jobsTable()} ORDER BY createdAt`);

      expect(jobs).toHaveLength(1);
      expect(jobs[0].status).toBe("running");
      expect(jobs[0].pendingDedupKey).toBe("invoice-789");

      resolveJob!();
      await worker.stop();
    });

    it("should allow re-enqueuing after job completion", async () => {
      const worker = await instance.work(queueName, () => {});
      void worker.start();
      const promise = instance.getJobExecutionPromise(queueName, 1);
      await instance.enqueue(queueName, {
        name: "send-to-sts",
        payload: { invoiceId: 789 },
        pendingDedupKey: "invoice-789",
      });
      await promise;

      await instance.enqueue(queueName, {
        name: "send-to-sts",
        payload: { invoiceId: 789, retry: true },
        pendingDedupKey: "invoice-789",
      });

      const allJobs = await queryDatabase.query<RowDataPacket[]>(
        `SELECT id, name, pendingDedupKey, status FROM ${instance.jobsTable()} ORDER BY createdAt`,
      );

      expect(allJobs).toHaveLength(2);
      expect(allJobs[0].status).toBe("completed");
      expect(allJobs[1].status).toBe("pending");
    });

    it("should allow re-enqueuing after job failure", async () => {
      const worker = await instance.work(queueName, () => {
        throw new Error();
      });
      void worker.start();
      const promise = instance.getJobExecutionPromise(queueName, 2);
      await instance.enqueue(queueName, {
        name: "send-to-sts",
        payload: { invoiceId: 789 },
        pendingDedupKey: "invoice-999",
      });
      await promise;

      await instance.enqueue(queueName, {
        name: "send-to-sts",
        payload: { invoiceId: 999, retry: true },
        pendingDedupKey: "invoice-999",
      });

      const allJobs = await queryDatabase.query<RowDataPacket[]>(
        `SELECT id, name, pendingDedupKey, status FROM ${instance.jobsTable()} ORDER BY createdAt`,
      );

      expect(allJobs).toHaveLength(2);
      expect(allJobs[0].status).toBe("failed");
      expect(allJobs[1].status).toBe("pending");
      expect(allJobs[0].pendingDedupKey).toBe("invoice-999");
      expect(allJobs[1].pendingDedupKey).toBe("invoice-999");
    });

    it("should allow different job names with same pendingDedupKey", async () => {
      await instance.enqueue(queueName, [
        {
          name: "job-a",
          payload: { entityId: 555 },
          pendingDedupKey: "entity-555",
        },
        {
          name: "job-b",
          payload: { entityId: 555 },
          pendingDedupKey: "entity-555",
        },
      ]);

      const jobs = await queryDatabase.query<RowDataPacket[]>(`SELECT name, pendingDedupKey FROM ${instance.jobsTable()} ORDER BY name`);

      expect(jobs).toHaveLength(2);
      expect(jobs[0].name).toBe("job-a");
      expect(jobs[1].name).toBe("job-b");
      expect(jobs[0].pendingDedupKey).toBe("entity-555");
      expect(jobs[1].pendingDedupKey).toBe("entity-555");
    });

    it("should allow enqueuing with same pendingDedupKey after job completes", async () => {
      const worker = await instance.work(queueName, () => {});
      void worker.start();
      const job1Promise = instance.getJobExecutionPromise(queueName, 1);
      const job = {
        name: "job-a",
        payload: { entityId: 555 },
        pendingDedupKey: "entity-555",
      };
      await instance.enqueue(queueName, job);
      await job1Promise;
      const job2Promise = instance.getJobExecutionPromise(queueName, 1);
      await instance.enqueue(queueName, job);
      await job2Promise;
      await worker.stop();

      const jobs = await queryDatabase.query<RowDataPacket[]>(
        `SELECT id, name, pendingDedupKey, status FROM ${instance.jobsTable()} ORDER BY createdAt`,
      );
      expect(jobs).toEqual([
        expect.objectContaining({ pendingDedupKey: "entity-555", status: "completed" }),
        expect.objectContaining({ pendingDedupKey: "entity-555", status: "completed" }),
      ]);
    });

    it("should allow enqueuing with same pendingDedupKey after job fails", async () => {
      const queueName = "fail";
      await instance.upsertQueue(queueName, { maxRetries: 1 });
      const worker = await instance.work(queueName, () => {
        throw new Error();
      });
      void worker.start();
      const job1Promise = instance.getJobExecutionPromise(queueName, 1);
      const job = {
        name: "job-a",
        payload: { entityId: 555 },
        pendingDedupKey: "entity-555",
      };
      await instance.enqueue(queueName, job);
      await job1Promise;
      const job2Promise = instance.getJobExecutionPromise(queueName, 1);
      await instance.enqueue(queueName, job);
      await job2Promise;
      await worker.stop();

      const jobs = await queryDatabase.query<RowDataPacket[]>(
        `SELECT id, name, pendingDedupKey, status FROM ${instance.jobsTable()} ORDER BY createdAt`,
      );
      expect(jobs).toEqual([
        expect.objectContaining({ pendingDedupKey: "entity-555", status: "failed" }),
        expect.objectContaining({ pendingDedupKey: "entity-555", status: "failed" }),
      ]);
    });
  });

  describe("Queue pausing", () => {
    const queueName = "test_queue";

    beforeEach(async () => {
      await instance.globalInitialize();
      await instance.upsertQueue(queueName);
    });

    afterEach(async () => {
      await instance.globalDestroy();
    });

    it("should set paused to true when pauseQueue is called", async () => {
      await instance.pauseQueue(queueName);

      const [row] = await queryDatabase.query<RowDataPacket[]>(`SELECT paused FROM ${instance.queuesTable()} WHERE name = ?`, [queueName]);

      expect(row.paused).toBe(1);
    });

    it("should set paused to false when resumeQueue is called", async () => {
      await instance.pauseQueue(queueName);
      await instance.resumeQueue(queueName);

      const [row] = await queryDatabase.query<RowDataPacket[]>(`SELECT paused FROM ${instance.queuesTable()} WHERE name = ?`, [queueName]);

      expect(row.paused).toBe(0);
    });

    it("should not process jobs when queue is paused", async () => {
      const workerCbMock = vi.fn();
      const worker = await instance.work(queueName, workerCbMock);

      await instance.enqueue(queueName, {
        name: "test_job",
        payload: {},
      });

      await instance.pauseQueue(queueName);

      void worker.start();
      await sleep(1000);

      expect(workerCbMock).not.toHaveBeenCalled();
      await worker.stop();
    });

    it("should resume processing jobs when queue is resumed", async () => {
      const promise = instance.getJobExecutionPromise(queueName);
      const workerCbMock = vi.fn();
      const worker = await instance.work(queueName, workerCbMock);

      await instance.enqueue(queueName, {
        name: "test_job",
        payload: {},
      });

      await instance.pauseQueue(queueName);
      void worker.start();

      await instance.resumeQueue(queueName);
      await promise;

      await worker.stop();
      expect(workerCbMock).toHaveBeenCalledTimes(1);
    });

    it("upsertQueue should not reset pause", async () => {
      await instance.pauseQueue(queueName);
      await instance.upsertQueue(queueName);

      const [row] = await queryDatabase.query<RowDataPacket[]>(`SELECT paused FROM ${instance.queuesTable()} WHERE name = ?`, [queueName]);

      expect(row.paused).toBe(1);
    });
  });
});

function isValidUUID(uuid: string): boolean {
  const regex = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
  return regex.test(uuid);
}
