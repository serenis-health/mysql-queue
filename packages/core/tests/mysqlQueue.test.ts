import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { MysqlQueue } from "../src";
import { QueryDatabase } from "./utils/queryDatabase";
import { randomUUID } from "node:crypto";
import { RowDataPacket } from "mysql2";

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
      await instance.initialize();

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
      ]);
    });

    it("should create tables", async () => {
      await instance.initialize();

      const rows = await queryDatabase.query<RowDataPacket[]>("SHOW TABLES;");
      const tableNames = rows.map((row) => row.Tables_in_serenis);
      expect(tableNames).toEqual(expect.arrayContaining([instance.jobsTable(), instance.migrationTable(), instance.queuesTable()]));

      await instance.destroy();
    });
  });

  describe("destroy", () => {
    beforeEach(async () => {
      await instance.initialize();
      await queryDatabase.query(`CREATE TABLE IF NOT EXISTS another_table (id INT AUTO_INCREMENT PRIMARY KEY);`);
    });

    afterEach(async () => {
      await queryDatabase.query("DROP TABLE IF EXISTS another_table;");
    });

    it("should remove all tables", async () => {
      await instance.destroy();

      const rows = await queryDatabase.query<RowDataPacket[]>("SHOW TABLES;");
      const tableNames = rows.map((row) => row.Tables_in_serenis);
      expect(tableNames).toEqual(expect.arrayContaining(["another_table"]));
    });
  });

  describe("upsertQueue", () => {
    beforeAll(async () => {
      await instance.initialize();
    });

    afterAll(async () => {
      await instance.destroy();
    });

    it("should create a row in mysql_queue_queues", async () => {
      const queueName = "test_quque";
      await instance.upsertQueue(queueName);

      const [row] = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${instance.queuesTable()};`);

      expect(isValidUUID(row.id)).toBeTruthy();
      expect(row).toEqual({
        backoffMultiplier: 2,
        id: expect.any(String),
        maxDurationMs: 5000,
        maxRetries: 3,
        minDelayMs: 1000,
        name: "test_quque",
      });
    });

    it("should update the row in mysql_queue_queues case already created", async () => {
      const queueName = "test_quque";
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
        name: "test_quque",
      });
    });
  });

  describe("enqueue", () => {
    const queueName = "test_quque";

    beforeEach(async () => {
      await instance.initialize();
      await instance.upsertQueue(queueName);
    });

    afterEach(async () => {
      await instance.destroy();
    });

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
        failedAt: null,
        id: expect.any(String),
        latestFailureReason: null,
        name: "test_job",
        payload: { message: "Hello, world!" },
        priority: 0,
        queueId: expect.any(String),
        startAfter: null,
        status: "pending",
      });
    });

    it("should throw case queue not exists", async () => {
      const unknownQueueName = "anotherQueue";

      await expect(
        instance.enqueue(unknownQueueName, {
          name: "test_job",
          payload: { message: "Hello, world!" },
        }),
      ).rejects.toThrowError("Failed to add jobs, maybe queue does not exist");
    });

    it("should not throw case 0 jobs params passed", async () => {
      await instance.enqueue(queueName, []);
      const sql = instance.getEnqueueRawSql(queueName, []);
      expect(sql).toEqual("SELECT NULL LIMIT 0;");
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
      expect(workerCbMock).toHaveBeenCalledWith(expect.objectContaining({ id: jobIds[0] }), expect.anything(), expect.anything());
    });
  });

  describe("work", () => {
    const queueName = "test_queue";

    beforeEach(async () => {
      await instance.initialize();
      await instance.upsertQueue(queueName, { maxRetries: 1 });
    });

    afterEach(async () => {
      await instance.destroy();
    });

    it("should fail job after attempts", async () => {
      const promise = instance.getJobExecutionPromise(queueName);

      const workerCbMock = vi.fn().mockImplementation(() => {
        throw new Error("a".repeat(120));
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
        failedAt: expect.any(Date),
        latestFailureReason: expect.stringContaining("<truncated>"),
        status: "failed",
      });

      await worker.stop();
    });
  });
});

function isValidUUID(uuid: string): boolean {
  const regex = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
  return regex.test(uuid);
}
