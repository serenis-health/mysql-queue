import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { Connection } from "../src/types";
import { createPool } from "mysql2/promise";
import { MysqlQueue } from "../src";
import { QueryDatabase } from "./utils/queryDatabase";
import { randomUUID } from "node:crypto";
import { ResultSetHeader, RowDataPacket } from "mysql2";

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

    beforeAll(async () => {
      await instance.initialize();
      await instance.upsertQueue(queueName);
    });

    afterAll(async () => {
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

    it("should create a row in mysql_queue_jobs case external connection", async () => {
      const pool = createPool(dbUri);
      const connection = await pool.getConnection();

      const externalConnectionMock = vi.fn();
      const externalConnection: Connection = {
        query: async (query: string, values?: unknown[]) => {
          const result = await connection.query<ResultSetHeader>(query, values);
          externalConnectionMock(query, values, result);
          return { affectedRows: result[0].affectedRows };
        },
      };

      const { jobIds } = await instance.enqueue(
        queueName,
        {
          name: "test_job",
          payload: { message: "Hello, world!" },
        },
        externalConnection,
      );

      const [row] = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${instance.jobsTable()};`);
      expect(row.id).toEqual(jobIds[0]);

      expect(externalConnectionMock).toHaveBeenCalled();
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
  });
});

function isValidUUID(uuid: string): boolean {
  const regex = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
  return regex.test(uuid);
}
