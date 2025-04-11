import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
} from "vitest";
import { MysqlQueue } from "../src";
import { QueryDatabase } from "./utils/queryDatabase";
import { RowDataPacket } from "mysql2";

const dbUri = "mysql://root:password@localhost:3306/serenis";

describe("mysqlQueue", () => {
  const queryDatabase = QueryDatabase({ dbUri });
  const instance = MysqlQueue({
    dbUri,
    loggingLevel: "fatal",
  });

  describe("initialize", () => {
    it("should apply migrations", async () => {
      await instance.initialize();

      const rows = await queryDatabase.query<RowDataPacket[]>(
        "SELECT * FROM mysql_queue_migrations;",
      );
      expect(rows).toEqual([
        {
          id: 1,
          name: "create-queues-table",
          applied_at: expect.any(Date),
        },
        {
          id: 2,
          name: "create-jobs-table",
          applied_at: expect.any(Date),
        },
      ]);
    });

    it("should create tables", async () => {
      await instance.initialize();

      const rows = await queryDatabase.query<RowDataPacket[]>("SHOW TABLES;");
      const tableNames = rows.map((row) => row.Tables_in_serenis);
      expect(tableNames).toEqual([
        "mysql_queue_jobs",
        "mysql_queue_migrations",
        "mysql_queue_queues",
      ]);

      await instance.destroy();
    });
  });

  describe("destroy", () => {
    beforeEach(async () => {
      await instance.initialize();
      await queryDatabase.query(
        `CREATE TABLE IF NOT EXISTS another_table (id INT AUTO_INCREMENT PRIMARY KEY);`,
      );
    });

    afterEach(async () => {
      await queryDatabase.query("DROP TABLE IF EXISTS another_table;");
    });

    it("should remove all tables", async () => {
      await instance.destroy();

      const rows = await queryDatabase.query<RowDataPacket[]>("SHOW TABLES;");
      const tableNames = rows.map((row) => row.Tables_in_serenis);
      expect(tableNames).toEqual(["another_table"]);
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

      const [row] = await queryDatabase.query<RowDataPacket[]>(
        "SELECT * FROM mysql_queue_queues;",
      );

      expect(isValidUUID(row.id)).toBeTruthy();
      expect(row).toEqual({
        id: expect.any(String),
        name: "test_quque",
        maxRetries: 3,
        maxDurationMs: 5000,
        minDelayMs: 1000,
        backoffMultiplier: 2,
      });
    });

    it("should update the row in mysql_queue_queues case already created", async () => {
      const queueName = "test_quque";
      await instance.upsertQueue(queueName);

      await instance.upsertQueue(queueName, {
        maxRetries: 5,
        maxDurationMs: 10000,
        minDelayMs: 2000,
        backoffMultiplier: 3,
      });

      const [row] = await queryDatabase.query<RowDataPacket[]>(
        "SELECT * FROM mysql_queue_queues;",
      );

      expect(row).toEqual({
        id: expect.any(String),
        name: "test_quque",
        maxRetries: 5,
        maxDurationMs: 10000,
        minDelayMs: 2000,
        backoffMultiplier: 3,
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

      const [row] = await queryDatabase.query<RowDataPacket[]>(
        "SELECT * FROM mysql_queue_jobs;",
      );

      expect(isValidUUID(jobIds[0])).toBeTruthy();
      expect(row).toEqual({
        id: expect.any(String),
        name: "test_job",
        payload: { message: "Hello, world!" },
        status: "pending",
        startAfter: null,
        createdAt: expect.any(Date),
        completedAt: null,
        failedAt: null,
        latestFailureReason: null,
        attempts: 0,
        priority: 0,
        queueId: expect.any(String),
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
  });
});

function isValidUUID(uuid: string): boolean {
  const regex =
    /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
  return regex.test(uuid);
}
