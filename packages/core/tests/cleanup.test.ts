import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { MysqlQueue } from "../src";
import { QueryDatabase } from "./utils/queryDatabase";
import { randomUUID } from "node:crypto";
import { RowDataPacket } from "mysql2/promise";

const DB_URI = "mysql://root:password@localhost:3306/serenis";

function subtractMs(ms: number): Date {
  return new Date(Date.now() - ms);
}

describe("cleanup", () => {
  let mysqlQueue: ReturnType<typeof MysqlQueue>;
  let queryDatabase: ReturnType<typeof QueryDatabase>;

  beforeEach(async () => {
    queryDatabase = QueryDatabase({ dbUri: DB_URI });
    mysqlQueue = MysqlQueue({
      dbUri: DB_URI,
      jobsRetentionDays: 1,
      loggingLevel: "fatal",
      tablesPrefix: `${randomUUID().slice(-4)}_`,
    });
    await mysqlQueue.globalInitialize();
  });

  afterEach(async () => {
    await mysqlQueue.globalDestroy();
    await mysqlQueue.dispose();
    await queryDatabase.dispose();
  });

  it("should delete only completed jobs older than retention", async () => {
    await mysqlQueue.upsertQueue("q");
    const { jobIds } = await mysqlQueue.enqueue("q", [
      { name: "old-completed", payload: {} },
      { name: "recent-completed", payload: {} },
      { name: "pending-job", payload: {} },
    ]);
    const [oldCompletedId, recentCompletedId, pendingId] = jobIds;

    const twoDaysAgo = subtractMs(2 * 24 * 60 * 60 * 1000);
    const now = new Date();

    await queryDatabase.query(`UPDATE ${mysqlQueue.jobsTable()} SET status = 'completed', completedAt = ?, runningAt = ? WHERE id = ?`, [
      twoDaysAgo,
      twoDaysAgo,
      oldCompletedId,
    ]);
    await queryDatabase.query(`UPDATE ${mysqlQueue.jobsTable()} SET status = 'completed', completedAt = ?, runningAt = ? WHERE id = ?`, [
      now,
      now,
      recentCompletedId,
    ]);

    await mysqlQueue.__internal.cleanup();

    const jobsAfter = await queryDatabase.query<RowDataPacket[]>(`SELECT id, status FROM ${mysqlQueue.jobsTable()} ORDER BY name`);
    expect(jobsAfter).toHaveLength(2);
    expect(jobsAfter.map((r) => r.id).sort()).toEqual([pendingId, recentCompletedId].sort());
    expect(jobsAfter.find((r) => r.id === recentCompletedId)?.status).toBe("completed");
    expect(jobsAfter.find((r) => r.id === pendingId)?.status).toBe("pending");
  });

  it("should not delete pending, running, or failed jobs", async () => {
    await mysqlQueue.upsertQueue("q");
    const { jobIds } = await mysqlQueue.enqueue("q", [
      { name: "pending-job", payload: {} },
      { name: "running-job", payload: {} },
      { name: "failed-job", payload: {} },
    ]);
    const [pendingId, runningId, failedId] = jobIds;
    const twoDaysAgo = subtractMs(2 * 24 * 60 * 60 * 1000);

    await queryDatabase.query(`UPDATE ${mysqlQueue.jobsTable()} SET createdAt = ? WHERE id = ?`, [twoDaysAgo, pendingId]);
    await queryDatabase.query(`UPDATE ${mysqlQueue.jobsTable()} SET status = 'running', runningAt = ? WHERE id = ?`, [
      twoDaysAgo,
      runningId,
    ]);
    await queryDatabase.query(`UPDATE ${mysqlQueue.jobsTable()} SET status = 'failed', failedAt = ?, completedAt = NULL WHERE id = ?`, [
      twoDaysAgo,
      failedId,
    ]);

    await mysqlQueue.__internal.cleanup();

    const jobsAfter = await queryDatabase.query<RowDataPacket[]>(`SELECT id, status FROM ${mysqlQueue.jobsTable()} ORDER BY name`);
    expect(jobsAfter).toHaveLength(3);
    expect(jobsAfter.find((r) => r.id === pendingId)?.status).toBe("pending");
    expect(jobsAfter.find((r) => r.id === runningId)?.status).toBe("running");
    expect(jobsAfter.find((r) => r.id === failedId)?.status).toBe("failed");
  });

  it("should respect partitionKey and only delete jobs of that partition", async () => {
    const prefix = `${randomUUID().slice(-4)}_`;
    const db = QueryDatabase({ dbUri: DB_URI });
    const queueP1 = MysqlQueue({
      dbUri: DB_URI,
      jobsRetentionDays: 1,
      loggingLevel: "fatal",
      partitionKey: "p1",
      tablesPrefix: prefix,
    });
    const queueP2 = MysqlQueue({
      dbUri: DB_URI,
      jobsRetentionDays: 1,
      loggingLevel: "fatal",
      partitionKey: "p2",
      tablesPrefix: prefix,
    });
    await queueP1.globalInitialize();
    await queueP1.upsertQueue("q1");
    await queueP2.upsertQueue("q2");

    const { jobIds: ids1 } = await queueP1.enqueue("q1", [{ name: "job-p1", payload: {} }]);
    const { jobIds: ids2 } = await queueP2.enqueue("q2", [{ name: "job-p2", payload: {} }]);
    const jobIdP1 = ids1[0];
    const jobIdP2 = ids2[0];
    const twoDaysAgo = subtractMs(2 * 24 * 60 * 60 * 1000);

    const jobsTable = queueP1.jobsTable();
    await db.query(`UPDATE ${jobsTable} SET status = 'completed', completedAt = ?, runningAt = ? WHERE id = ?`, [
      twoDaysAgo,
      twoDaysAgo,
      jobIdP1,
    ]);
    await db.query(`UPDATE ${jobsTable} SET status = 'completed', completedAt = ?, runningAt = ? WHERE id = ?`, [
      twoDaysAgo,
      twoDaysAgo,
      jobIdP2,
    ]);

    await queueP1.__internal.cleanup();

    const remaining = await db.query<RowDataPacket[]>(`SELECT id FROM ${jobsTable}`);
    expect(remaining).toHaveLength(1);
    expect(remaining[0].id).toBe(jobIdP2);

    await queueP1.globalDestroy();
    await queueP1.dispose();
    await queueP2.dispose();
    await db.dispose();
  });

  it("should complete without error when there are no completed jobs to delete", async () => {
    await mysqlQueue.upsertQueue("q");
    await mysqlQueue.enqueue("q", [{ name: "pending-job", payload: {} }]);

    await expect(mysqlQueue.__internal.cleanup()).resolves.toBeUndefined();

    const jobsAfter = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${mysqlQueue.jobsTable()}`);
    expect(jobsAfter).toHaveLength(1);
  });

  it("should delete in batches when many old completed jobs exist", async () => {
    await mysqlQueue.upsertQueue("q");
    const { jobIds } = await mysqlQueue.enqueue(
      "q",
      Array.from({ length: 5 }, (_, i) => ({ name: `job-${i}`, payload: {} })),
    );
    const twoDaysAgo = subtractMs(2 * 24 * 60 * 60 * 1000);

    for (const id of jobIds) {
      await queryDatabase.query(`UPDATE ${mysqlQueue.jobsTable()} SET status = 'completed', completedAt = ?, runningAt = ? WHERE id = ?`, [
        twoDaysAgo,
        twoDaysAgo,
        id,
      ]);
    }

    await mysqlQueue.__internal.cleanup();

    const jobsAfter = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${mysqlQueue.jobsTable()}`);
    expect(jobsAfter).toHaveLength(0);
  });

  it("should use per-queue retention when set, falling back to global retention", async () => {
    const prefix = `${randomUUID().slice(-4)}_`;
    const db = QueryDatabase({ dbUri: DB_URI });
    const instance = MysqlQueue({
      dbUri: DB_URI,
      jobsRetentionDays: 1,
      loggingLevel: "fatal",
      tablesPrefix: prefix,
    });
    await instance.globalInitialize();

    await instance.upsertQueue("short-retention", { jobsRetentionDays: 1 });
    await instance.upsertQueue("long-retention", { jobsRetentionDays: 30 });
    await instance.upsertQueue("default-retention");

    const { jobIds: shortIds } = await instance.enqueue("short-retention", [{ name: "j", payload: {} }]);
    const { jobIds: longIds } = await instance.enqueue("long-retention", [{ name: "j", payload: {} }]);
    const { jobIds: defaultIds } = await instance.enqueue("default-retention", [{ name: "j", payload: {} }]);

    const twoDaysAgo = subtractMs(2 * 24 * 60 * 60 * 1000);
    const jobsTable = instance.jobsTable();
    for (const id of [...shortIds, ...longIds, ...defaultIds]) {
      await db.query(`UPDATE ${jobsTable} SET status = 'completed', completedAt = ?, runningAt = ? WHERE id = ?`, [
        twoDaysAgo,
        twoDaysAgo,
        id,
      ]);
    }

    await instance.__internal.cleanup();

    const remaining = await db.query<RowDataPacket[]>(`SELECT id FROM ${jobsTable}`);
    const remainingIds = remaining.map((r) => r.id);

    expect(remainingIds).not.toContain(shortIds[0]);
    expect(remainingIds).not.toContain(defaultIds[0]);
    expect(remainingIds).toContain(longIds[0]);

    await instance.globalDestroy();
    await instance.dispose();
    await db.dispose();
  });

  it("getCleanupNextRun returns Date or null", () => {
    const nextRun = mysqlQueue.__internal.getCleanupNextRun();
    expect(nextRun === null || nextRun instanceof Date).toBe(true);
  });
});
