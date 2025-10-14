import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { MysqlQueue } from "../src";
import { QueryDatabase } from "./utils/queryDatabase";
import { randomUUID } from "node:crypto";
import { RowDataPacket } from "mysql2/promise";

const DB_URI = "mysql://root:password@localhost:3306/serenis";

describe("rescuer", () => {
  let mysqlQueue: MysqlQueue;
  let queryDatabase: ReturnType<typeof QueryDatabase>;

  beforeEach(async () => {
    queryDatabase = QueryDatabase({ dbUri: DB_URI });
    mysqlQueue = MysqlQueue({
      dbUri: DB_URI,
      //loggingLevel: "fatal",
      tablesPrefix: `${randomUUID().slice(-4)}_`,
    });
    await mysqlQueue.globalInitialize();
  });

  afterEach(async () => {
    await mysqlQueue.globalDestroy();
    await mysqlQueue.dispose();
    await queryDatabase.dispose();
  });

  it("given initialized instance, should scheduled the next run for next hour", () => {
    const nextRun = mysqlQueue.__internal.getRescuerNextRun();
    const nextHour = new Date();
    nextHour.setHours(nextHour.getHours() + 1, 0, 0, 0);

    expect(nextRun.getTime()).toEqual(nextRun.getTime());
  });

  it("given one stuck job, should re set pending status", async () => {
    await mysqlQueue.upsertQueue("q", { maxRetries: 2 });
    const {
      jobIds: [jobId],
    } = await mysqlQueue.enqueue("q", [{ name: "foo", payload: {} }]);

    await simulateJobRun(jobId);

    const [jobBefore] = await queryDatabase.query<RowDataPacket[]>(`SELECT * from ${mysqlQueue.jobsTable()} WHERE id = ?;`, [jobId]);
    expect(jobBefore).toEqual(expect.objectContaining({ id: jobId, status: "running" }));

    await mysqlQueue.__internal.rescue();

    const [jobAfter] = await queryDatabase.query<RowDataPacket[]>(`SELECT * from ${mysqlQueue.jobsTable()} WHERE id = ?;`, [jobId]);
    expect(jobAfter).toEqual(
      expect.objectContaining({
        attempts: 1,
        errors: [
          {
            at: expect.any(String),
            attempt: 1,
            error: '"{\\"message\\":\\"Job stuck in running state and was rescued\\",\\"name\\":\\"RescuerError\\"}"',
          },
        ],
        id: jobId,
        status: "pending",
      }),
    );
  });

  it("given one stuck job on last attempt, should set failed status", async () => {
    await mysqlQueue.upsertQueue("q", { maxRetries: 1 });
    const {
      jobIds: [jobId],
    } = await mysqlQueue.enqueue("q", [{ name: "foo", payload: {} }]);

    await simulateJobRun(jobId);

    const [jobBefore] = await queryDatabase.query<RowDataPacket[]>(`SELECT * from ${mysqlQueue.jobsTable()} WHERE id = ?;`, [jobId]);
    expect(jobBefore).toEqual(expect.objectContaining({ id: jobId, status: "running" }));

    await mysqlQueue.__internal.rescue();

    const [jobAfter] = await queryDatabase.query<RowDataPacket[]>(`SELECT * from ${mysqlQueue.jobsTable()} WHERE id = ?;`, [jobId]);
    expect(jobAfter).toEqual(
      expect.objectContaining({
        attempts: 1,
        errors: [
          {
            at: expect.any(String),
            attempt: 1,
            error: '"{\\"message\\":\\"Job stuck in running state and was rescued\\",\\"name\\":\\"RescuerError\\"}"',
          },
        ],
        id: jobId,
        status: "failed",
      }),
    );
  });

  it("given one stuck job, should re set pending status two times and then fail", async () => {
    await mysqlQueue.upsertQueue("q", { maxRetries: 3 });
    const {
      jobIds: [jobId],
    } = await mysqlQueue.enqueue("q", [{ name: "foo", payload: {} }]);

    await simulateJobRun(jobId);
    await mysqlQueue.__internal.rescue();
    await simulateJobRun(jobId);
    await mysqlQueue.__internal.rescue();

    const [jobAfter] = await queryDatabase.query<RowDataPacket[]>(`SELECT * from ${mysqlQueue.jobsTable()} WHERE id = ?;`, [jobId]);
    expect(jobAfter).toEqual(
      expect.objectContaining({
        attempts: 2,
        errors: [
          {
            at: expect.any(String),
            attempt: 1,
            error: '"{\\"message\\":\\"Job stuck in running state and was rescued\\",\\"name\\":\\"RescuerError\\"}"',
          },
          {
            at: expect.any(String),
            attempt: 2,
            error: '"{\\"message\\":\\"Job stuck in running state and was rescued\\",\\"name\\":\\"RescuerError\\"}"',
          },
        ],
        id: jobId,
        status: "pending",
      }),
    );

    await simulateJobRun(jobId);
    await mysqlQueue.__internal.rescue();

    const [jobAfter2] = await queryDatabase.query<RowDataPacket[]>(`SELECT * from ${mysqlQueue.jobsTable()} WHERE id = ?;`, [jobId]);
    expect(jobAfter2).toEqual(
      expect.objectContaining({
        attempts: 3,
        errors: [
          {
            at: expect.any(String),
            attempt: 1,
            error: '"{\\"message\\":\\"Job stuck in running state and was rescued\\",\\"name\\":\\"RescuerError\\"}"',
          },
          {
            at: expect.any(String),
            attempt: 2,
            error: '"{\\"message\\":\\"Job stuck in running state and was rescued\\",\\"name\\":\\"RescuerError\\"}"',
          },
          {
            at: expect.any(String),
            attempt: 3,
            error: '"{\\"message\\":\\"Job stuck in running state and was rescued\\",\\"name\\":\\"RescuerError\\"}"',
          },
        ],
        id: jobId,
        status: "failed",
      }),
    );
  });

  async function simulateJobRun(jobId: string) {
    await queryDatabase.query(`UPDATE ${mysqlQueue.jobsTable()}  SET status = 'running', runningAt = ? WHERE id = ?;`, [
      subtractMs(40_000_000),
      jobId,
    ]);
  }
});

function subtractMs(ms: number) {
  const now = new Date();
  return new Date(now.getTime() - ms);
}
