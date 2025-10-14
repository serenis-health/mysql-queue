import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { approxEqual } from "./utils/approxEqual";
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

  it("given uninitialized instance, getNextRun should return undefined", () => {
    const uninitializedQueue = MysqlQueue({ dbUri: DB_URI, loggingLevel: "fatal" });

    const nextRun = uninitializedQueue.__internal.getRescuerNextRun();
    expect(nextRun).toBeNull();
  });

  it("given initialized instance, should scheduled the next run in an hour", () => {
    const expectedNextRun = new Date(Date.now() + 1_800_000);
    const nextRun = mysqlQueue.__internal.getRescuerNextRun();

    expect(approxEqual(nextRun!.getTime(), expectedNextRun.getTime(), 10)).toBeTruthy();
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
            error: '{"message":"Job stuck in running state and was rescued","name":"RescuerError"}',
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
            error: '{"message":"Job stuck in running state and was rescued","name":"RescuerError"}',
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
            error: '{"message":"Job stuck in running state and was rescued","name":"RescuerError"}',
          },
          {
            at: expect.any(String),
            attempt: 2,
            error: '{"message":"Job stuck in running state and was rescued","name":"RescuerError"}',
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
            error: '{"message":"Job stuck in running state and was rescued","name":"RescuerError"}',
          },
          {
            at: expect.any(String),
            attempt: 2,
            error: '{"message":"Job stuck in running state and was rescued","name":"RescuerError"}',
          },
          {
            at: expect.any(String),
            attempt: 3,
            error: '{"message":"Job stuck in running state and was rescued","name":"RescuerError"}',
          },
        ],
        id: jobId,
        status: "failed",
      }),
    );
  });

  it("given no stuck jobs, should complete without errors", async () => {
    await mysqlQueue.upsertQueue("q", { maxRetries: 2 });
    await mysqlQueue.enqueue("q", [{ name: "foo", payload: {} }]);

    await mysqlQueue.__internal.rescue();

    const jobs = await queryDatabase.query<RowDataPacket[]>(`SELECT * from ${mysqlQueue.jobsTable()};`);
    expect(jobs[0]).toEqual(expect.objectContaining({ attempts: 0, status: "pending" }));
  });

  it("given multiple queues with stuck jobs, should rescue all", async () => {
    await mysqlQueue.upsertQueue("queue-a", { maxRetries: 2 });
    await mysqlQueue.upsertQueue("queue-b", { maxRetries: 3 });
    await mysqlQueue.upsertQueue("queue-c", { maxRetries: 1 });

    const {
      jobIds: [jobId1],
    } = await mysqlQueue.enqueue("queue-a", [{ name: "job-a", payload: {} }]);
    const {
      jobIds: [jobId2],
    } = await mysqlQueue.enqueue("queue-b", [{ name: "job-b", payload: {} }]);
    const {
      jobIds: [jobId3],
    } = await mysqlQueue.enqueue("queue-c", [{ name: "job-c", payload: {} }]);

    await simulateJobRun(jobId1);
    await simulateJobRun(jobId2);
    await simulateJobRun(jobId3);

    await mysqlQueue.__internal.rescue();

    const [job1] = await queryDatabase.query<RowDataPacket[]>(`SELECT * from ${mysqlQueue.jobsTable()} WHERE id = ?;`, [jobId1]);
    const [job2] = await queryDatabase.query<RowDataPacket[]>(`SELECT * from ${mysqlQueue.jobsTable()} WHERE id = ?;`, [jobId2]);
    const [job3] = await queryDatabase.query<RowDataPacket[]>(`SELECT * from ${mysqlQueue.jobsTable()} WHERE id = ?;`, [jobId3]);

    expect(job1).toEqual(expect.objectContaining({ attempts: 1, status: "pending" }));
    expect(job2).toEqual(expect.objectContaining({ attempts: 1, status: "pending" }));
    expect(job3).toEqual(expect.objectContaining({ attempts: 1, status: "failed" })); // Only 1 retry
  });

  it("given 100+ stuck jobs, should process first 100", async () => {
    await mysqlQueue.upsertQueue("q", { maxRetries: 2 });

    const jobs = Array.from({ length: 150 }, (_, i) => ({ name: `job-${i}`, payload: { index: i } }));
    const { jobIds } = await mysqlQueue.enqueue("q", jobs);

    for (const jobId of jobIds) await simulateJobRun(jobId);

    await mysqlQueue.__internal.rescue();

    const allJobs = await queryDatabase.query<RowDataPacket[]>(`SELECT status, attempts from ${mysqlQueue.jobsTable()} ORDER BY id;`);

    const rescued = allJobs.filter((j) => j.status === "pending" && j.attempts === 1);
    const stillStuck = allJobs.filter((j) => j.status === "running" && j.attempts === 0);

    expect(rescued.length).toBe(100);
    expect(stillStuck.length).toBe(50);
  });

  it("given stuck job with custom backoff, should calculate correct startAfter", async () => {
    await mysqlQueue.upsertQueue("q", { backoffMultiplier: 2, maxRetries: 3, minDelayMs: 1000 });
    const {
      jobIds: [jobId],
    } = await mysqlQueue.enqueue("q", [{ name: "foo", payload: {} }]);

    await simulateJobRun(jobId);
    await mysqlQueue.__internal.rescue();

    const [job1] = await queryDatabase.query<RowDataPacket[]>(`SELECT * from ${mysqlQueue.jobsTable()} WHERE id = ?;`, [jobId]);

    expect(job1.attempts).toBe(1);
    expect(job1.status).toBe("pending");

    // Rescue again to test exponential backoff
    await simulateJobRun(jobId);
    await mysqlQueue.__internal.rescue();

    const [job2] = await queryDatabase.query<RowDataPacket[]>(`SELECT * from ${mysqlQueue.jobsTable()} WHERE id = ?;`, [jobId]);

    // After second rescue: attempts=2, startAfter = now + minDelayMs * backoffMultiplier^(attempts-1)
    // = now + 1000 * 2^(2-1) = now + 1000 * 2^1 = now + 2000ms
    const startAfter1 = new Date(job1.startAfter).getTime();
    const startAfter2 = new Date(job2.startAfter).getTime();

    // The second startAfter should be approximately 1000ms later than first (difference in delay calculation)
    // First had +1000ms, second has +2000ms, so difference in execution time plus 1000ms delay increase
    expect(job2.attempts).toBe(2);
    expect(job2.status).toBe("pending");
    expect(startAfter2).toBeGreaterThan(startAfter1);
  });

  it("given disposed instance, should not run scheduled rescue", async () => {
    const testQueue = MysqlQueue({
      dbUri: DB_URI,
      loggingLevel: "fatal",
      tablesPrefix: `${randomUUID().slice(-4)}_`,
    });
    await testQueue.globalInitialize();

    const nextRun = testQueue.__internal.getRescuerNextRun();
    expect(nextRun).toBeDefined();

    await testQueue.globalDestroy();
    await testQueue.dispose();

    const nextRun2 = testQueue.__internal.getRescuerNextRun();
    expect(nextRun2).toBeNull();
  });

  it("given stuck job with deleted queue, should throw queue not found error", async () => {
    // Create a queue and job normally
    await mysqlQueue.upsertQueue("q", { maxRetries: 2 });
    const {
      jobIds: [jobId],
    } = await mysqlQueue.enqueue("q", [{ name: "foo", payload: {} }]);

    await simulateJobRun(jobId);

    const [queueInfo] = await queryDatabase.query<RowDataPacket[]>(`SELECT id FROM ${mysqlQueue.queuesTable()} WHERE name = 'q'`);
    const queueId = queueInfo.id;

    await queryDatabase.query(`SET FOREIGN_KEY_CHECKS = 0`);
    await queryDatabase.query(`DELETE FROM ${mysqlQueue.queuesTable()} WHERE id = ?`, [queueId]);
    await queryDatabase.query(`SET FOREIGN_KEY_CHECKS = 1`);

    const jobs = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${mysqlQueue.jobsTable()} WHERE id = ?`, [jobId]);
    expect(jobs.length).toBe(1);

    await expect(mysqlQueue.__internal.rescue()).rejects.toThrow("Queue not found");
  });

  it("given jobs in non-running states, should not rescue them", async () => {
    await mysqlQueue.upsertQueue("q", { maxRetries: 2 });

    // Create 4 jobs
    const { jobIds } = await mysqlQueue.enqueue("q", [
      { name: "job1", payload: {} },
      { name: "job2", payload: {} },
      { name: "job3", payload: {} },
      { name: "job4", payload: {} },
    ]);

    const [jobId1, jobId2, jobId3, jobId4] = jobIds;
    await queryDatabase.query(`UPDATE ${mysqlQueue.jobsTable()} SET status = 'pending', createdAt = ? WHERE id = ?;`, [
      subtractMs(40_000_000),
      jobId1,
    ]);
    await queryDatabase.query(`UPDATE ${mysqlQueue.jobsTable()} SET status = 'completed', completedAt = ?, runningAt = ? WHERE id = ?;`, [
      subtractMs(40_000_000),
      subtractMs(40_000_000),
      jobId2,
    ]);
    await queryDatabase.query(`UPDATE ${mysqlQueue.jobsTable()} SET status = 'failed', failedAt = ?, runningAt = ? WHERE id = ?;`, [
      subtractMs(40_000_000),
      subtractMs(40_000_000),
      jobId3,
    ]);

    await simulateJobRun(jobId4);

    await mysqlQueue.__internal.rescue();

    const [job1] = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${mysqlQueue.jobsTable()} WHERE id = ?`, [jobId1]);
    const [job2] = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${mysqlQueue.jobsTable()} WHERE id = ?`, [jobId2]);
    const [job3] = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${mysqlQueue.jobsTable()} WHERE id = ?`, [jobId3]);
    const [job4] = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${mysqlQueue.jobsTable()} WHERE id = ?`, [jobId4]);
    expect(job1).toEqual(expect.objectContaining({ attempts: 0, status: "pending" }));
    expect(job2).toEqual(expect.objectContaining({ status: "completed" }));
    expect(job3).toEqual(expect.objectContaining({ status: "failed" }));
    expect(job4).toEqual(expect.objectContaining({ attempts: 1, status: "pending" }));
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
