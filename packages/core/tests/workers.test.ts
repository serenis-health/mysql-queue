/* eslint-disable @typescript-eslint/no-explicit-any */
import { afterEach, beforeEach, describe, expect, it, vitest } from "vitest";
import { CallbackContext, MysqlQueue } from "../src";
import { createPool, RowDataPacket } from "mysql2/promise";
import { approxEqual } from "./utils/approxEqual";
import { connectionToSession } from "../src/jobProcessor";
import { Job } from "../src";
import { randomUUID } from "node:crypto";

const DB_URI = "mysql://root:password@localhost:3306/serenis";

describe("workers", () => {
  const pool = createPool(DB_URI);

  let mysqlQueue: MysqlQueue;

  beforeEach(async () => {
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
  });

  describe("two worker with different handler latency", () => {
    const queueName = "test_queue";
    const Worker1HandlerMock = { handle: vitest.fn().mockImplementation(async () => await sleep(3000)) };
    const Worker2HandlerMock = { handle: vitest.fn() };
    let worker1: Awaited<ReturnType<typeof mysqlQueue.work>>;
    let worker2: Awaited<ReturnType<typeof mysqlQueue.work>>;

    beforeEach(async () => {
      await mysqlQueue.upsertQueue(queueName, { maxDurationMs: 10_000 });
      worker1 = await mysqlQueue.work(queueName, Worker1HandlerMock.handle, undefined, 5);
      worker2 = await mysqlQueue.work(queueName, Worker2HandlerMock.handle, undefined, 5);
    });

    afterEach(async () => {
      await Promise.all([worker1.stop(), worker2.stop()]);
    });

    it("should distribute jobs between two workers without any job being processed more than once", async () => {
      const promise = mysqlQueue.getJobExecutionPromise(queueName, 10);

      await enqueueNJobs(mysqlQueue, queueName, 10);
      void Promise.all([worker1.start(), worker2.start()]);
      await promise;

      const w1JobIds = Worker1HandlerMock.handle.mock.calls.flatMap((c) => c[0].map((j: any) => j.id));
      const w2JobIds = Worker2HandlerMock.handle.mock.calls.flatMap((c) => c[0].map((j: any) => j.id));
      expect(haveNoCommonElements(w1JobIds, w2JobIds)).toBeTruthy();
      expect(Worker1HandlerMock.handle).toHaveBeenCalledTimes(1);
      expect(Worker2HandlerMock.handle).toHaveBeenCalledTimes(1);
    });

    it("should ensure that a slow worker does not block or delay other workers, case jobs already on queue", async () => {
      await enqueueNJobs(mysqlQueue, queueName, 10);

      const promise = mysqlQueue.getJobExecutionPromise(queueName, 10);
      void Promise.all([worker1.start(), worker2.start()]);
      await promise;

      const [rows] = await pool.query<RowDataPacket[]>(`SELECT id, createdAt, completedAt from ${mysqlQueue.jobsTable()}`);
      const jobs = rows.map((j) => ({ ...j, durationMs: new Date(j.completedAt).getTime() - new Date(j.createdAt).getTime() }));
      expect(hasExactly(jobs, 5, (item) => item.durationMs > 3000)).toBeTruthy();
      expect(hasExactly(jobs, 5, (item) => item.durationMs < 100)).toBeTruthy();
    });

    it("should ensure that a slow worker does not block or delay other workers, case no jobs on queue", async () => {
      void Promise.all([worker1.start(), worker2.start()]);

      const promise = mysqlQueue.getJobExecutionPromise(queueName, 10);
      await enqueueNJobs(mysqlQueue, queueName, 10);
      await promise;

      const [rows] = await pool.query<RowDataPacket[]>(`SELECT id, createdAt, completedAt from ${mysqlQueue.jobsTable()}`);
      const jobs = rows.map((j) => ({ ...j, durationMs: new Date(j.completedAt).getTime() - new Date(j.createdAt).getTime() }));
      expect(hasExactly(jobs, 5, (item) => item.durationMs > 3000)).toBeTruthy();
      expect(hasExactly(jobs, 5, (item) => item.durationMs < 1000)).toBeTruthy();
    });
  });

  describe("one worker with super fast handler", () => {
    let worker: Awaited<ReturnType<typeof mysqlQueue.work>>;

    afterEach(async () => {
      await worker.stop();
    });

    it("should apply the right backoff strategy", async () => {
      const calls: Date[] = [];
      const Worker1HandlerMock = {
        handle: vitest.fn().mockImplementation(() => {
          calls.push(new Date());
          throw new Error("Boom");
        }),
      };
      const queueName = "test_queue2";
      await mysqlQueue.upsertQueue(queueName, { backoffMultiplier: 2, maxRetries: 4 });
      worker = await mysqlQueue.work(queueName, Worker1HandlerMock.handle, 100);
      void worker.start();

      const promise = mysqlQueue.getJobExecutionPromise(queueName, 4);
      await enqueueNJobs(mysqlQueue, queueName, 1);
      await promise;

      const expected = [0, 1032, 3064, 7135];
      callsToTimeFromFirst(calls).forEach((ms, i) => {
        expect(approxEqual(ms, expected[i], 200)).toBeTruthy();
      });
    }, 10_000);

    it("should abort handling if handler exceeds max duration", async () => {
      const Worker1HandlerMock = {
        handle: vitest.fn().mockImplementation(async () => {
          await sleep(2000);
        }),
      };
      const queueName = "test_queue";
      await mysqlQueue.upsertQueue(queueName, { maxDurationMs: 1000, maxRetries: 1 });
      worker = await mysqlQueue.work(queueName, Worker1HandlerMock.handle, 100);
      void worker.start();

      const promise = mysqlQueue.getJobExecutionPromise(queueName, 1);
      await enqueueNJobs(mysqlQueue, queueName, 1);
      await promise;

      const [rows] = await pool.query<RowDataPacket[]>(`SELECT * from ${mysqlQueue.jobsTable()}`);
      expect(rows[0]).toMatchObject({
        errors: [
          {
            at: expect.any(String),
            attempt: 1,
            error: expect.stringContaining("Job execution exceed the timeout of 1000"),
          },
        ],
        failedAt: expect.any(Date),
        status: "failed",
      });
    }, 10_000);

    it("should handle jobs in order of priority", async () => {
      const WorkerHandlerMock = { handle: vitest.fn() };
      const queueName = "test_queue";
      await mysqlQueue.upsertQueue(queueName);
      worker = await mysqlQueue.work(queueName, WorkerHandlerMock.handle, 100);
      void worker.start();

      const promise = mysqlQueue.getJobExecutionPromise(queueName, 3);
      await mysqlQueue.enqueue(queueName, [
        { name: "priority-1", payload: {}, priority: 1 },
        { name: "priority-2", payload: {}, priority: 2 },
        { name: "priority-3", payload: {}, priority: 3 },
      ]);
      await promise;

      expect(WorkerHandlerMock.handle.mock.calls.flatMap((c) => c[0].map((j: any) => j.name))).toEqual([
        "priority-3",
        "priority-2",
        "priority-1",
      ]);
    }, 10_000);

    it("tracker promise should resolved after status are committed", async () => {
      const WorkerHandlerMock = { handle: vitest.fn() };
      const queueName = "test_queue";
      await mysqlQueue.upsertQueue(queueName);
      worker = await mysqlQueue.work(queueName, WorkerHandlerMock.handle, 100);
      void worker.start();

      const promise = mysqlQueue.getJobExecutionPromise(queueName, 1);
      mysqlQueue.enqueue(queueName, { name: "1", payload: {} });
      await promise;

      const [rows] = await pool.query<RowDataPacket[]>(`SELECT * from ${mysqlQueue.jobsTable()}`);
      expect(rows[0]).toMatchObject({
        attempts: 1,
        completedAt: expect.any(Date),
        status: "completed",
      });
    });

    it("should call onJobFailed when job process fails (and not on every attempt)", async () => {
      const error = new Error("Unexpected");
      const OnJobFailedMock = vitest.fn();
      const WorkerHandlerMock = {
        handle: vitest.fn(() => {
          throw error;
        }),
      };
      const queueName = "test_queue";
      const maxRetries = 3;
      await mysqlQueue.upsertQueue(queueName, { maxRetries });
      worker = await mysqlQueue.work(queueName, WorkerHandlerMock.handle, 100, undefined, OnJobFailedMock);
      void worker.start();

      const promise = mysqlQueue.getJobExecutionPromise(queueName, maxRetries);
      const {
        jobIds: [jobId],
      } = await mysqlQueue.enqueue(queueName, { name: "1", payload: {} });
      await promise;

      expect(OnJobFailedMock).toHaveBeenCalledTimes(1);
      expect(OnJobFailedMock).toHaveBeenCalledWith(error, { id: jobId, queueName });
    });
  });

  describe("transactional job completion", () => {
    let worker: Awaited<ReturnType<typeof mysqlQueue.work>>;

    afterEach(async () => {
      if (worker) {
        await worker.stop();
      }
      await pool.query(`DROP TABLE IF EXISTS testTable`);
    });

    it("context.markJobsAsCompleted should throw if job is timed out", async () => {
      await pool.query(`CREATE TABLE testTable (id VARCHAR(36) PRIMARY KEY)`);
      const queueName = "test_queue";
      await mysqlQueue.upsertQueue(queueName, { maxDurationMs: 100, maxRetries: 1 });

      worker = await mysqlQueue.work(
        queueName,
        async function ([job]: Job[], signal: AbortSignal, ctx: CallbackContext) {
          const connection = await pool.getConnection();
          try {
            await connection.beginTransaction();
            const session = connectionToSession(connection);
            await session.execute(`INSERT INTO testTable (id) VALUES (?)`, [job.id]);
            await sleep(200); // needed for timeout
            await ctx.markJobsAsCompleted(session);
            await connection.commit();
          } catch (e) {
            await connection.rollback();
            throw e;
          } finally {
            connection.release();
          }
        },
        100,
      );
      void worker.start();

      const promise = mysqlQueue.getJobExecutionPromise(queueName, 1);
      const {
        jobIds: [jobId],
      } = await mysqlQueue.enqueue(queueName, { name: "test-job", payload: { test: true } });
      await promise;

      const job = await mysqlQueue.getJobById(jobId);
      expect(job).toMatchObject({
        attempts: 1,
        errors: [
          {
            at: expect.any(String),
            attempt: 1,
            error: expect.stringContaining("Job execution exceed the timeout of 100"),
          },
        ],
        status: "failed",
      });

      const [rows] = await pool.query<RowDataPacket[]>(`SELECT * FROM testTable`);
      expect(rows).length(0);
    }, 10_000);
  });
});

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function haveNoCommonElements(arr1: unknown[], arr2: unknown[]) {
  const set1 = new Set(arr1);
  return arr2.every((item) => !set1.has(item));
}

function enqueueNJobs(mysqlQueue: MysqlQueue, queueName: string, n: number) {
  return mysqlQueue.enqueue(
    queueName,
    Array.from({ length: n }).map((_, index) => ({ name: `job-${index}`, payload: {} })),
  );
}

function hasExactly(arr: any[], count: number, predicate: (item: any) => boolean) {
  return arr.filter(predicate).length === count;
}

function callsToTimeFromFirst(calls: Date[]) {
  if (calls.length === 0) return [];
  const first = calls[0].getTime();
  return calls.map((call) => call.getTime() - first);
}
