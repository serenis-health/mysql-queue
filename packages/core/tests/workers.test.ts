import { afterEach, beforeEach, describe, expect, it, vitest } from "vitest";
import { createPool, RowDataPacket } from "mysql2/promise";
import { MysqlQueue } from "../src";
import { randomUUID } from "node:crypto";

const DB_URI = "mysql://root:password@localhost:3306/serenis";

describe("workers", () => {
  const pool = createPool(DB_URI);
  const queueName = "test_queue";

  let mysqlQueue: MysqlQueue;

  beforeEach(async () => {
    mysqlQueue = MysqlQueue({
      dbUri: DB_URI,
      tablesPrefix: `${randomUUID().slice(-4)}_`,
    });
    await mysqlQueue.initialize();
    await mysqlQueue.upsertQueue(queueName, { maxDurationMs: 10_000 });
  });

  afterEach(async () => {
    await mysqlQueue.destroy();
    await mysqlQueue.dispose();
  });

  describe("two worker with different handler latency", () => {
    const Worker1HandlerMock = { handle: vitest.fn().mockImplementation(async () => await sleep(3000)) };
    const Worker2HandlerMock = { handle: vitest.fn() };
    let worker1: Awaited<ReturnType<typeof mysqlQueue.work>>;
    let worker2: Awaited<ReturnType<typeof mysqlQueue.work>>;

    beforeEach(async () => {
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

      const w1JobIds = Worker1HandlerMock.handle.mock.calls.map((c) => c[0].id);
      const w2JobIds = Worker2HandlerMock.handle.mock.calls.map((c) => c[0].id);
      expect(haveNoCommonElements(w1JobIds, w2JobIds)).toBeTruthy();
      expect(Worker1HandlerMock.handle).toHaveBeenCalledTimes(5);
      expect(Worker2HandlerMock.handle).toHaveBeenCalledTimes(5);
    });

    it("should ensure that a slow worker does not block or delay other workers, case jobs already on queue", async () => {
      await enqueueNJobs(mysqlQueue, queueName, 10);
      void Promise.all([worker1.start(), worker2.start()]);
      const promise = mysqlQueue.getJobExecutionPromise(queueName, 10);

      await promise;

      await sleep(500);
      const [rows] = await pool.query<RowDataPacket[]>(`SELECT id, createdAt, completedAt from ${mysqlQueue.jobsTable()}`);
      const jobs = rows.map((j) => ({ ...j, durationMs: new Date(j.completedAt).getTime() - new Date(j.createdAt).getTime() }));
      expect(hasExactly(jobs, 5, (item) => item.durationMs > 3000)).toBeTruthy();
      expect(hasExactly(jobs, 5, (item) => item.durationMs < 100)).toBeTruthy();
    });

    it("should ensure that a slow worker does not block or delay other workers, case no jobs on queue", async () => {
      void Promise.all([worker1.start(), worker2.start()]);
      await enqueueNJobs(mysqlQueue, queueName, 10);
      const promise = mysqlQueue.getJobExecutionPromise(queueName, 10);

      await promise;

      await sleep(500);
      const [rows] = await pool.query<RowDataPacket[]>(`SELECT id, createdAt, completedAt from ${mysqlQueue.jobsTable()}`);
      const jobs = rows.map((j) => ({ ...j, durationMs: new Date(j.completedAt).getTime() - new Date(j.createdAt).getTime() }));
      expect(hasExactly(jobs, 5, (item) => item.durationMs > 3000)).toBeTruthy();
      expect(hasExactly(jobs, 5, (item) => item.durationMs < 1000)).toBeTruthy();
    });
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
    Array.from({ length: n }).map((_, index) => ({ name: `job-${index}`, payload: {}, startAfter: new Date(Date.now() - 10_000) })),
  );
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function hasExactly(arr: any[], count: number, predicate: (item: any) => boolean) {
  let matchCount = 0;
  for (const item of arr) {
    if (predicate(item)) {
      matchCount++;
      if (matchCount > count) return false;
    }
  }
  return matchCount === count;
}
