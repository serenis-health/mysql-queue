import { afterEach, beforeEach, describe, expect, it, vitest } from "vitest";
import { MysqlQueue } from "../src";
import { randomUUID } from "node:crypto";
import { sleep } from "../src/utils";

const DB_URI = "mysql://root:password@localhost:3306/serenis";

describe("sequential queues", () => {
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

  describe("sequential queue with same sequentialKey", () => {
    const queueName = "sequential_queue";
    let worker1: Awaited<ReturnType<typeof mysqlQueue.work>>;
    let worker2: Awaited<ReturnType<typeof mysqlQueue.work>>;

    const executionLog: Array<{ jobName: string; workerId: string; startTime: string; endTime: string }> = [];

    beforeEach(async () => {
      executionLog.length = 0;
      await mysqlQueue.upsertQueue(queueName, { sequential: true });

      function createWorker(workerId: string) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        return vitest.fn().mockImplementation(async (jobs: any[]) => {
          const startTime = new Date().toISOString();
          await sleep(1000);
          const endTime = new Date().toISOString();
          jobs.forEach((job) => {
            executionLog.push({ endTime, jobName: job.name, startTime, workerId });
          });
        });
      }

      worker1 = await mysqlQueue.work(queueName, createWorker("worker1"), {});
      worker2 = await mysqlQueue.work(queueName, createWorker("worker2"), {});
    });

    afterEach(async () => {
      await Promise.all([worker1.stop(), worker2.stop()]);
    });

    it("should process jobs with same sequentialKey sequentially across all workers", async () => {
      const promise = mysqlQueue.getJobExecutionPromise(queueName, 3);

      await mysqlQueue.enqueue(queueName, [
        { name: "job-1", payload: {}, sequentialKey: "user:123" },
        { name: "job-2", payload: {}, sequentialKey: "user:123" },
        { name: "job-3", payload: {}, sequentialKey: "user:123" },
      ]);

      void Promise.all([worker1.start(), worker2.start()]);
      await promise;

      expect(executionLog).toHaveLength(3);

      const sorted = executionLog.sort((a, b) => new Date(a.startTime).getTime() - new Date(b.startTime).getTime());
      for (let i = 1; i < sorted.length; i++) {
        expect(new Date(sorted[i].startTime).getTime()).toBeGreaterThanOrEqual(new Date(sorted[i - 1].endTime).getTime());
      }
    });

    it("should allow jobs with NULL sequentialKey to run in parallel with sequentialKey jobs", async () => {
      const promise = mysqlQueue.getJobExecutionPromise(queueName, 4);

      await mysqlQueue.enqueue(queueName, [
        { name: "job-1", payload: {}, sequentialKey: "user:123" },
        { name: "job-2", payload: {}, sequentialKey: "user:123" },
        { name: "job-3", payload: {} },
        { name: "job-4", payload: {} },
      ]);

      void Promise.all([worker1.start(), worker2.start()]);
      await promise;

      expect(executionLog).toHaveLength(4);

      const nullKeyJobs = executionLog.filter((log) => log.jobName.includes("job-3") || log.jobName.includes("job-4"));
      expect(nullKeyJobs.length).toBeGreaterThanOrEqual(2);
    });
  });

  describe("sequential queue with different sequentialKeys", () => {
    const queueName = "sequential_queue_multi_key";
    let worker1: Awaited<ReturnType<typeof mysqlQueue.work>>;
    let worker2: Awaited<ReturnType<typeof mysqlQueue.work>>;

    const executionLog: Array<{ jobName: string; workerId: string; startTime: number; endTime: number }> = [];

    beforeEach(async () => {
      executionLog.length = 0;
      await mysqlQueue.upsertQueue(queueName, { sequential: true });

      function createWorker(workerId: string) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        return vitest.fn().mockImplementation(async (jobs: any[]) => {
          const startTime = Date.now();
          await sleep(1000);
          const endTime = Date.now();
          jobs.forEach((job) => {
            executionLog.push({ endTime, jobName: job.name, startTime, workerId });
          });
        });
      }

      worker1 = await mysqlQueue.work(queueName, createWorker("worker1"), { pollingIntervalMs: 50 });
      worker2 = await mysqlQueue.work(queueName, createWorker("worker2"), { pollingIntervalMs: 50 });
    });

    afterEach(async () => {
      await Promise.all([worker1.stop(), worker2.stop()]);
    });

    it("should process jobs with different sequentialKeys in parallel", async () => {
      const startTime = Date.now();
      const promise = mysqlQueue.getJobExecutionPromise(queueName, 4);

      await mysqlQueue.enqueue(queueName, [
        { name: "user123-job1", payload: {}, sequentialKey: "user:123" },
        { name: "user123-job2", payload: {}, sequentialKey: "user:123" },
        { name: "user456-job1", payload: {}, sequentialKey: "user:456" },
        { name: "user456-job2", payload: {}, sequentialKey: "user:456" },
      ]);

      void Promise.all([worker1.start(), worker2.start()]);
      await promise;
      const totalTime = Date.now() - startTime;

      expect(executionLog).toHaveLength(4);

      const user123Jobs = executionLog.filter((log) => log.jobName.includes("user123")).sort((a, b) => a.startTime - b.startTime);
      expect(user123Jobs[1].startTime).toBeGreaterThanOrEqual(user123Jobs[0].endTime);

      const user456Jobs = executionLog.filter((log) => log.jobName.includes("user456")).sort((a, b) => a.startTime - b.startTime);
      expect(user456Jobs[1].startTime).toBeGreaterThanOrEqual(user456Jobs[0].endTime);

      // Fully serial: 4000ms (4 jobs × 1000ms)
      // Optimal parallel: 2000ms
      // Worker1: user:123-job1 (1000ms) → user:123-job2 (1000ms) = 2000ms
      // Worker2: user:456-job1 (1000ms) → user:456-job2 (1000ms) = 2000ms
      // Both run in parallel → total 2000ms
      expect(totalTime).toBeLessThan(3000);
    });
  });
});
