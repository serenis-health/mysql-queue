import { afterEach, beforeEach, describe, expect, it, vitest } from "vitest";
import { MysqlQueue } from "../src";
import { randomUUID } from "node:crypto";

describe("Performance", () => {
  const WORKER_COUNT = 10;
  const JOB_COUNT = 1000;

  const WorkerMocks = {
    handle: vitest.fn<(j: unknown) => void>(),
  };
  const mysqlQueue = MysqlQueue({
    dbUri: "mysql://root:password@localhost:3306/serenis",
    loggingLevel: "fatal",
    tablesPrefix: `${randomUUID().slice(-4)}_`,
  });

  afterEach(async () => {
    await mysqlQueue.globalDestroy();
    await mysqlQueue.dispose();
  });

  beforeEach(async () => {
    await mysqlQueue.globalInitialize();
  });

  it("should handle 1000 jobs in less than 5 seconds (with 10 workers)", async () => {
    const queue = "performance";
    await mysqlQueue.upsertQueue(queue);

    const workers = await Promise.all(
      Array.from({ length: WORKER_COUNT }).map((_) => {
        return mysqlQueue.work(queue, (j) => WorkerMocks.handle(j), { callbackBatchSize: 50, pollingBatchSize: 50 });
      }),
    );

    void Promise.all(workers.map((w) => w.start()));

    const jobs = Array.from({ length: JOB_COUNT }).map((_, i) => ({
      name: "perf-job",
      payload: { i },
    }));

    const start = performance.now();
    const promise = mysqlQueue.getJobExecutionPromise(queue, JOB_COUNT);
    await mysqlQueue.enqueue(queue, jobs);
    await promise;

    const end = performance.now();
    expect(end - start).toBeLessThan(5000);
    await Promise.all(workers.map((w) => w.stop()));
  }, 10000);
});
