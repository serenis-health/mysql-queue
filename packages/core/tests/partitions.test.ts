import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { cancellablePromiseFactory } from "../src/cancellablePromise";
import { MysqlQueue } from "../src";
import { QueryDatabase } from "./utils/queryDatabase";
import { sleep } from "../src/utils";

const dbUri = "mysql://root:password@localhost:3306/serenis";

describe("partition functionality", () => {
  let instance1: ReturnType<typeof MysqlQueue>;
  let partitionedInstance1: ReturnType<typeof MysqlQueue>;
  let partitionedInstance2: ReturnType<typeof MysqlQueue>;

  const partitionKey1 = "1";
  const partitionKey2 = "2";
  const queueName = "test-queue";

  beforeAll(async () => {
    instance1 = MysqlQueue({
      dbUri,
      loggingLevel: "fatal",
    });
    partitionedInstance1 = MysqlQueue({
      dbUri,
      loggingLevel: "fatal",
      partitionKey: partitionKey1,
    });
    partitionedInstance2 = MysqlQueue({
      dbUri,
      loggingLevel: "fatal",
      partitionKey: partitionKey2,
    });

    await instance1.globalInitialize();
    await partitionedInstance1.globalInitialize();
    await partitionedInstance2.globalInitialize();
  });

  afterAll(async () => {
    await instance1.dispose();
    await partitionedInstance1.dispose();
    await partitionedInstance2.dispose();
  });

  describe("upsertQueue", () => {
    it("should create queues with different partitionKeys", async () => {
      const queue1 = await partitionedInstance1.upsertQueue(queueName);
      const queue2 = await partitionedInstance2.upsertQueue(queueName);
      const queue3 = await instance1.upsertQueue(queueName);

      expect(queue1.name).toBe(queueName);
      expect(queue2.name).toBe(queueName);
      expect(queue3.name).toBe(queueName);
      expect(queue1.partitionKey).toBe(partitionKey1);
      expect(queue2.partitionKey).toBe(partitionKey2);
      expect(queue3.partitionKey).toBe("default");
      expect(queue1.id).not.toBe(queue2.id);
    });
  });

  describe("enqueue", () => {
    const queueName = "queue";

    afterEach(async () => {
      await partitionedInstance1.purge();
      await partitionedInstance2.purge();
      await instance1.purge();
    });

    it("should automatically use factory partitionKey for enqueue", async () => {
      await partitionedInstance1.upsertQueue(queueName);
      await partitionedInstance2.upsertQueue(queueName);
      await instance1.upsertQueue(queueName);

      await partitionedInstance1.enqueue(queueName, { name: "foo", payload: { foo: "bar" } });
      await partitionedInstance2.enqueue(queueName, { name: "foo", payload: { foo: "bar" } });
      await instance1.enqueue(queueName, { name: "foo", payload: { foo: "bar" } });

      expect(await partitionedInstance1.countJobs(queueName)).toBe(1);
      expect(await partitionedInstance2.countJobs(queueName)).toBe(1);
      expect(await instance1.countJobs(queueName)).toBe(1);
    });
  });

  describe("work", () => {
    afterEach(async () => {
      await partitionedInstance1.purge();
      await partitionedInstance2.purge();
      await instance1.purge();
    });

    it("should consume the right queue for partition", async () => {
      const handlersMock = { instance1: vi.fn(), partitionedInstance1: vi.fn(), partitionedInstance2: vi.fn() };

      await partitionedInstance1.upsertQueue(queueName);
      await partitionedInstance2.upsertQueue(queueName);
      await instance1.upsertQueue(queueName);
      const partitionedInstance1Worker = await partitionedInstance1.work(queueName, (j) => handlersMock.partitionedInstance1(j));
      const partitionedInstance2Worker = await partitionedInstance2.work(queueName, (j) => handlersMock.partitionedInstance2(j));
      const instance1Worker = await instance1.work(queueName, (j) => handlersMock.instance1(j));

      void partitionedInstance1Worker.start();
      void partitionedInstance2Worker.start();
      void instance1Worker.start();

      const promise1 = partitionedInstance1.getJobExecutionPromise(queueName, 1);
      const promise2 = partitionedInstance2.getJobExecutionPromise(queueName, 1);
      const promise3 = instance1.getJobExecutionPromise(queueName, 1);

      await Promise.all([
        partitionedInstance1.enqueue(queueName, { name: "jp1", payload: { foo: "bar" } }),
        partitionedInstance2.enqueue(queueName, { name: "jp2", payload: { foo: "bar" } }),
        instance1.enqueue(queueName, { name: "j1", payload: { foo: "bar" } }),
      ]);

      await Promise.allSettled([promise1, promise2, promise3]);

      await Promise.all([partitionedInstance1Worker.stop(), partitionedInstance2Worker.stop(), instance1Worker.stop()]);

      expect(handlersMock.instance1).toBeCalledTimes(1);
      expect(handlersMock.partitionedInstance1).toBeCalledTimes(1);
      expect(handlersMock.partitionedInstance2).toBeCalledTimes(1);
    });
  });

  describe("purgePartition", () => {
    const queryDatabase = QueryDatabase({ dbUri });

    it("should remove all queue and jobs in the partition", async () => {
      await partitionedInstance1.upsertQueue(queueName);
      await partitionedInstance2.upsertQueue(queueName);

      await partitionedInstance1.enqueue(queueName, { name: "jp1", payload: { foo: "bar" } });
      await partitionedInstance2.enqueue(queueName, { name: "jp1", payload: { foo: "bar" } });

      const partitionedInstance1Worker = await partitionedInstance1.work(queueName, async (job, signal) => {
        await cancellablePromiseFactory(() => sleep(5000), signal).promise;
      });

      void partitionedInstance1Worker.start();

      await sleep(500);

      await partitionedInstance1.purge();

      const queues = (await queryDatabase.query(`SELECT * FROM ${partitionedInstance1.queuesTable()}`)) as {
        partitionKey: string;
        id: string;
      }[];
      const jobs = (await queryDatabase.query(`SELECT * FROM ${partitionedInstance1.jobsTable()}`)) as { queueId: string }[];
      expect(queues.length).toBe(1);
      expect(queues[0]).toMatchObject({ partitionKey: partitionKey2 });
      expect(jobs.length).toBe(1);
      expect(jobs[0].queueId).toBe(queues[0].id);
    });
  });
});
