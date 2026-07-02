import { afterEach, beforeEach, describe, expect, it, vitest } from "vitest";
import { createPool, RowDataPacket } from "mysql2/promise";
import { Job, MysqlQueue } from "../src";
import { randomUUID } from "node:crypto";
import { sleep } from "../src/utils";

const DB_URI = "mysql://root:password@localhost:3306/serenis";

describe("sequenceKey", () => {
  const pool = createPool(DB_URI);
  let mysqlQueue: MysqlQueue;
  let worker: Awaited<ReturnType<typeof mysqlQueue.work>>;

  beforeEach(async () => {
    mysqlQueue = MysqlQueue({
      dbUri: DB_URI,
      loggingLevel: "fatal",
      tablesPrefix: `${randomUUID().slice(-4)}_`,
    });
    await mysqlQueue.globalInitialize();
  });

  afterEach(async () => {
    if (worker) await worker.stop();
    await mysqlQueue.globalDestroy();
    await mysqlQueue.dispose();
  });

  async function jobsByName() {
    const [rows] = await pool.query<RowDataPacket[]>(`SELECT name, status, seq FROM ${mysqlQueue.jobsTable()}`);
    return new Map(rows.map((r) => [r.name as string, r as { name: string; status: string; seq: number }]));
  }

  it("runs jobs sharing a sequenceKey in strict creation order, regardless of priority", async () => {
    const order: string[] = [];
    const handle = vitest.fn((jobs: Job[]) => {
      order.push(...jobs.map((j) => j.name));
    });
    const queueName = "test_queue";
    await mysqlQueue.upsertQueue(queueName);
    // Batch size > 1: without the gate, all three would be claimed at once and run in priority order.
    worker = await mysqlQueue.work(queueName, handle, { callbackBatchSize: 5, pollingBatchSize: 5, pollingIntervalMs: 50 });
    void worker.start();

    const promise = mysqlQueue.getJobExecutionPromise(queueName, 3);
    await mysqlQueue.enqueue(queueName, [
      { name: "seq-1", payload: {}, priority: 1, sequenceKey: "flow" },
      { name: "seq-2", payload: {}, priority: 2, sequenceKey: "flow" },
      { name: "seq-3", payload: {}, priority: 3, sequenceKey: "flow" },
    ]);
    await promise;

    // Same createdAt for all three and priority would reverse them; only the seq ordinal keeps creation order.
    expect(order).toEqual(["seq-1", "seq-2", "seq-3"]);
  }, 10_000);

  it("keeps strict order for jobs enqueued together in a single batch (identical createdAt)", async () => {
    const order: string[] = [];
    const handle = vitest.fn((jobs: Job[]) => {
      order.push(...jobs.map((j) => j.name));
    });
    const queueName = "test_queue";
    await mysqlQueue.upsertQueue(queueName);
    worker = await mysqlQueue.work(queueName, handle, { callbackBatchSize: 5, pollingBatchSize: 5, pollingIntervalMs: 50 });
    void worker.start();

    const promise = mysqlQueue.getJobExecutionPromise(queueName, 5);
    await mysqlQueue.enqueue(
      queueName,
      Array.from({ length: 5 }, (_, i) => ({ name: `job-${i}`, payload: {}, sequenceKey: "batch" })),
    );
    await promise;

    expect(order).toEqual(["job-0", "job-1", "job-2", "job-3", "job-4"]);
  }, 10_000);

  it("runs multiple independent sequences concurrently, each in its own creation order", async () => {
    const order: string[] = [];
    const handle = vitest.fn((jobs: Job[]) => {
      order.push(...jobs.map((j) => j.name));
    });
    const queueName = "test_queue";
    await mysqlQueue.upsertQueue(queueName);
    worker = await mysqlQueue.work(queueName, handle, { callbackBatchSize: 5, pollingBatchSize: 5, pollingIntervalMs: 50 });
    void worker.start();

    const promise = mysqlQueue.getJobExecutionPromise(queueName, 6);
    await mysqlQueue.enqueue(queueName, [
      { name: "a-1", payload: {}, sequenceKey: "A" },
      { name: "b-1", payload: {}, sequenceKey: "B" },
      { name: "a-2", payload: {}, sequenceKey: "A" },
      { name: "b-2", payload: {}, sequenceKey: "B" },
      { name: "a-3", payload: {}, sequenceKey: "A" },
      { name: "b-3", payload: {}, sequenceKey: "B" },
    ]);
    await promise;

    // Global order may interleave A and B, but each key must preserve its own creation order.
    expect(order.filter((n) => n.startsWith("a-"))).toEqual(["a-1", "a-2", "a-3"]);
    expect(order.filter((n) => n.startsWith("b-"))).toEqual(["b-1", "b-2", "b-3"]);
  }, 10_000);

  it("does not let a running sequence head block other keys or keyless jobs", async () => {
    const completed = new Set<string>();
    const handle = vitest.fn(async (jobs: Job[]) => {
      for (const j of jobs) {
        if (j.name === "a-1") await sleep(500); // stall the head of sequence A
        completed.add(j.name);
      }
    });
    const queueName = "test_queue";
    await mysqlQueue.upsertQueue(queueName, { maxDurationMs: 5000 });
    worker = await mysqlQueue.work(queueName, handle, { callbackBatchSize: 1, pollingBatchSize: 5, pollingIntervalMs: 50 });
    void worker.start();

    await mysqlQueue.enqueue(queueName, [
      { name: "a-1", payload: {}, sequenceKey: "A" },
      { name: "a-2", payload: {}, sequenceKey: "A" },
      { name: "b-1", payload: {}, sequenceKey: "B" },
      { name: "free", payload: {} },
    ]);

    // While a-1 is still stalling: B and the keyless job flow through, but a-2 stays blocked behind a-1.
    await sleep(250);
    expect(completed.has("b-1")).toBe(true);
    expect(completed.has("free")).toBe(true);
    expect(completed.has("a-2")).toBe(false);

    // Once a-1 finishes, its successor is released.
    await mysqlQueue.getJobExecutionPromise(queueName, 4);
    expect(completed.has("a-2")).toBe(true);
  }, 10_000);

  it("releases the successor only after the head completes, even across a retry", async () => {
    const order: string[] = [];
    const attempts = new Map<string, number>();
    const handle = vitest.fn((jobs: Job[]) => {
      for (const j of jobs) {
        order.push(j.name);
        const n = (attempts.get(j.name) ?? 0) + 1;
        attempts.set(j.name, n);
        if (j.name === "head" && n === 1) throw new Error("boom"); // fail once, then succeed on retry
      }
    });
    const queueName = "test_queue";
    await mysqlQueue.upsertQueue(queueName, { backoffMultiplier: 1, maxRetries: 3, minDelayMs: 100 });
    worker = await mysqlQueue.work(queueName, handle, { callbackBatchSize: 1, pollingBatchSize: 5, pollingIntervalMs: 50 });
    void worker.start();

    // head fails (tick 1), retries and succeeds (tick 2), then successor runs (tick 3).
    const promise = mysqlQueue.getJobExecutionPromise(queueName, 3);
    await mysqlQueue.enqueue(queueName, [
      { name: "head", payload: {}, sequenceKey: "flow" },
      { name: "successor", payload: {}, sequenceKey: "flow" },
    ]);
    await promise;

    // successor must appear exactly once, and only after head's final (successful) attempt.
    expect(order).toEqual(["head", "head", "successor"]);
    expect((await jobsByName()).get("successor")!.status).toBe("completed");
  }, 10_000);

  it("blocks the rest of a sequence when the head fails permanently", async () => {
    const executed: string[] = [];
    const failed: string[] = [];
    const handle = vitest.fn((jobs: Job[]) => {
      for (const j of jobs) {
        executed.push(j.name);
        if (j.name === "head") throw new Error("permanent failure");
      }
    });
    const queueName = "test_queue";
    // maxRetries: 1 -> head reaches terminal 'failed' after a single attempt.
    await mysqlQueue.upsertQueue(queueName, { maxRetries: 1 });
    worker = await mysqlQueue.work(queueName, handle, {
      callbackBatchSize: 1,
      onJobFailed: (_e, j) => {
        failed.push(j.name);
      },
      pollingBatchSize: 5,
      pollingIntervalMs: 50,
    });
    void worker.start();

    await mysqlQueue.enqueue(queueName, [
      { name: "head", payload: {}, sequenceKey: "flow" },
      { name: "successor", payload: {}, sequenceKey: "flow" },
      { name: "free", payload: {} }, // keyless control: proves the worker is alive and processing
    ]);

    // head processed (and fails) + free completed = 2 processing ticks.
    await mysqlQueue.getJobExecutionPromise(queueName, 2);
    // Give the worker several more polls to (incorrectly) pick up the successor, if the gate were broken.
    await sleep(300);

    expect(failed).toContain("head");
    expect(executed).toContain("free");
    expect(executed).not.toContain("successor"); // head-of-line blocking: strict semantics

    const jobs = await jobsByName();
    expect(jobs.get("head")!.status).toBe("failed");
    expect(jobs.get("successor")!.status).toBe("pending"); // still waiting on the failed head
    expect(jobs.get("free")!.status).toBe("completed");
  }, 10_000);
});
