import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { MysqlQueue, PeriodicJob } from "../src";
import { QueryDatabase } from "./utils/queryDatabase";
import { randomUUID } from "node:crypto";
import { RowDataPacket } from "mysql2";

const dbUri = "mysql://root:password@localhost:3306/serenis";

describe("periodic jobs", () => {
  const queryDatabase = QueryDatabase({ dbUri });
  const tablesPrefix = `${randomUUID().slice(-4)}_`;
  const instance = MysqlQueue({
    dbUri,
    leaderElectionHeartbeatMs: 100,
    loggingLevel: "fatal",
    tablesPrefix,
  });

  function getPeriodicJobsTable() {
    return `mysql_queue_${tablesPrefix}periodic_jobs`;
  }

  beforeAll(async () => {
    await instance.globalInitialize();
    await instance.upsertQueue("default");
  });

  afterAll(async () => {
    await instance.globalDestroy();
    await instance.dispose();
    await queryDatabase.dispose();
  });

  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(async () => {
    vi.clearAllTimers();
    vi.useRealTimers();

    const periodicJobs = instance.getPeriodicJobs();
    for (const job of periodicJobs) {
      await instance.removePeriodicJob(job.name);
    }

    await queryDatabase.query(`DELETE FROM ${instance.jobsTable()}`);
  });

  describe("registration", () => {
    it("should register a periodic job successfully", async () => {
      const periodicJobName = randomUUID();
      const job: PeriodicJob = {
        catchUpStrategy: "none",
        cronExpression: "*/5 * * * *",
        jobTemplate: {
          name: "test-job",
          payload: { data: "test" },
        },
        name: periodicJobName,
        targetQueue: "default",
      };

      await instance.registerPeriodicJob(job);

      const jobs = instance.getPeriodicJobs();
      expect(jobs).toHaveLength(1);
      expect(jobs[0]).toMatchObject({
        catchUpStrategy: "none",
        cronExpression: "*/5 * * * *",
        name: periodicJobName,
        targetQueue: "default",
      });

      const [dbState] = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${getPeriodicJobsTable()} WHERE name = ?`, [
        periodicJobName,
      ]);
      expect(dbState).toBeDefined();
      expect(dbState.name).toBe(periodicJobName);
      expect(dbState.nextRunAt).toBeInstanceOf(Date);
      expect(dbState.definition).toBeDefined();
    });

    it("should reject invalid cron expressions", async () => {
      const job: PeriodicJob = {
        catchUpStrategy: "none",
        cronExpression: "invalid cron",
        jobTemplate: {
          name: "test-job",
          payload: { data: "test" },
        },
        name: "test-periodic",
        targetQueue: "default",
      };

      await expect(instance.registerPeriodicJob(job)).rejects.toThrow();
    });

    it("should list all registered periodic jobs", async () => {
      const periodicJobName1 = randomUUID();
      const job1: PeriodicJob = {
        catchUpStrategy: "none",
        cronExpression: "*/5 * * * *",
        jobTemplate: { name: "job1", payload: {} },
        name: periodicJobName1,
        targetQueue: "queue1",
      };

      const periodicJobName2 = randomUUID();
      const job2: PeriodicJob = {
        catchUpStrategy: "all",
        cronExpression: "0 * * * *",
        jobTemplate: { name: "job2", payload: {} },
        maxCatchUp: 50,
        name: periodicJobName2,
        targetQueue: "queue2",
      };

      await instance.registerPeriodicJob(job1);
      await instance.registerPeriodicJob(job2);

      const jobs = instance.getPeriodicJobs();
      expect(jobs).toHaveLength(2);
      expect(jobs).toEqual(
        expect.arrayContaining([expect.objectContaining({ name: periodicJobName1 }), expect.objectContaining({ name: periodicJobName2 })]),
      );
    });

    it("should replace existing job when re-registering with same name", async () => {
      const periodicJobName = randomUUID();
      const job1: PeriodicJob = {
        catchUpStrategy: "none",
        cronExpression: "*/5 * * * *",
        jobTemplate: { name: "job1", payload: { version: 1 } },
        name: periodicJobName,
        targetQueue: "default",
      };

      await instance.registerPeriodicJob(job1);

      const job2: PeriodicJob = {
        catchUpStrategy: "latest",
        cronExpression: "*/10 * * * *",
        jobTemplate: { name: "job1", payload: { version: 2 } },
        name: periodicJobName,
        targetQueue: "default",
      };

      await instance.registerPeriodicJob(job2);

      const jobs = instance.getPeriodicJobs();
      expect(jobs).toHaveLength(1);
      expect(jobs[0].cronExpression).toBe("*/10 * * * *");
      expect(jobs[0].catchUpStrategy).toBe("latest");
    });
  });

  describe("removal", () => {
    it("should remove a periodic job", async () => {
      const periodicJobName = randomUUID();
      const job: PeriodicJob = {
        catchUpStrategy: "none",
        cronExpression: "*/5 * * * *",
        jobTemplate: { name: "test-job", payload: {} },
        name: periodicJobName,
        targetQueue: "default",
      };

      await instance.registerPeriodicJob(job);
      const removed = await instance.removePeriodicJob(periodicJobName);

      expect(removed).toBe(true);
      const jobs = instance.getPeriodicJobs();
      expect(jobs).toHaveLength(0);
    });

    it("should return false when removing non-existent job", async () => {
      const removed = await instance.removePeriodicJob("non-existent");
      expect(removed).toBe(false);
    });
  });

  describe("includeScheduledTime feature", () => {
    it("should include scheduled time in payload when enabled", async () => {
      vi.setSystemTime(new Date("2024-01-01T00:00:00.000Z"));
      const periodicJobName = randomUUID();
      const job: PeriodicJob = {
        catchUpStrategy: "none",
        cronExpression: "*/5 * * * *",
        includeScheduledTime: true,
        jobTemplate: {
          name: "test-job",
          payload: { originalData: "test" },
        },
        name: periodicJobName,
        targetQueue: "default",
      };

      await instance.registerPeriodicJob(job);

      await vi.advanceTimersByTimeAsync(100);
      await vi.advanceTimersByTimeAsync(5 * 60 * 1000);
      await instance.__internal.waitForPendingPeriodicExecutions();

      const jobs = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${instance.jobsTable()}`);
      expect(jobs[0].payload).toHaveProperty("_periodic");
      expect(jobs[0].payload._periodic).toHaveProperty("scheduledTime");
      expect(jobs[0].payload.originalData).toBe("test");
    });

    it("should not include scheduled time when disabled", async () => {
      const job: PeriodicJob = {
        catchUpStrategy: "none",
        cronExpression: "*/5 * * * *",
        includeScheduledTime: false,
        jobTemplate: {
          name: "test-job",
          payload: { originalData: "test" },
        },
        name: "test-periodic-with-time-2",
        targetQueue: "default",
      };

      await instance.registerPeriodicJob(job);

      await vi.advanceTimersByTimeAsync(100);
      await vi.advanceTimersByTimeAsync(5 * 60 * 1000);
      await instance.__internal.waitForPendingPeriodicExecutions();

      const jobs = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${instance.jobsTable()}`);
      expect(jobs[0].payload).not.toHaveProperty("_periodic");
      expect(jobs[0].payload.originalData).toBe("test");
    });
  });

  describe("catch-up strategies", () => {
    describe("latest strategy", () => {
      it("should enqueue only the latest missed run", async () => {
        vi.setSystemTime(new Date("2024-01-01T00:00:00.000Z"));

        const job: PeriodicJob = {
          catchUpStrategy: "latest",
          cronExpression: "*/5 * * * *",
          jobTemplate: {
            name: randomUUID(),
            payload: { data: "test" },
          },
          name: randomUUID(),
          targetQueue: "default",
        };

        // Manually insert initial state to simulate a job that was previously run
        await queryDatabase.query(`INSERT INTO ${getPeriodicJobsTable()} (name, lastRunAt, nextRunAt, definition) VALUES (?, ?, ?, ?)`, [
          job.name,
          new Date("2024-01-01T00:00:00.000Z"),
          new Date("2024-01-01T00:05:00.000Z"),
          JSON.stringify({}),
        ]);

        // Simulate a restart 15 minutes later
        vi.setSystemTime(new Date("2024-01-01T00:15:00.000Z"));
        await instance.registerPeriodicJob(job);
        await instance.__internal.waitForPendingPeriodicExecutions();

        const jobs = await queryDatabase.query<RowDataPacket[]>(
          `SELECT * FROM ${instance.jobsTable()} WHERE name = ? ORDER BY idempotentKey`,
          [job.jobTemplate.name],
        );
        expect(jobs).toHaveLength(1);
      });
    });

    describe("all strategy", () => {
      it("should enqueue all missed runs up to maxCatchUp limit", async () => {
        vi.setSystemTime(new Date("2024-01-01T00:00:00.000Z"));

        const job: PeriodicJob = {
          catchUpStrategy: "all",
          cronExpression: "*/5 * * * *",
          jobTemplate: {
            name: "test-job",
            payload: {},
          },
          name: randomUUID(),
          targetQueue: "default",
        };
        // Manually insert initial state to simulate a job that was previously run
        await queryDatabase.query(`INSERT INTO ${getPeriodicJobsTable()} (name, lastRunAt, nextRunAt, definition) VALUES (?, ?, ?, ?)`, [
          job.name,
          new Date("2024-01-01T00:00:00.000Z"),
          new Date("2024-01-01T00:05:00.000Z"),
          JSON.stringify({}),
        ]);

        // Now simulate a restart 30 minutes later
        vi.setSystemTime(new Date("2024-01-01T00:30:00.000Z"));
        await instance.registerPeriodicJob(job);

        const jobs = await queryDatabase.query<RowDataPacket[]>(
          `SELECT * FROM ${instance.jobsTable()} WHERE name = ? ORDER BY idempotentKey`,
          ["test-job"],
        );
        // Should enqueue all 6 missed runs (00:05, 00:10, 00:15, 00:20, 00:25, 00:30)
        expect(jobs).toHaveLength(6);

        // Verify idempotent keys are unique and in order
        const keys = jobs.map((j) => j.idempotentKey);
        expect(new Set(keys).size).toBe(6);
        expect(keys).toEqual([
          expect.stringContaining("2024-01-01T00:05:00"),
          expect.stringContaining("2024-01-01T00:10:00"),
          expect.stringContaining("2024-01-01T00:15:00"),
          expect.stringContaining("2024-01-01T00:20:00"),
          expect.stringContaining("2024-01-01T00:25:00"),
          expect.stringContaining("2024-01-01T00:30:00"),
        ]);
      });

      it("should respect maxCatchUp limit", async () => {
        vi.setSystemTime(new Date("2024-01-01T00:00:00.000Z"));

        const job: PeriodicJob = {
          catchUpStrategy: "all",
          cronExpression: "*/5 * * * *",
          jobTemplate: {
            name: "test-job",
            payload: {},
          },
          maxCatchUp: 3,
          name: randomUUID(),
          targetQueue: "default",
        };
        // Manually insert initial state to simulate a job that was previously run
        await queryDatabase.query(`INSERT INTO ${getPeriodicJobsTable()} (name, lastRunAt, nextRunAt, definition) VALUES (?, ?, ?, ?)`, [
          job.name,
          new Date("2024-01-01T00:00:00.000Z"),
          new Date("2024-01-01T00:05:00.000Z"),
          JSON.stringify({}),
        ]);

        // Now simulate a restart 30 minutes later
        vi.setSystemTime(new Date("2024-01-01T00:30:00.000Z"));
        await instance.registerPeriodicJob(job);

        const jobs = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${instance.jobsTable()} WHERE name = ?`, ["test-job"]);
        // Should only enqueue 3 jobs (respecting maxCatchUp)
        expect(jobs).toHaveLength(3);
      });
    });

    describe("none strategy", () => {
      it("should not enqueue missed runs on registration", async () => {
        vi.setSystemTime(new Date("2024-01-01T00:00:00.000Z"));

        const job: PeriodicJob = {
          catchUpStrategy: "none",
          cronExpression: "*/5 * * * *",
          jobTemplate: {
            name: "test-job",
            payload: {},
          },
          name: randomUUID(),
          targetQueue: "default",
        };
        await queryDatabase.query(`INSERT INTO ${getPeriodicJobsTable()} (name, lastRunAt, nextRunAt, definition) VALUES (?, ?, ?, ?)`, [
          job.name,
          new Date("2024-01-01T00:00:00.000Z"),
          new Date("2024-01-01T00:05:00.000Z"),
          JSON.stringify({}),
        ]);

        // Now simulate a restart 30 minutes later
        vi.setSystemTime(new Date("2024-01-01T00:30:00.000Z"));
        await instance.registerPeriodicJob(job);

        const jobs = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${instance.jobsTable()} WHERE name = ?`, ["test-job"]);
        expect(jobs).toHaveLength(0);
      });
    });
  });

  describe("idempotent key generation", () => {
    it("should generate idempotent keys in correct format", async () => {
      vi.setSystemTime(new Date("2024-01-01T00:00:00.000Z"));

      const job: PeriodicJob = {
        catchUpStrategy: "all",
        cronExpression: "*/5 * * * *",
        jobTemplate: {
          name: "test-job",
          payload: {},
        },
        maxCatchUp: 2,
        name: "test-periodic",
        targetQueue: "default",
      };
      // Manually insert initial state to simulate a job that was previously run
      await queryDatabase.query(`INSERT INTO ${getPeriodicJobsTable()} (name, lastRunAt, nextRunAt, definition) VALUES (?, ?, ?, ?)`, [
        job.name,
        new Date("2024-01-01T00:00:00.000Z"),
        new Date("2024-01-01T00:05:00.000Z"),
        JSON.stringify({}),
      ]);

      // Now simulate a restart 10 minutes later
      vi.setSystemTime(new Date("2024-01-01T00:10:00.000Z"));

      await instance.registerPeriodicJob(job);

      const jobs = await queryDatabase.query<RowDataPacket[]>(
        `SELECT * FROM ${instance.jobsTable()} WHERE name = ? ORDER BY idempotentKey`,
        ["test-job"],
      );
      expect(jobs).toHaveLength(2);

      jobs.forEach((job) => {
        expect(job.idempotentKey).toMatch(/^periodic:test-periodic:\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
      });
    });

    it("should prevent duplicate jobs with same idempotent key", async () => {
      const job: PeriodicJob = {
        catchUpStrategy: "all",
        cronExpression: "*/5 * * * *",
        jobTemplate: {
          name: "test-job",
          payload: {},
        },
        maxCatchUp: 10,
        name: "test-periodic",
        targetQueue: "default",
      };

      vi.setSystemTime(new Date("2024-01-01T00:00:00.000Z"));
      await instance.registerPeriodicJob(job);

      await queryDatabase.query(`UPDATE ${getPeriodicJobsTable()} SET lastRunAt = ? WHERE name = ?`, [
        new Date("2024-01-01T00:00:00.000Z"),
        "test-periodic",
      ]);

      vi.setSystemTime(new Date("2024-01-01T00:10:00.000Z"));
      await instance.registerPeriodicJob(job);

      const firstRunJobs = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${instance.jobsTable()} WHERE name = ?`, ["test-job"]);
      const firstJobCount = firstRunJobs.length;

      // Try to register again at the same time (simulating a restart)
      // Should not create duplicates due to idempotent keys
      await instance.registerPeriodicJob(job);

      const secondRunJobs = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${instance.jobsTable()} WHERE name = ?`, [
        "test-job",
      ]);

      // Should have same number of jobs (idempotent key prevents duplicates)
      expect(secondRunJobs.length).toBe(firstJobCount);
    });
  });

  it("should update database nextRunAt to the next scheduled time after execution", async () => {
    vi.setSystemTime(new Date("2024-01-01T00:00:00.000Z"));

    const periodicJobName = randomUUID();
    const job: PeriodicJob = {
      catchUpStrategy: "none",
      cronExpression: "*/5 * * * *",
      jobTemplate: {
        name: "test-job",
        payload: { data: "test" },
      },
      name: periodicJobName,
      targetQueue: "default",
    };

    await instance.registerPeriodicJob(job);
    const [initialState] = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${getPeriodicJobsTable()} WHERE name = ?`, [
      periodicJobName,
    ]);
    expect(initialState.lastRunAt).toBeNull();
    expect(new Date(initialState.nextRunAt).toISOString()).toBe("2024-01-01T00:05:00.000Z");

    await vi.advanceTimersByTimeAsync(100);
    await vi.advanceTimersByTimeAsync(5 * 60 * 1000);
    await instance.__internal.waitForPendingPeriodicExecutions();

    const [stateAfterExecution] = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${getPeriodicJobsTable()} WHERE name = ?`, [
      periodicJobName,
    ]);

    expect(new Date(stateAfterExecution.lastRunAt).toISOString()).toBe("2024-01-01T00:05:00.000Z");
    expect(new Date(stateAfterExecution.nextRunAt).toISOString()).toBe("2024-01-01T00:10:00.000Z");
  });
});
