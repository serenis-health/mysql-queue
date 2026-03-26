import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createCronScheduler, createScheduler } from "./scheduler";
import { Logger } from "./logger";

describe("scheduler", () => {
  const loggerMock = { debug: vi.fn(), error: vi.fn(), trace: vi.fn() };
  const fakeTask = vi.fn();

  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.clearAllTimers();
    vi.useRealTimers();
    vi.resetAllMocks();
  });

  it("should schedule and run the task periodically", async () => {
    const scheduler = createScheduler(fakeTask, loggerMock as unknown as Logger, {
      intervalMs: 1000,
      runOnStart: false,
      taskName: "testTask",
    });
    scheduler.start();
    expect(fakeTask).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(1000); // Advance time to trigger the first scheduled run

    expect(fakeTask).toHaveBeenCalledTimes(1);
    expect(loggerMock.trace).toHaveBeenCalledWith(expect.objectContaining({ taskName: "testTask" }), "scheduler.runStarted");

    await vi.advanceTimersByTimeAsync(1000); // Advance another interval to ensure it runs again
    expect(fakeTask).toHaveBeenCalledTimes(2);

    scheduler.stop();
  });

  it("should run immediately when runOnStart = true", async () => {
    const scheduler = createScheduler(fakeTask, loggerMock as unknown as Logger, {
      intervalMs: 1000,
      runOnStart: true,
      taskName: "testTask",
    });

    scheduler.start();
    await Promise.resolve(); // Allow microtasks to resolve the first immediate run

    expect(fakeTask).toHaveBeenCalledTimes(1);
    scheduler.stop();
  });

  it("should stop scheduling after stop() is called", async () => {
    const scheduler = createScheduler(fakeTask, loggerMock as unknown as Logger, {
      intervalMs: 1000,
      runOnStart: false,
      taskName: "testTask",
    });

    scheduler.start();
    scheduler.stop();

    await vi.advanceTimersByTimeAsync(5000);

    expect(fakeTask).not.toHaveBeenCalled();
  });

  it("should log errors when task throws", async () => {
    const error = new Error("boom");
    const fakeFailingTask = vi.fn().mockRejectedValue(error);

    const scheduler = createScheduler(fakeFailingTask, loggerMock as unknown as Logger, {
      intervalMs: 1000,
      runOnStart: false,
      taskName: "testTask",
    });
    scheduler.start();

    await vi.advanceTimersByTimeAsync(1000);

    expect(loggerMock.error).toHaveBeenCalledWith(expect.objectContaining({ message: "boom", taskName: "testTask" }), "scheduler.runError");

    scheduler.stop();
  });

  it("should skip interval ticks when task takes longer than interval", async () => {
    let resolveTask: () => void;
    const longRunningTask = vi.fn().mockImplementation(() => {
      return new Promise<void>((resolve) => {
        resolveTask = resolve;
      });
    });

    const scheduler = createScheduler(longRunningTask, loggerMock as unknown as Logger, {
      intervalMs: 1000,
      runOnStart: false,
      taskName: "testTask",
    });

    scheduler.start();

    // First interval triggers
    await vi.advanceTimersByTimeAsync(1000);
    expect(longRunningTask).toHaveBeenCalledTimes(1);

    // Second interval happens while task is still running - should be skipped
    await vi.advanceTimersByTimeAsync(1000);
    expect(longRunningTask).toHaveBeenCalledTimes(1); // Still 1, not 2
    expect(loggerMock.trace).toHaveBeenCalledWith(expect.objectContaining({ taskName: "testTask" }), "scheduler.skipRunTaskInProgress");

    // Third interval also skipped
    await vi.advanceTimersByTimeAsync(1000);
    expect(longRunningTask).toHaveBeenCalledTimes(1);

    // Now complete the long-running task
    resolveTask!();
    await Promise.resolve();

    // Next interval should run normally
    await vi.advanceTimersByTimeAsync(1000);
    expect(longRunningTask).toHaveBeenCalledTimes(2);

    scheduler.stop();
  });
});

describe("createCronScheduler", () => {
  const loggerMock = { debug: vi.fn(), error: vi.fn(), trace: vi.fn() };
  const fakeTask = vi.fn();

  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2024-01-01T00:00:00Z"));
  });

  afterEach(() => {
    vi.clearAllTimers();
    vi.useRealTimers();
    vi.resetAllMocks();
  });

  it("should schedule task at the next cron tick", async () => {
    const scheduler = createCronScheduler(fakeTask, loggerMock as unknown as Logger, {
      cronExpression: "* * * * *",
      taskName: "testTask",
    });
    scheduler.start();
    expect(fakeTask).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(59_999);
    expect(fakeTask).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(1);
    expect(fakeTask).toHaveBeenCalledTimes(1);

    scheduler.stop();
  });

  it("should reschedule after task completes", async () => {
    const scheduler = createCronScheduler(fakeTask, loggerMock as unknown as Logger, {
      cronExpression: "* * * * *",
      taskName: "testTask",
    });
    scheduler.start();

    await vi.advanceTimersByTimeAsync(60_000);
    expect(fakeTask).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(60_000);
    expect(fakeTask).toHaveBeenCalledTimes(2);

    scheduler.stop();
  });

  it("should stop after stop() is called", async () => {
    const scheduler = createCronScheduler(fakeTask, loggerMock as unknown as Logger, {
      cronExpression: "* * * * *",
      taskName: "testTask",
    });
    scheduler.start();
    scheduler.stop();

    await vi.advanceTimersByTimeAsync(300_000);
    expect(fakeTask).not.toHaveBeenCalled();
  });

  it("should log errors and still reschedule", async () => {
    const error = new Error("boom");
    const fakeFailingTask = vi.fn().mockRejectedValue(error);

    const scheduler = createCronScheduler(fakeFailingTask, loggerMock as unknown as Logger, {
      cronExpression: "* * * * *",
      taskName: "testTask",
    });
    scheduler.start();

    await vi.advanceTimersByTimeAsync(60_000);
    expect(loggerMock.error).toHaveBeenCalledWith(expect.objectContaining({ message: "boom", taskName: "testTask" }), "scheduler.runError");

    await vi.advanceTimersByTimeAsync(60_000);
    expect(fakeFailingTask).toHaveBeenCalledTimes(2);

    scheduler.stop();
  });

  it("should not run while task is in progress — next run schedules from task completion", async () => {
    let resolveTask!: () => void;
    const longRunningTask = vi.fn().mockImplementation(() => new Promise<void>((resolve) => (resolveTask = resolve)));

    const scheduler = createCronScheduler(longRunningTask, loggerMock as unknown as Logger, {
      cronExpression: "* * * * *",
      taskName: "testTask",
    });
    scheduler.start();

    await vi.advanceTimersByTimeAsync(60_000); // T+60s: first tick fires, task starts
    expect(longRunningTask).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(60_000); // T+120s: no timer pending while task runs
    expect(longRunningTask).toHaveBeenCalledTimes(1);

    resolveTask(); // task completes at T+120s — schedules next tick at T+180s
    await Promise.resolve();

    await vi.advanceTimersByTimeAsync(60_000); // T+180s: next tick fires
    expect(longRunningTask).toHaveBeenCalledTimes(2);

    scheduler.stop();
  });

  it("should not create duplicate chains when stop+start is called while task is in progress", async () => {
    let resolveTask!: () => void;
    const longRunningTask = vi.fn().mockImplementation(() => new Promise<void>((resolve) => (resolveTask = resolve)));

    const scheduler = createCronScheduler(longRunningTask, loggerMock as unknown as Logger, {
      cronExpression: "* * * * *",
      taskName: "testTask",
    });
    scheduler.start();

    await vi.advanceTimersByTimeAsync(60_000); // first tick fires, task starts
    expect(longRunningTask).toHaveBeenCalledTimes(1);

    scheduler.stop();
    scheduler.start(); // start a new chain while old task is still running

    resolveTask(); // old task finishes — its finally must NOT schedule a second chain
    await Promise.resolve();

    await vi.advanceTimersByTimeAsync(60_000); // only one timeout should fire
    expect(longRunningTask).toHaveBeenCalledTimes(2); // exactly one run from the new chain

    scheduler.stop();
  });

  it("should return next run time and null after stop", () => {
    const scheduler = createCronScheduler(fakeTask, loggerMock as unknown as Logger, {
      cronExpression: "* * * * *",
      taskName: "testTask",
    });
    expect(scheduler.getNextRun()).toBeNull();

    scheduler.start();
    expect(scheduler.getNextRun()).toEqual(new Date("2024-01-01T00:01:00Z"));

    scheduler.stop();
    expect(scheduler.getNextRun()).toBeNull();
  });
});
