import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createScheduler } from "./scheduler";
import { Logger } from "./logger";

describe("scheduler", () => {
  const loggerMock = { debug: vi.fn(), error: vi.fn(), info: vi.fn(), warn: vi.fn() };
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
    expect(loggerMock.debug).toHaveBeenCalledWith(expect.objectContaining({ taskName: "testTask" }), "scheduler.runStarted");

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

  it("should skip interval ticks when task takes longer than interval (River-style)", async () => {
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
    expect(loggerMock.debug).toHaveBeenCalledWith(expect.objectContaining({ taskName: "testTask" }), "scheduler.skipRunTaskInProgress");

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
