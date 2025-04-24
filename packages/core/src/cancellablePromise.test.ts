import { describe, expect, it } from "vitest";
import { cancellablePromiseFactory } from "./cancellablePromise";
import { sleep } from "./utils";

describe("cancellablePromise", () => {
  it("should resolve without cancelling", async () => {
    const { promise } = cancellablePromiseFactory(async () => {
      await sleep(200);
    });

    await promise;
  });

  it("should be cancellable before resolving", async () => {
    const { promise, cancel } = cancellablePromiseFactory(async (signal) => {
      await new Promise<void>((resolve) => {
        const timerId = setTimeout(resolve, 200);
        signal.addEventListener("abort", () => {
          clearTimeout(timerId);
        });
      });
    });

    setTimeout(cancel, 100);

    await expect(() => promise).rejects.toThrow("Aborted");
  });

  it("should be cancellable before resolving with another cancellable promise", async () => {
    const { promise, cancel } = cancellablePromiseFactory(async (signal) => {
      const { promise: innerPromise } = cancellablePromiseFactory(() => sleep(200), signal);
      await innerPromise;
    });

    setTimeout(cancel, 100);

    await expect(() => promise).rejects.toThrow("Aborted");
  });

  it("should resolve after waiting the correct time without cancellation", async () => {
    const { promise } = cancellablePromiseFactory(async () => {
      await sleep(1000);
    });

    const startTime = Date.now();
    await promise;
    const endTime = Date.now();

    expect(endTime - startTime).toBeGreaterThanOrEqual(1000);
  });
});
