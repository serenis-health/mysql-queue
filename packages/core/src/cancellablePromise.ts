export function cancellablePromiseFactory(
  executor: (signal: AbortSignal) => Promise<void>,
  externalSignal?: AbortSignal,
) {
  const controller = new AbortController();
  const { signal } = controller;

  if (externalSignal) {
    externalSignal.addEventListener("abort", () => controller.abort());
  }

  return {
    cancel: () => controller.abort(),
    promise: new Promise<void>((resolve, reject) => {
      signal.addEventListener("abort", () => reject(new Error("Aborted")));

      executor(signal).then(resolve).catch(reject);
    }),
  };
}
