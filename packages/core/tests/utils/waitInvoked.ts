/* eslint-disable @typescript-eslint/no-explicit-any  */

export function waitInvoked(target: any, method: string, invocationCount = 1) {
  let callCount = 0;

  return new Promise((resolve, reject) => {
    const originalMethod = target[method];
    const invocationResults: any[] = [];
    const invocationArgs: any[] = [];

    target[method] = async (...args: any[]) => {
      let error, result;
      invocationArgs.push(args);
      try {
        result = await originalMethod.call(target, ...args);
        invocationResults.push(result);
      } catch (err) {
        error = err;
        invocationResults.push(err);
      }

      if (++callCount >= invocationCount) {
        if (error) {
          reject({ args: invocationArgs, results: invocationResults });
        } else {
          resolve({ args: invocationArgs, results: invocationResults });
        }

        target[method] = originalMethod;
      }

      if (error) {
        throw error;
      }

      return result;
    };
  });
}
