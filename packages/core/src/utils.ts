export function sleep(ms: number) {
  return new Promise<void>((resolve) => setTimeout(resolve, ms));
}

export function errorToJson(error: Error) {
  return {
    message: error.message,
    name: error.name,
    stack: error.stack,
  };
}

export function truncateStr(string: string, length: number) {
  if (string.length <= length) {
    return string;
  }
  return `${string.slice(0, length)} <truncated>`;
}

export function chunk<T>(array: T[], size: number): T[][] {
  if (size <= 0) throw new Error("chunk size must be greater than 0");
  const result: T[][] = [];
  for (let i = 0; i < array.length; i += size) {
    result.push(array.slice(i, i + size));
  }
  return result;
}
