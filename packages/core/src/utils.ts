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
