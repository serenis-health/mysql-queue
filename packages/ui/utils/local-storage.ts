export function getLocalStorage(key: string): string | null {
  if (typeof window === "undefined") {
    return null;
  }

  try {
    return localStorage.getItem(key);
  } catch (error) {
    console.error(`Error reading from local storage (key: ${key}):`, error);
    return null;
  }
}

export function setLocalStorage(key: string, value: string): boolean {
  if (typeof window === "undefined") {
    return false;
  }

  try {
    localStorage.setItem(key, value);
    return true;
  } catch (error) {
    console.error(`Error writing to local storage (key: ${key}):`, error);
    return false;
  }
}
