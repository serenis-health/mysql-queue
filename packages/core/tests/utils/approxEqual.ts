export function approxEqual(value: number, expected: number, tolerance: number) {
  return Math.abs(value - expected) <= tolerance;
}
