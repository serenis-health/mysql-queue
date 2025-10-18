export type Queue = {
  id: string;
  name: string;
  jobsCount: number;
  isPaused: boolean;
  scheduledCount: number;
  failedCount: number;
  completedCount: number;
  maxRetries: number;
  minDelayMs: number;
  maxDurationMs: number;
  backoffMultiplier: number | null;
  partitionKey: string | null;
};
