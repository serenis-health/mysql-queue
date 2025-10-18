export type DbJob = {
  attempts: number;
  completedAt: Date | null;
  createdAt: Date;
  failedAt: Date | null;
  runningAt: Date | null;
  id: string;
  latestFailureReason: string | null;
  name: string;
  payload: unknown;
  errors: unknown;
  priority: number;
  queueId: string;
  startAfter: Date;
  status: "pending" | "completed" | "failed" | "running";
  queueName: string;
  maxRetries: number;
};

export interface DbQueue {
  backoffMultiplier: number | null;
  id: string;
  maxDurationMs: number;
  maxRetries: number;
  minDelayMs: number;
  name: string;
  count: number;
  paused: number;
  partitionKey: string | null;
  scheduledCount?: number;
  failedCount?: number;
  completedCount?: number;
}

export type DbJobWithQueue = DbJob & {
  queueName: string;
};

export type GetJobsParams = {
  queueId?: string;
  status?: string | string[];
  queueName?: string | string[];
  name?: string | string[];
  searchQuery?: string;
  createdAtFrom?: string;
  createdAtTo?: string;
  offset?: string;
  limit?: string;
};

export type DbPeriodicJob = {
  name: string;
  lastEnqueuedAt: Date | null;
  nextRunAt: Date;
  createdAt: Date;
  updatedAt: Date;
  definition: {
    name: string;
    targetQueue: string;
    cronExpression: string;
    jobTemplate: unknown;
    catchUpStrategy: "all" | "latest" | "none";
    maxCatchUp?: number;
  };
};

export type DbLeader = {
  leaderId: string;
  electedAt: Date;
  expiresAt: Date;
};
