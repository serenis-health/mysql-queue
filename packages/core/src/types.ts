import { LevelWithSilentOrString } from "pino";

export interface Options {
  dbUri: string;
  loggingLevel?: LevelWithSilentOrString;
  loggingPrettyPrint?: boolean;
  maxPayloadSizeKb?: number;
  tablesPrefix?: string;
  partitionKey?: string;
  rescuerIntervalMs?: number;
  rescuerRescueAfterMs?: number;
  rescuerBatchSize?: number;
  rescuerRunOnStart?: boolean;
}

export interface Queue {
  id: string;
  name: string;
  maxRetries: number;
  minDelayMs: number;
  backoffMultiplier: number;
  maxDurationMs: number;
  partitionKey: string;
  paused: boolean;
}

export interface Job {
  id: string;
  name: string;
  payload: unknown;
  status: "pending" | "running" | "completed" | "failed";
  createdAt: Date;
  completedAt: Date | null;
  failedAt: Date | null;
  attempts: number;
  latestFailureReason: string | null;
  queueId: string;
  startAfter: Date;
}

export type JobWithQueueName = Job & { queueName: string };

export interface JobForInsert {
  id: string;
  name: string;
  payload: unknown;
  status: "pending";
  priority: number;
  startAfter: Date;
  createdAt: Date;
  idempotentKey?: string;
  pendingDedupKey?: string;
}

export type WorkerCallback = (jobs: Job[], signal: AbortSignal, ctx: CallbackContext) => Promise<void> | void;

export interface UpsertQueueParams {
  maxRetries?: number;
  minDelayMs?: number;
  backoffMultiplier?: number | null;
  maxDurationMs?: number;
}

export interface RetrieveQueueParams {
  name: string;
}

export interface AddParams {
  name: string;
  payload: unknown;
  priority?: number;
  startAfter?: Date;
  idempotentKey?: string;
  pendingDedupKey?: string;
}

export type EnqueueParams = AddParams | AddParams[];

export interface PurgePartitionParams {
  partitionKey: string;
}

export type DbCreateQueueParams = Queue;
export type DbUpdateQueueParams = Queue;
export type DbAddJobsParams = JobForInsert[];

export type Session = {
  query: <TRow = unknown>(sql: string, parameters: unknown[]) => Promise<TRow[]>;
  execute: (sql: string, parameters: unknown[]) => Promise<[{ affectedRows: number }]>;
};

export type CallbackContext = {
  markJobsAsCompleted: (session: Session) => Promise<void>;
};
