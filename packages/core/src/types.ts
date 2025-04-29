import { LevelWithSilentOrString } from "pino";
import { Connection as Mysql2Connection } from "mysql2/promise";

export interface Options {
  dbUri: string;
  loggingLevel?: LevelWithSilentOrString;
  loggingPrettyPrint?: boolean;
  tablesPrefix?: string;
}

export interface Queue {
  id: string;
  name: string;
  maxRetries: number;
  minDelayMs: number;
  backoffMultiplier: number | null;
  maxDurationMs: number;
}

export interface Job {
  id: string;
  name: string;
  payload: unknown;
  status: "pending" | "completed" | "failed";
  createdAt: Date;
  completedAt: Date | null;
  failedAt: Date | null;
  attempts: number;
  latestFailureReason: string | null;
  queueId: string;
  startAfter: Date | null;
}

export interface JobForInsert {
  id: string;
  name: string;
  payload: unknown;
  status: "pending";
  priority: number;
  startAfter: Date | null;
}

export type WorkerCallback = (job: Job, signal: AbortSignal, connection: Mysql2Connection) => Promise<void> | void;

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
}

export type EnqueueParams = AddParams | AddParams[];

export type DbCreateQueueParams = Queue;
export type DbUpdateQueueParams = Queue;
export type DbAddJobsParams = JobForInsert;

export interface Connection {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  query: (sql: string, values?: any[]) => Promise<{ affectedRows: number }>;
}
