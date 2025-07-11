import { LevelWithSilentOrString } from "pino";

export interface Options {
  dbUri: string;
  loggingLevel?: LevelWithSilentOrString;
  loggingPrettyPrint?: boolean;
  maxPayloadSizeKb?: number;
  tablesPrefix?: string;
}

export interface Queue {
  id: string;
  name: string;
  maxRetries: number;
  minDelayMs: number;
  backoffMultiplier: number;
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
}

export type WorkerCallback = (job: Job, signal: AbortSignal, session: Session) => Promise<void> | void;

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
export type DbAddJobsParams = JobForInsert[];

export type Session = {
  query: (sql: string, parameters: unknown[]) => Promise<[{ affectedRows: number }]>;
};
