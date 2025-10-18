export type Job = {
  id: string;
  queueName: string;
  status: "pending" | "running" | "completed" | "failed" | "scheduled";
  createdAt: string;
  completedAt: string | null;
  runningAt: string | null;
  startAfter: string | null;
  failedAt: string | null;
  duration: number | null;
  payload: Record<string, unknown>;
  attempts: number;
  errors: Record<string, unknown> | null;
  maxRetries: number;
  name: string;
  completedInMs: number | null;
};
