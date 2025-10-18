export type PeriodicJob = {
  name: string;
  cronExpression?: string;
  targetQueue?: string;
  lastEnqueuedAt: string | null;
  nextRunAt: string;
  createdAt: string;
  updatedAt: string;
  definition: {
    name: string;
    targetQueue: string;
    cronExpression: string;
    jobTemplate: unknown;
    catchUpStrategy: "all" | "latest" | "none";
    maxCatchUp?: number;
  } | null;
};
