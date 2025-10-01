export interface WorkflowStep<TData = unknown, TStepData = unknown> {
  name: string;
  handler: WorkflowStepHandler<TData, TStepData>;
  condition?: WorkflowConditionHandler<TData>;
  next?: string | string[] | WorkflowNextHandler<TData, TStepData>;
}

export interface WorkflowDefinition<TData = unknown> {
  name: string;
  steps: WorkflowStep<TData>[];
  startStep: string;
}

export interface WorkflowContext<TData = unknown> {
  workflowId: string;
  definitionName: string;
  currentStep: string;
  data: TData;
  stepResults: Record<string, unknown>;
  completedSteps: string[];
  pendingSteps: string[];
  createdAt: Date;
}

export interface WorkflowStepPayload<TData = unknown> {
  context: WorkflowContext<TData>;
  step: { name: string };
}

export type WorkflowStepHandler<TData = unknown, TStepData = unknown> = (
  context: WorkflowContext<TData>,
  data: TData,
  session: import("../types").Session,
) => Promise<TStepData> | TStepData;

export type WorkflowConditionHandler<TData = unknown> = (
  context: WorkflowContext<TData>,
  data: TData,
  session: import("../types").Session,
) => Promise<boolean> | boolean;

export type WorkflowNextHandler<TData = unknown, TStepData = unknown> = (
  context: WorkflowContext<TData>,
  stepResult: TStepData,
  session: import("../types").Session,
) => Promise<string | string[]> | string | string[];

export interface WorkflowStartParams<TData = unknown> {
  definition: WorkflowDefinition<TData>;
  initialData: TData;
  workflowId?: string;
}

export interface WorkflowsOptions {
  queueName?: string;
  maxRetries?: number;
  minRetryDelayMs?: number;
  backoffMultiplier?: number;
  maxStepDurationMs?: number;
}
