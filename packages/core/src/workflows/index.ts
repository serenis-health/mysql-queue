import { WorkflowDefinition, WorkflowsOptions, WorkflowStartParams } from "./types";
import { createWorkflowEngine } from "./engine";
import { createWorkflowManager } from "./manager";
import { MysqlQueue } from "../index";

export function createWorkflows(mysqlQueue: MysqlQueue, options: WorkflowsOptions = {}) {
  const workflowQueueName = options.queueName || "workflows";
  const definitions: Record<string, WorkflowDefinition<any>> = {};

  const workflowManager = createWorkflowManager(mysqlQueue, workflowQueueName);
  const workflowEngine = createWorkflowEngine(workflowManager, definitions);

  return {
    getDefinitions() {
      return workflowEngine.getDefinitions();
    },
    async getWorkflowProgress(workflowId: string) {
      return await workflowManager.getWorkflowProgress(workflowId);
    },
    getWorkflowQueueName() {
      return workflowManager.getWorkflowQueueName();
    },
    async getWorkflowStatus(workflowId: string) {
      return await workflowManager.getWorkflowStatus(workflowId);
    },
    async initialize() {
      await mysqlQueue.upsertQueue(workflowQueueName, {
        maxRetries: options.maxRetries ?? 3,
        minDelayMs: options.minRetryDelayMs ?? 1000,
        backoffMultiplier: options.backoffMultiplier ?? 2,
        maxDurationMs: options.maxStepDurationMs ?? 30000,
      });
    },
    async startWorker(pollingIntervalMs = 500, batchSize = 1) {
      return await mysqlQueue.work(workflowQueueName, workflowEngine.processWorkflowStep, pollingIntervalMs, batchSize);
    },
    async startWorkflow<TData = unknown>(params: WorkflowStartParams<TData>) {
      if (!definitions[params.definition.name]) {
        definitions[params.definition.name] = params.definition;
      }
      return await workflowManager.startWorkflow(params);
    },
  };
}

export type Workflows = ReturnType<typeof createWorkflows>;

export * from "./types";
export { createWorkflowManager, createWorkflowEngine };
