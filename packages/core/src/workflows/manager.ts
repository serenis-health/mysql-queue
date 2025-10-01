import { WorkflowContext, WorkflowDefinition, WorkflowStartParams } from "./types";
import { Connection } from "mysql2/promise";
import { connectionToSession } from "../database";
import { createWorkflowDatabase } from "./database";
import { MysqlQueue } from "../index";
import { randomUUID } from "node:crypto";
import { Session } from "../types";

export function createWorkflowManager(mysqlQueue: MysqlQueue, workflowQueueName = "workflows") {
  const workflowDatabase = createWorkflowDatabase(mysqlQueue.database);

  return {
    async enqueueNextSteps(context: WorkflowContext, nextSteps: string[], definition: WorkflowDefinition, session?: Session) {
      for (const nextStepName of nextSteps) {
        const nextStep = definition.steps.find((step) => step.name === nextStepName);
        if (!nextStep) {
          throw new Error(`Step '${nextStepName}' not found in workflow definition`);
        }

        const nextContext: WorkflowContext = {
          ...context,
          currentStep: nextStepName,
        };

        await mysqlQueue.enqueue(
          workflowQueueName,
          {
            name: "workflow-step",
            payload: {
              context: nextContext,
              step: { name: nextStep.name },
            },
            pendingDedupKey: `${context.workflowId}:${nextStepName}`,
          },
          session,
        );
      }
    },
    async getWorkflowProgress(workflowId: string) {
      const workflow = await workflowDatabase.getWorkflowById(workflowId);
      if (!workflow) {
        return null;
      }

      return {
        completedAt: workflow.completedAt,
        completedSteps: typeof workflow.completedSteps === "string" ? JSON.parse(workflow.completedSteps) : workflow.completedSteps,
        createdAt: workflow.createdAt,
        currentStep: workflow.currentStep,
        data: typeof workflow.data === "string" ? JSON.parse(workflow.data) : workflow.data,
        stepResults: typeof workflow.stepResults === "string" ? JSON.parse(workflow.stepResults) : workflow.stepResults,
        pendingSteps: typeof workflow.pendingSteps === "string" ? JSON.parse(workflow.pendingSteps) : workflow.pendingSteps,
        failedAt: workflow.failedAt,
        failureReason: workflow.failureReason,
        status: workflow.status,
        workflowId: workflow.id,
      };
    },
    getWorkflowQueueName() {
      return workflowQueueName;
    },

    async getWorkflowStatus(workflowId: string) {
      return await workflowDatabase.getWorkflowById(workflowId);
    },
    mysqlQueue,
    async startWorkflow<TData = unknown>(params: WorkflowStartParams<TData>) {
      const { definition, initialData, workflowId = randomUUID() } = params;

      const context: WorkflowContext = {
        completedSteps: [],
        createdAt: new Date(),
        currentStep: definition.startStep,
        data: initialData,
        stepResults: {},
        pendingSteps: [definition.startStep],
        definitionName: definition.name,
        workflowId,
      };

      const firstStep = definition.steps.find((step) => step.name === definition.startStep);
      if (!firstStep) {
        throw new Error(`Start step '${definition.startStep}' not found in workflow definition`);
      }

      // Use a transaction to ensure atomicity between creating workflow and enqueuing job
      return await mysqlQueue.database.runWithPoolConnection(async (connection: Connection) => {
        await connection.beginTransaction();
        const session = connectionToSession(connection);
        try {
          await workflowDatabase.createWorkflow(workflowId, definition.name, definition.startStep, initialData, session);
          await mysqlQueue.enqueue(
            workflowQueueName,
            {
              name: "workflow-step",
              payload: {
                context,
                step: { name: firstStep.name },
              },
              pendingDedupKey: `${workflowId}:${definition.startStep}`,
            },
            session,
          );

          await connection.commit();
          return { workflowId };
        } catch (error) {
          await connection.rollback();
          throw error;
        }
      });
    },
    workflowDatabase,
  };
}

export type WorkflowManager = ReturnType<typeof createWorkflowManager>;
