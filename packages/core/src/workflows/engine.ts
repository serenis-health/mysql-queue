import { Job, Session } from "../types";
import { WorkflowContext, WorkflowDefinition, WorkflowStep, WorkflowStepPayload } from "./types";
import { WorkflowManager } from "./manager";

export function createWorkflowEngine(
  workflowManager: WorkflowManager,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  definitions: Record<string, WorkflowDefinition<any>>,
  onWorkflowCompleted?: (definitionName: string) => void,
) {
  async function determineNextSteps(
    step: WorkflowStep,
    stepResult: any,
    context: WorkflowContext,
    definition: WorkflowDefinition,
    session: Session,
  ): Promise<string[]> {
    // If step explicitly defines next steps
    if (step.next) {
      if (typeof step.next === "string") {
        return [step.next];
      } else if (Array.isArray(step.next)) {
        return step.next;
      } else {
        // It's a function
        const result = await step.next(context, stepResult, session);
        return Array.isArray(result) ? result : [result];
      }
    }

    // Check condition to see if we should proceed
    if (step.condition) {
      const shouldProceed = await step.condition(context, context.data, session);
      if (!shouldProceed) {
        return [];
      }
    }

    // Default: go to next sequential step
    const currentStepIndex = definition.steps.findIndex((s) => s.name === step.name);
    const nextStepIndex = currentStepIndex + 1;

    if (nextStepIndex >= definition.steps.length) {
      return [];
    }

    const nextStep = definition.steps[nextStepIndex];
    return [nextStep.name];
  }

  async function executeStep(context: WorkflowContext, step: WorkflowStep, session: Session): Promise<any> {
    return step.handler(context, context.data, session);
  }

  return {
    getDefinitions() {
      return definitions;
    },
    async processWorkflowStep(job: Job, signal: AbortSignal, session: Session) {
      if (signal.aborted) {
        return;
      }

      const payload = job.payload as WorkflowStepPayload;
      const { context, step } = payload;

      const definition = definitions[context.definitionName];
      if (!definition) {
        throw new Error(`Workflow definition '${context.definitionName}' not found`);
      }

      const stepDefinition = definition.steps.find((s) => s.name === step.name);
      if (!stepDefinition) {
        throw new Error(`Step '${step.name}' not found in workflow definition`);
      }

      try {
        const stepResult = await executeStep(context, stepDefinition, session);

        // Fetch fresh workflow state to handle parallel execution correctly
        const freshWorkflow = await workflowManager.workflowDatabase.getWorkflowById(context.workflowId);
        if (!freshWorkflow) {
          throw new Error(`Workflow ${context.workflowId} not found`);
        }

        const currentPendingSteps =
          typeof freshWorkflow.pendingSteps === "string" ? JSON.parse(freshWorkflow.pendingSteps) : freshWorkflow.pendingSteps;
        const currentCompletedSteps =
          typeof freshWorkflow.completedSteps === "string" ? JSON.parse(freshWorkflow.completedSteps) : freshWorkflow.completedSteps;
        const currentStepResults =
          typeof freshWorkflow.stepResults === "string" ? JSON.parse(freshWorkflow.stepResults) : freshWorkflow.stepResults;

        // Remove current step from pending, add to completed
        const remainingPendingSteps = currentPendingSteps.filter((s: string) => s !== context.currentStep);

        const updatedContext: WorkflowContext = {
          ...context,
          completedSteps: [...currentCompletedSteps, context.currentStep],
          pendingSteps: remainingPendingSteps,
          stepResults: { ...currentStepResults, [stepDefinition.name]: stepResult },
        };

        // Only determine next steps if all parallel steps are complete
        if (remainingPendingSteps.length === 0) {
          const nextSteps = await determineNextSteps(stepDefinition, stepResult, updatedContext, definition, session);

          if (nextSteps.length > 0) {
            // Add all next steps to pending
            const newPendingSteps = [...nextSteps];
            const nextStep = nextSteps[0];

            await workflowManager.workflowDatabase.updateWorkflowStep(
              context.workflowId,
              nextStep,
              updatedContext.data,
              updatedContext.stepResults,
              updatedContext.completedSteps,
              newPendingSteps,
              session,
            );

            const contextForNextStep = { ...updatedContext, currentStep: nextStep, pendingSteps: newPendingSteps };
            await workflowManager.enqueueNextSteps(contextForNextStep, nextSteps, definition, session);
          } else {
            // No next steps, workflow complete
            await workflowManager.workflowDatabase.updateWorkflowStep(
              context.workflowId,
              context.currentStep,
              updatedContext.data,
              updatedContext.stepResults,
              updatedContext.completedSteps,
              [],
              session,
            );
            await workflowManager.workflowDatabase.markWorkflowCompleted(context.workflowId, session);
            onWorkflowCompleted?.(context.definitionName);
          }
        } else {
          // Still have pending parallel steps, just update state
          await workflowManager.workflowDatabase.updateWorkflowStep(
            context.workflowId,
            context.currentStep,
            updatedContext.data,
            updatedContext.stepResults,
            updatedContext.completedSteps,
            remainingPendingSteps,
            session,
          );
        }
      } catch (error) {
        await workflowManager.workflowDatabase.markWorkflowFailed(
          context.workflowId,
          `Step '${step.name}' failed: ${(error as Error).message}`,
          session,
        );
        throw new Error(`Workflow step '${step.name}' failed: ${(error as Error).message}`);
      }
    },
  };
}

export type WorkflowEngine = ReturnType<typeof createWorkflowEngine>;
