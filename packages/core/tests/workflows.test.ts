import { afterAll, afterEach, beforeAll, describe, it } from "vitest";
import { createWorkflows, MysqlQueue } from "../src";
import { QueryDatabase } from "./utils/queryDatabase";
import { WorkflowDefinition } from "../src/workflows";

const dbUri = "mysql://root:password@localhost:3306/serenis";

describe("workflows", () => {
  const queryDatabase = QueryDatabase({ dbUri });
  const mysqlQueue = MysqlQueue({
    dbUri,
    //loggingLevel: "fatal",
  });

  beforeAll(async () => {
    await mysqlQueue.globalInitialize();
  });

  afterAll(async () => {
    //await mysqlQueue.globalDestroy();
    await mysqlQueue.dispose();
    await queryDatabase.dispose();
  });

  afterEach(async () => {
    await mysqlQueue.purge();
  });

  describe("workflow creation and execution", () => {
    it("should execute a workflow with explicit next transitions", async () => {
      const workflows = createWorkflows(mysqlQueue);
      await workflows.initialize();

      interface WorkflowData {
        value: number;
      }

      const definition: WorkflowDefinition<WorkflowData> = {
        name: "branching-workflow",
        startStep: "start",
        steps: [
          {
            handler: async () => 10,
            name: "start",
            next: (context, stepResult) => {
              // Branch based on step result
              return stepResult > 5 ? "highPath" : "lowPath";
            },
          },
          {
            handler: async () => "took high path",
            name: "highPath",
            next: "end",
          },
          {
            handler: async () => "took low path",
            name: "lowPath",
            next: "end",
          },
          {
            handler: async () => "completed",
            name: "end",
          },
        ],
      };

      const worker = await workflows.startWorker();
      void worker.start();

      const promise = mysqlQueue.getJobExecutionPromise("workflows", 3); // 3 steps in workflow
      const { workflowId } = await workflows.startWorkflow({
        definition,
        initialData: { value: 10 },
      });

      await promise;

      const progress = await workflows.getWorkflowProgress(workflowId);
      if (!progress) {
        throw new Error("Workflow not found");
      }

      // Should have taken high path
      if (!progress.completedSteps.includes("highPath")) {
        throw new Error("Expected highPath to be completed");
      }
      if (progress.completedSteps.includes("lowPath")) {
        throw new Error("Expected lowPath to not be completed");
      }

      await worker.stop();
    }, 20_000);

    it("should execute a simple linear workflow", async () => {
      const workflows = createWorkflows(mysqlQueue);
      await workflows.initialize();

      interface WorkflowData {
        userId: string;
        step1?: string;
        step2?: string;
      }
      const definition: WorkflowDefinition<WorkflowData> = {
        name: "simple-workflow",
        startStep: "step1",
        steps: [
          {
            handler: async (_context, _data: WorkflowData, _session) => {
              await new Promise((resolve) => setTimeout(resolve, 2000));
              return "completed";
            },
            name: "step1",
          },
          {
            handler: async (_context, _data: WorkflowData, _session) => {
              await new Promise((resolve) => setTimeout(resolve, 2000));
              return "done";
            },
            name: "step2",
          },
          {
            handler: async (_context, _data: WorkflowData, _session) => {
              await new Promise((resolve) => setTimeout(resolve, 2000));
              return "success";
            },
            name: "step3",
          },
        ],
      };

      const worker = await workflows.startWorker();
      void worker.start();

      const promise = mysqlQueue.getJobExecutionPromise("workflows", 3); // 3 steps in workflow
      await workflows.startWorkflow({
        definition,
        initialData: { userId: "123" },
      });

      await promise;
      await worker.stop();
    }, 20_000);

    it("should store step results separately from workflow data", async () => {
      const workflows = createWorkflows(mysqlQueue);
      await workflows.initialize();

      interface WorkflowData {
        userId: string;
        step1?: string; // This should NOT collide with step result
      }

      const definition: WorkflowDefinition<WorkflowData> = {
        name: "step-results-workflow",
        startStep: "step1",
        steps: [
          {
            handler: async () => {
              return { result: "step1 completed" };
            },
            name: "step1",
          },
          {
            handler: async () => {
              return { result: "step2 completed" };
            },
            name: "step2",
          },
        ],
      };

      const worker = await workflows.startWorker();
      void worker.start();

      const promise = mysqlQueue.getJobExecutionPromise("workflows", 2); // 2 steps in workflow
      const { workflowId } = await workflows.startWorkflow({
        definition,
        initialData: { userId: "test-user", step1: "original-value" },
      });

      await promise;

      const progress = await workflows.getWorkflowProgress(workflowId);
      if (!progress) {
        throw new Error("Workflow not found");
      }

      // Check that original data is preserved
      if ((progress.data as any).userId !== "test-user") {
        throw new Error(`Expected userId to be test-user but got ${(progress.data as any).userId}`);
      }
      if ((progress.data as any).step1 !== "original-value") {
        throw new Error(`Expected step1 data to be original-value but got ${(progress.data as any).step1}`);
      }

      // Check that step results are stored separately
      if (!progress.stepResults) {
        throw new Error("Expected stepResults to exist");
      }
      if (!(progress.stepResults as any).step1) {
        throw new Error("Expected step1 result to exist");
      }
      if ((progress.stepResults as any).step1.result !== "step1 completed") {
        throw new Error(`Expected step1 result to be 'step1 completed' but got ${(progress.stepResults as any).step1.result}`);
      }

      await worker.stop();
    }, 10_000);

    it("should execute parallel steps concurrently", async () => {
      const workflows = createWorkflows(mysqlQueue);
      await workflows.initialize();

      const executionOrder: string[] = [];

      interface WorkflowData {
        value: number;
      }

      const definition: WorkflowDefinition<WorkflowData> = {
        name: "parallel-workflow",
        startStep: "start",
        steps: [
          {
            handler: async () => {
              executionOrder.push("start");
              return "started";
            },
            name: "start",
            next: ["parallel1", "parallel2"], // Both should run in parallel
          },
          {
            handler: async () => {
              executionOrder.push("parallel1");
              await new Promise((resolve) => setTimeout(resolve, 100));
              return "p1-done";
            },
            name: "parallel1",
            next: "end", // Both parallel steps converge to end
          },
          {
            handler: async () => {
              executionOrder.push("parallel2");
              await new Promise((resolve) => setTimeout(resolve, 100));
              return "p2-done";
            },
            name: "parallel2",
            next: "end", // Both parallel steps converge to end
          },
          {
            handler: async () => {
              executionOrder.push("end");
              return "completed";
            },
            name: "end",
          },
        ],
      };

      const worker = await workflows.startWorker();
      void worker.start();

      const promise = mysqlQueue.getJobExecutionPromise("workflows", 4); // 4 steps total
      const { workflowId } = await workflows.startWorkflow({
        definition,
        initialData: { value: 1 },
      });

      await promise;

      const progress = await workflows.getWorkflowProgress(workflowId);
      if (!progress) {
        throw new Error("Workflow not found");
      }

      // Verify workflow completed
      if (progress.status !== "completed") {
        throw new Error(`Expected workflow to be completed but got ${progress.status}`);
      }

      // Verify both parallel steps executed
      if (!progress.completedSteps.includes("parallel1")) {
        throw new Error("Expected parallel1 to be completed");
      }
      if (!progress.completedSteps.includes("parallel2")) {
        throw new Error("Expected parallel2 to be completed");
      }

      // Verify end step ran after both parallel steps
      const parallel1Index = executionOrder.indexOf("parallel1");
      const parallel2Index = executionOrder.indexOf("parallel2");
      const endIndex = executionOrder.indexOf("end");

      if (endIndex === -1 || parallel1Index === -1 || parallel2Index === -1) {
        throw new Error(`Missing steps in execution order: ${executionOrder.join(", ")}`);
      }

      if (endIndex < parallel1Index || endIndex < parallel2Index) {
        throw new Error(`End step executed before parallel steps completed: ${executionOrder.join(", ")}`);
      }

      await worker.stop();
    }, 10_000);

    it("should prevent duplicate step execution with pendingDedupKey", async () => {
      const workflows = createWorkflows(mysqlQueue);
      await workflows.initialize();

      const executionCounts = new Map<string, number>();

      interface WorkflowData {
        value: number;
      }

      const definition: WorkflowDefinition<WorkflowData> = {
        name: "dedup-workflow",
        startStep: "step1",
        steps: [
          {
            handler: async (context) => {
              const count = (executionCounts.get(context.currentStep) || 0) + 1;
              executionCounts.set(context.currentStep, count);
              return "done";
            },
            name: "step1",
            next: ["step2", "step2"], // Intentionally duplicate - should only execute once
          },
          {
            handler: async (context) => {
              const count = (executionCounts.get(context.currentStep) || 0) + 1;
              executionCounts.set(context.currentStep, count);
              return "done";
            },
            name: "step2",
          },
        ],
      };

      const worker = await workflows.startWorker();
      void worker.start();

      const promise = mysqlQueue.getJobExecutionPromise("workflows", 2); // Should be 2 jobs, not 3
      const { workflowId } = await workflows.startWorkflow({
        definition,
        initialData: { value: 1 },
      });

      await promise;

      const progress = await workflows.getWorkflowProgress(workflowId);
      if (!progress) {
        throw new Error("Workflow not found");
      }

      // Verify workflow completed
      if (progress.status !== "completed") {
        throw new Error(`Expected workflow to be completed but got ${progress.status}`);
      }

      // Verify step2 only executed once despite being in next array twice
      const step2Count = executionCounts.get("step2");
      if (step2Count !== 1) {
        throw new Error(`Expected step2 to execute once but got ${step2Count} times`);
      }

      await worker.stop();
    }, 10_000);

    it("should retry failed steps according to queue configuration", async () => {
      const attemptCounts = new Map<string, number>();

      const workflows = createWorkflows(mysqlQueue, {
        backoffMultiplier: 1,
        maxRetries: 3,
        minRetryDelayMs: 100,
        queueName: "foo",
      });
      await workflows.initialize();

      interface WorkflowData {
        value: number;
      }

      const definition: WorkflowDefinition<WorkflowData> = {
        name: "retry-workflow",
        startStep: "failingStep",
        steps: [
          {
            handler: async (context) => {
              const count = (attemptCounts.get(context.workflowId) || 0) + 1;
              attemptCounts.set(context.workflowId, count);

              if (count < 3) {
                throw new Error("Temporary failure");
              }
              return "success";
            },
            name: "failingStep",
          },
        ],
      };

      const worker = await workflows.startWorker();
      void worker.start();

      const promise = mysqlQueue.getJobExecutionPromise("foo", 3);
      const { workflowId } = await workflows.startWorkflow({
        definition,
        initialData: { value: 1 },
      });

      await promise;

      const progress = await workflows.getWorkflowProgress(workflowId);
      if (!progress) {
        throw new Error("Workflow not found");
      }

      // Should have succeeded after retries
      if (progress.status !== "completed") {
        throw new Error(`Expected workflow to be completed but got ${progress.status}`);
      }

      // Should have attempted 3 times (1 initial + 2 retries)
      const finalCount = attemptCounts.get(workflowId);
      if (finalCount !== 3) {
        throw new Error(`Expected 3 attempts but got ${finalCount}`);
      }

      await worker.stop();
    }, 20_000);
  });
});
