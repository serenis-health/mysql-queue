import { Database } from "../database";
import { Session } from "../types";

export function createWorkflowDatabase(database: Database) {
  return {
    async createWorkflow(workflowId: string, definitionName: string, currentStep: string, data: any, session?: Session) {
      const sql = `INSERT INTO ${database.workflowsTable()} (id, definitionName, currentStep, data, stepResults, completedSteps, pendingSteps, createdAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`;
      const values = [
        workflowId,
        definitionName,
        currentStep,
        JSON.stringify(data),
        JSON.stringify({}),
        JSON.stringify([]),
        JSON.stringify([currentStep]),
        new Date(),
      ];

      if (session) {
        await session.execute(sql, values);
      } else {
        await database.runWithPoolConnection((c) => c.execute(sql, values));
      }
    },

    async getWorkflowById(workflowId: string) {
      const [rows] = await database.runWithPoolConnection((connection) =>
        connection.query<any[]>(`SELECT * FROM ${database.workflowsTable()} WHERE id = ?`, [workflowId]),
      );
      return rows.length ? rows[0] : null;
    },

    async updateWorkflowStep<T = unknown>(
      workflowId: string,
      currentStep: string,
      data: T,
      stepResults: Record<string, unknown>,
      completedSteps: string[],
      pendingSteps: string[],
      session?: Session,
    ) {
      if (!workflowId || !currentStep) {
        throw new Error(`Invalid parameters: workflowId=${workflowId}, currentStep=${currentStep}`);
      }
      const sql = `UPDATE ${database.workflowsTable()} SET currentStep = ?, data = ?, stepResults = ?, completedSteps = ?, pendingSteps = ? WHERE id = ?`;
      const values = [
        currentStep,
        JSON.stringify(data || {}),
        JSON.stringify(stepResults || {}),
        JSON.stringify(completedSteps || []),
        JSON.stringify(pendingSteps || []),
        workflowId,
      ];

      if (session) {
        await session.execute(sql, values);
      } else {
        await database.runWithPoolConnection((c) => c.execute(sql, values));
      }
    },

    async markWorkflowCompleted(workflowId: string, session?: Session) {
      if (!workflowId) {
        throw new Error(`Invalid workflowId: ${workflowId}`);
      }
      const sql = `UPDATE ${database.workflowsTable()} SET status = ?, completedAt = ? WHERE id = ?`;
      const values = ["completed", new Date(), workflowId];

      if (session) {
        await session.execute(sql, values);
      } else {
        await database.runWithPoolConnection((c) => c.execute(sql, values));
      }
    },

    async markWorkflowFailed(workflowId: string, failureReason: string, session?: Session) {
      if (!workflowId) {
        throw new Error(`Invalid workflowId: ${workflowId}`);
      }
      const sql = `UPDATE ${database.workflowsTable()} SET status = ?, failedAt = ?, failureReason = ? WHERE id = ?`;
      const values = ["failed", new Date(), failureReason || "Unknown error", workflowId];

      if (session) {
        await session.execute(sql, values);
      } else {
        await database.runWithPoolConnection((c) => c.query(sql, values));
      }
    },
  };
}

export type WorkflowDatabase = ReturnType<typeof createWorkflowDatabase>;
