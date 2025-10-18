import { buildScheduledStatusFilter, normalizeToArray, parsePagination, withConnection } from "@/lib/db-utils";
import type { DbJobWithQueue, DbLeader, DbPeriodicJob, DbQueue, GetJobsParams } from "@/lib/db-types";
import mysql, { Pool, RowDataPacket } from "mysql2/promise";
import { Job } from "@/types/job";
import { Queue } from "@/types/queue";

const pools = new Map<string, Pool>();

function getPool(dbUri: string): Pool {
  if (!pools.has(dbUri)) {
    pools.set(
      dbUri,
      mysql.createPool({
        uri: dbUri,
        timezone: "Z",
        connectionLimit: 20,
      }),
    );
  }
  return pools.get(dbUri)!;
}

export async function getQueues(dbUri: string, params?: { limit?: string; offset?: string }): Promise<{ queues: Queue[]; total: number }> {
  return withConnection(getPool(dbUri), async (connection) => {
    const [countRows] = await connection.query<RowDataPacket[]>(`
      SELECT COUNT(*) as total
      FROM mysql_queue_queues
    `);
    const total = countRows[0].total as number;

    const { limit, offset } = parsePagination(params);

    const [rows] = await connection.query<RowDataPacket[]>(
      `
      SELECT
        q.id,
        q.name,
        q.paused,
        q.maxRetries,
        q.minDelayMs,
        q.maxDurationMs,
        q.backoffMultiplier,
        q.partitionKey,
        COUNT(j.id) as count,
        SUM(CASE WHEN j.status = 'pending' AND j.startAfter != j.createdAt THEN 1 ELSE 0 END) as scheduledCount,
        SUM(CASE WHEN j.status = 'failed' THEN 1 ELSE 0 END) as failedCount,
        SUM(CASE WHEN j.status = 'completed' THEN 1 ELSE 0 END) as completedCount
      FROM mysql_queue_queues q
      LEFT JOIN mysql_queue_jobs j ON q.id = j.queueId
      GROUP BY q.id, q.name, q.maxRetries, q.minDelayMs, q.maxDurationMs, q.backoffMultiplier, q.partitionKey
      ORDER BY q.name
      LIMIT ? OFFSET ?
    `,
      [limit, offset],
    );
    const dbQueues: DbQueue[] = rows as unknown as DbQueue[];

    const queues = dbQueues.map((q) => ({
      id: q.id,
      name: q.name,
      jobsCount: q.count,
      isPaused: !!q.paused,
      scheduledCount: q.scheduledCount || 0,
      failedCount: q.failedCount || 0,
      completedCount: q.completedCount || 0,
      maxRetries: q.maxRetries,
      minDelayMs: q.minDelayMs,
      maxDurationMs: q.maxDurationMs,
      backoffMultiplier: q.backoffMultiplier,
      partitionKey: q.partitionKey,
    })) satisfies Queue[];

    return { queues, total };
  });
}

export async function getJobs(dbUri: string, params: GetJobsParams): Promise<{ jobs: Job[]; total: number }> {
  return withConnection(getPool(dbUri), async (connection) => {
    let whereClause = `WHERE 1=1`;
    const filters: string[] = [];

    if (params.queueId) {
      whereClause += ` AND j.queueId = ?`;
      filters.push(params.queueId);
    }

    if (params.status) {
      const statuses = normalizeToArray(params.status);
      if (statuses.length > 0) {
        const statusFilter = buildScheduledStatusFilter(statuses);
        whereClause += statusFilter.clause;
        filters.push(...statusFilter.params);
      }
    }

    if (params.queueName) {
      const queueNames = normalizeToArray(params.queueName);
      if (queueNames.length > 0) {
        whereClause += ` AND q.name IN (${queueNames.map(() => "?").join(",")})`;
        filters.push(...queueNames);
      }
    }

    if (params.name) {
      const names = normalizeToArray(params.name);
      if (names.length > 0) {
        whereClause += ` AND j.name IN (${names.map(() => "?").join(",")})`;
        filters.push(...names);
      }
    }

    if (params.searchQuery) {
      whereClause += ` AND (j.id LIKE ? OR JSON_SEARCH(j.payload, 'one', ?, NULL, '$') IS NOT NULL)`;
      filters.push(`%${params.searchQuery}%`);
      filters.push(`%${params.searchQuery}%`);
    }

    if (params.createdAtFrom) {
      whereClause += ` AND j.createdAt >= ?`;
      filters.push(params.createdAtFrom);
    }

    if (params.createdAtTo) {
      whereClause += ` AND j.createdAt <= ?`;
      filters.push(params.createdAtTo);
    }

    const countQuery = `
      SELECT COUNT(*) as total
      FROM mysql_queue_jobs j
      LEFT JOIN mysql_queue_queues q ON j.queueId = q.id
      ${whereClause}
    `;
    const [countRows] = await connection.query<RowDataPacket[]>(countQuery, filters);
    const total = countRows[0].total as number;

    const { limit, offset } = parsePagination(params);

    const jobsQuery = `
      SELECT j.*, q.name as queueName, q.maxRetries
      FROM mysql_queue_jobs j
      LEFT JOIN mysql_queue_queues q ON j.queueId = q.id
      ${whereClause}
      ORDER BY j.createdAt DESC LIMIT ? OFFSET ?
    `;
    const [rows] = await connection.query(jobsQuery, [...filters, limit, offset]);
    const dbJobs = rows as unknown as DbJobWithQueue[];

    const jobs: Job[] = dbJobs.map((j) => {
      const isScheduled = j.status === "pending" && j.createdAt.getTime() !== j.startAfter.getTime();
      const endedAt = j.completedAt || j.failedAt;
      return {
        id: j.id,
        completedAt: j.completedAt ? j.completedAt.toISOString() : null,
        createdAt: j.createdAt.toISOString(),
        duration: endedAt && j.runningAt ? endedAt.getTime() - j.createdAt.getTime() : null,
        failedAt: j.failedAt ? j.failedAt.toISOString() : null,
        queueName: j.queueName,
        runningAt: j.runningAt ? j.runningAt.toISOString() : null,
        startAfter: j.startAfter.toISOString(),
        status: isScheduled ? "scheduled" : j.status,
        payload: j.payload as Record<string, unknown>,
        attempts: j.attempts,
        errors: j.errors as Record<string, unknown> | null,
        maxRetries: j.maxRetries,
        name: j.name,
        completedInMs: endedAt ? endedAt.getTime() - j.createdAt.getTime() : null,
      };
    });

    return { jobs, total };
  });
}

export async function getJobById(dbUri: string, id: string): Promise<DbJobWithQueue | null> {
  return withConnection(getPool(dbUri), async (connection) => {
    const [rows] = await connection.query<RowDataPacket[]>(
      `SELECT j.*, q.name as queueName
       FROM mysql_queue_jobs j
       LEFT JOIN mysql_queue_queues q ON j.queueId = q.id
       WHERE j.id = ?`,
      [id],
    );

    if (rows.length === 0) return null;
    return rows[0] as unknown as DbJobWithQueue;
  });
}

export async function getJobCounts(dbUri: string) {
  return withConnection(getPool(dbUri), async (connection) => {
    const [rows] = await connection.query(`
      SELECT status, COUNT(*) as count
      FROM mysql_queue_jobs
      GROUP BY status
    `);

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return (rows as any[]).reduce(
      (acc, row) => {
        acc[row.status] = row.count;
        return acc;
      },
      {} as Record<string, number>,
    );
  });
}

export async function getJobCountsByFilters(dbUri: string, params: GetJobsParams) {
  return withConnection(getPool(dbUri), async (connection) => {
    const baseFilters: string[] = [];
    let baseWhereClause = `WHERE 1=1`;

    if (params.searchQuery) {
      baseWhereClause += ` AND (j.id LIKE ? OR JSON_SEARCH(j.payload, 'one', ?, NULL, '$') IS NOT NULL)`;
      baseFilters.push(`%${params.searchQuery}%`);
      baseFilters.push(`%${params.searchQuery}%`);
    }

    if (params.createdAtFrom) {
      baseWhereClause += ` AND j.createdAt >= ?`;
      baseFilters.push(params.createdAtFrom);
    }

    if (params.createdAtTo) {
      baseWhereClause += ` AND j.createdAt <= ?`;
      baseFilters.push(params.createdAtTo);
    }

    let statusWhereClause = baseWhereClause;
    const statusFilters = [...baseFilters];

    if (params.queueName) {
      const queueNames = normalizeToArray(params.queueName);
      if (queueNames.length > 0) {
        statusWhereClause += ` AND q.name IN (${queueNames.map(() => "?").join(",")})`;
        statusFilters.push(...queueNames);
      }
    }

    const statusQuery = `
      SELECT j.status, COUNT(*) as count
      FROM mysql_queue_jobs j
      LEFT JOIN mysql_queue_queues q ON j.queueId = q.id
      ${statusWhereClause}
      GROUP BY j.status
    `;
    const [statusRows] = await connection.query<RowDataPacket[]>(statusQuery, statusFilters);
    const statusCounts = statusRows.reduce(
      (acc, row) => {
        acc[row.status as string] = row.count as number;
        return acc;
      },
      {} as Record<string, number>,
    );

    const scheduledQuery = `
      SELECT COUNT(*) as count
      FROM mysql_queue_jobs j
      LEFT JOIN mysql_queue_queues q ON j.queueId = q.id
      ${statusWhereClause}
      AND j.status = 'pending'
      AND j.startAfter != j.createdAt
    `;
    const [scheduledRows] = await connection.query<RowDataPacket[]>(scheduledQuery, statusFilters);
    const scheduledCount = (scheduledRows[0] as { count: number }).count;

    if (scheduledCount > 0) {
      statusCounts.scheduled = scheduledCount;
      if (statusCounts.pending) {
        statusCounts.pending = statusCounts.pending - scheduledCount;
      }
    }

    let queueWhereClause = baseWhereClause;
    const queueFilters = [...baseFilters];

    if (params.status) {
      const statuses = normalizeToArray(params.status);
      if (statuses.length > 0) {
        const statusFilter = buildScheduledStatusFilter(statuses);
        queueWhereClause += statusFilter.clause;
        queueFilters.push(...statusFilter.params);
      }
    }

    const queueQuery = `
      SELECT q.name, COUNT(*) as count
      FROM mysql_queue_jobs j
      LEFT JOIN mysql_queue_queues q ON j.queueId = q.id
      ${queueWhereClause}
      GROUP BY q.name
    `;
    const [queueRows] = await connection.query<RowDataPacket[]>(queueQuery, queueFilters);
    const queueCounts = queueRows.reduce(
      (acc, row) => {
        acc[row.name as string] = row.count as number;
        return acc;
      },
      {} as Record<string, number>,
    );

    let nameWhereClause = baseWhereClause;
    const nameFilters = [...baseFilters];

    if (params.status) {
      const statuses = normalizeToArray(params.status);
      if (statuses.length > 0) {
        const statusFilter = buildScheduledStatusFilter(statuses);
        nameWhereClause += statusFilter.clause;
        nameFilters.push(...statusFilter.params);
      }
    }

    if (params.queueName) {
      const queueNames = normalizeToArray(params.queueName);
      if (queueNames.length > 0) {
        nameWhereClause += ` AND q.name IN (${queueNames.map(() => "?").join(",")})`;
        nameFilters.push(...queueNames);
      }
    }

    const nameQuery = `
      SELECT j.name, COUNT(*) as count
      FROM mysql_queue_jobs j
      LEFT JOIN mysql_queue_queues q ON j.queueId = q.id
      ${nameWhereClause}
      GROUP BY j.name
    `;
    const [nameRows] = await connection.query<RowDataPacket[]>(nameQuery, nameFilters);
    const nameCounts = nameRows.reduce(
      (acc, row) => {
        acc[row.name as string] = row.count as number;
        return acc;
      },
      {} as Record<string, number>,
    );

    return {
      byStatus: statusCounts,
      byQueue: queueCounts,
      byName: nameCounts,
    };
  });
}

export async function getDashboardStats(dbUri: string, params: { days: number }) {
  return withConnection(getPool(dbUri), async (connection) => {
    const { days } = params;
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - days);

    const totalJobsQuery = `
      SELECT DATE(createdAt) as date, COUNT(*) as jobsCount
      FROM mysql_queue_jobs
      WHERE createdAt >= ?
      GROUP BY DATE(createdAt)
      ORDER BY date ASC
    `;
    const [totalJobsRows] = await connection.query<RowDataPacket[]>(totalJobsQuery, [startDate.toISOString()]);

    const failuresQuery = `
      SELECT
        DATE(createdAt) as date,
        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failures,
        SUM(CASE WHEN status = 'completed' AND attempts > 1 THEN 1 ELSE 0 END) as retries
      FROM mysql_queue_jobs
      WHERE createdAt >= ?
      GROUP BY DATE(createdAt)
      ORDER BY date ASC
    `;
    const [failuresRows] = await connection.query<RowDataPacket[]>(failuresQuery, [startDate.toISOString()]);

    const totalJobsData = totalJobsRows.map((row) => ({
      date: new Date(row.date as string).toISOString(),
      jobsCount: row.jobsCount as number,
    }));

    const failuresData = failuresRows.map((row) => ({
      date: new Date(row.date as string).toISOString(),
      failures: row.failures as number,
      retries: row.retries as number,
    }));

    return {
      totalJobs: totalJobsData,
      failures: failuresData,
    };
  });
}

export async function getPeriodicJobs(dbUri: string) {
  return withConnection(getPool(dbUri), async (connection) => {
    const [rows] = await connection.query<RowDataPacket[]>(`
      SELECT * FROM mysql_queue_periodic_jobs
      ORDER BY name ASC
    `);

    const dbPeriodicJobs: DbPeriodicJob[] = rows as unknown as DbPeriodicJob[];

    return dbPeriodicJobs.map((row) => ({
      createdAt: row.createdAt.toISOString(),
      cronExpression: row.definition?.cronExpression,
      definition: row.definition,
      lastEnqueuedAt: row.lastEnqueuedAt ? row.lastEnqueuedAt.toISOString() : null,
      name: row.name,
      nextRunAt: row.nextRunAt.toISOString(),
      targetQueue: row.definition?.targetQueue,
      updatedAt: row.updatedAt.toISOString(),
    }));
  });
}

export async function getLeader(dbUri: string) {
  return withConnection(getPool(dbUri), async (connection) => {
    const [rows] = await connection.query<RowDataPacket[]>(`
      SELECT * FROM mysql_queue_leader_election
      ORDER BY electedAt DESC
      LIMIT 1
    `);

    if (rows.length === 0) return null;

    const dbLeader = rows[0] as unknown as DbLeader;

    return {
      leaderId: dbLeader.leaderId,
      electedAt: dbLeader.electedAt.toISOString(),
      expiresAt: dbLeader.expiresAt.toISOString(),
    };
  });
}
