import mysql, { RowDataPacket } from "mysql2/promise";

export async function getConnection() {
  return await mysql.createConnection({
    uri: process.env.DB_URI,
  });
}

export async function getQueues(): Promise<Queue[]> {
  const connection = await getConnection();
  try {
    const [rows] = await connection.execute<RowDataPacket[]>(`
      SELECT q.id, q.name, COUNT(j.id) as count
      FROM mysql_queue_queues q
      LEFT JOIN mysql_queue_jobs j ON q.id = j.queueId
      GROUP BY q.id, q.name
      ORDER BY q.name
    `);
    const dbQueues: DbQueue[] = rows as unknown as DbQueue[];

    return dbQueues.map((q) => ({
      ...q,
      jobsCount: q.count,
    }));
  } finally {
    await connection.end();
  }
}

export async function getJobs(params: GetJobsParams): Promise<Job[]> {
  const connection = await getConnection();
  try {
    let query = `
      SELECT j.*, q.name as queueName
      FROM mysql_queue_jobs j
      LEFT JOIN mysql_queue_queues q ON j.queueId = q.id
      WHERE 1=1
    `;

    const filters: string[] = [];

    if (params.queueId) {
      query += ` AND j.queueId = ?`;
      filters.push(params.queueId);
    }

    if (params.status) {
      query += ` AND j.status = ?`;
      filters.push(params.status);
    }

    if (params.searchQuery) {
      query += ` AND j.name LIKE ?`;
      filters.push(`%${params.searchQuery}%`);
    }

    query += ` ORDER BY j.createdAt DESC LIMIT 100`;

    const [rows] = await connection.execute(query, filters);
    const dbJobs = rows as unknown as DbJobWithQueue[];

    const jobs: Job[] = dbJobs.map((j) => ({
      ...j,
      completedAt: j.completedAt ? j.completedAt.toISOString() : null,
      createdAt: j.createdAt.toISOString(),
      durationMs: j.completedAt
        ? j.completedAt.getTime() - j.createdAt.getTime()
        : j.failedAt
          ? j.failedAt.getTime() - j.createdAt.getTime()
          : null,
      failedAt: j.failedAt ? j.failedAt.toISOString() : null,
      scheduledFor: j.startAfter.getTime() !== j.createdAt.getTime() ? j.startAfter.toISOString() : null,
      status: j.status === "pending" && j.startAfter !== j.createdAt ? "scheduled" : j.status,
    }));

    return jobs;
  } finally {
    await connection.end();
  }
}

export async function getJobById(id: string): Promise<DbJobWithQueue | null> {
  const connection = await getConnection();
  try {
    const [rows] = await connection.execute<RowDataPacket[]>(
      `SELECT j.*, q.name as queueName
       FROM mysql_queue_jobs j
       LEFT JOIN mysql_queue_queues q ON j.queueId = q.id
       WHERE j.id = ?`,
      [id],
    );

    if (rows.length === 0) return null;
    return rows[0] as unknown as DbJobWithQueue;
  } finally {
    await connection.end();
  }
}

export async function getJobCounts() {
  const connection = await getConnection();
  try {
    const [rows] = await connection.execute(`
      SELECT status, COUNT(*) as count
      FROM mysql_queue_jobs
      GROUP BY status
    `);

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const counts = (rows as any[]).reduce(
      (acc, row) => {
        acc[row.status] = row.count;
        return acc;
      },
      {} as Record<string, number>,
    );

    return counts;
  } finally {
    await connection.end();
  }
}

type DbJob = {
  attempts: number | null;
  completedAt: Date | null;
  createdAt: Date;
  failedAt: Date | null;
  id: string;
  latestFailureReason: string | null;
  name: string;
  payload: unknown;
  priority: number;
  queueId: string;
  startAfter: Date;
  status: "pending" | "completed" | "failed";

  queueName: string;
};

export interface DbQueue {
  backoffMultiplier: number | null;
  id: string;
  maxDurationMs: number;
  maxRetries: number;
  minDelayMs: number;
  name: string;

  count: number;
}

type DbJobWithQueue = DbJob & {
  queueName: string;
};

type GetJobsParams = {
  queueId?: string;
  status?: string;
  searchQuery?: string;
};

export type Job = Omit<DbJob, "status" | "queueName" | "completedAt" | "createdAt" | "failedAt" | "startAfter"> & {
  status: "pending" | "completed" | "failed" | "scheduled";
  durationMs: number | null;
  completedAt: string | null;
  createdAt: string;
  failedAt: string | null;
  scheduledFor: string | null;
  queueName: string;
};
export type Queue = DbQueue & { jobsCount: number };
