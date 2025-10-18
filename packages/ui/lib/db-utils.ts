import { Pool, PoolConnection } from "mysql2/promise";

export function parsePagination(params?: { limit?: string; offset?: string }) {
  let limit = params?.limit ? parseInt(params.limit, 10) : 50;
  let offset = params?.offset ? parseInt(params.offset, 10) : 0;

  if (isNaN(limit) || limit < 0 || !isFinite(limit)) limit = 50;
  if (isNaN(offset) || offset < 0 || !isFinite(offset)) offset = 0;
  limit = Math.min(limit, 1000);
  offset = Math.floor(offset);
  limit = Math.floor(limit);

  return { limit, offset };
}

export function normalizeToArray<T>(value: T | T[] | undefined): T[] {
  if (!value) return [];
  return Array.isArray(value) ? value : [value];
}

export function buildScheduledStatusFilter(statuses: string[]) {
  const hasScheduled = statuses.includes("scheduled");
  const dbStatuses = statuses.filter((s) => s !== "scheduled");

  if (hasScheduled && dbStatuses.length > 0) {
    return {
      clause: ` AND (j.status IN (${dbStatuses.map(() => "?").join(",")}) OR (j.status = 'pending' AND j.startAfter != j.createdAt))`,
      params: dbStatuses,
    };
  } else if (hasScheduled) {
    return {
      clause: ` AND j.status = 'pending' AND j.startAfter != j.createdAt`,
      params: [],
    };
  } else {
    return {
      clause: ` AND j.status IN (${dbStatuses.map(() => "?").join(",")})`,
      params: dbStatuses,
    };
  }
}

export async function withConnection<T>(pool: Pool, callback: (connection: PoolConnection) => Promise<T>): Promise<T> {
  const connection = await pool.getConnection();
  try {
    return await callback(connection);
  } finally {
    connection.release();
  }
}
