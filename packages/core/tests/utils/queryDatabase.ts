import mysql, { QueryResult } from "mysql2/promise";

export function QueryDatabase(params: { dbUri: string }) {
  const pool = mysql.createPool({
    timezone: "Z",
    uri: params.dbUri,
    waitForConnections: true,
  });

  return {
    async dispose() {
      await pool.end();
    },
    pool,
    async query<T extends QueryResult>(sql: string, parameters?: unknown[]) {
      const connection = await pool.getConnection();
      try {
        const [rows] = await connection.query<T>(sql, parameters);
        return rows;
      } finally {
        connection.release();
      }
    },
  };
}
