import { Connection, createPool, PoolConnection, ResultSetHeader, RowDataPacket } from "mysql2/promise";
import { DbAddJobsParams, DbCreateQueueParams, DbUpdateQueueParams } from "./types";
import { Logger } from "./logger";

const TABLES_NAME_PREFIX = "mysql_queue_";

export type Database = ReturnType<typeof Database>;

export function Database(logger: Logger, options: { uri: string; tablesPrefix?: string }) {
  const pool = createPool({ uri: options.uri, waitForConnections: true });

  const migrations = [
    {
      down: `DROP TABLE IF EXISTS ${queuesTable()}`,
      name: "create-queues-table",
      number: 1,
      up: `
        CREATE TABLE IF NOT EXISTS ${queuesTable()} (
          id CHAR(36) NOT NULL PRIMARY KEY,
          name VARCHAR(50) NOT NULL UNIQUE,
          maxRetries INT UNSIGNED NOT NULL,
          maxDurationMs INT UNSIGNED NOT NULL,
          minDelayMs INT UNSIGNED NOT NULL,
          backoffMultiplier FLOAT,
          INDEX idx_name (name))`,
    },
    {
      down: `DROP TABLE IF EXISTS ${jobsTable()}`,
      name: "create-jobs-table",
      number: 2,
      up: `
      CREATE TABLE IF NOT EXISTS ${jobsTable()} (
        id CHAR(36) NOT NULL PRIMARY KEY,
        name VARCHAR(50) NOT NULL,
        payload JSON NOT NULL,
        status VARCHAR(50) NOT NULL,
        startAfter TIMESTAMP,
        createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        completedAt TIMESTAMP NULL,
        failedAt TIMESTAMP NULL,
        latestFailureReason VARCHAR(100) NULL,
        attempts INT DEFAULT 0 NULL,
        priority INT NOT NULL,
        queueId CHAR(36) NULL,
        INDEX idx_queueId_status_startAfter_priority_createdAt  (queueId, status, startAfter, priority, createdAt)
      )`,
    },
  ];

  async function withConnection<T>(cb: (connection: PoolConnection) => Promise<T>) {
    const connection = await pool.getConnection();
    try {
      return await cb(connection);
    } finally {
      pool.releaseConnection(connection);
    }
  }

  return {
    async addJobs(queueName: string, params: Omit<DbAddJobsParams, "queueId">[], connection?: Connection) {
      if (params.length === 0) return;

      async function query(conn: Connection) {
        const values = params.flatMap((job) => [job.id, job.name, job.payload, job.status, job.priority, job.startAfter]);

        const [{ affectedRows }] = await conn.query<ResultSetHeader>(
          `INSERT INTO ${jobsTable()} (id, name, payload, status, priority, startAfter, queueId)
           SELECT j.*, q.id
           FROM (SELECT ? AS id, ? AS name, ? AS payload, ? AS status, ? AS priority, ? AS startAfter ${params
             .slice(1)
             .map(() => "UNION ALL SELECT ?, ?, ?, ?, ?, ?")
             .join(" ")}) AS j
          JOIN ${queuesTable()} q ON q.name = ?`,
          [...values, queueName],
        );
        if (affectedRows === 0) throw new Error("Failed to add jobs, maybe queue does not exist");
      }

      await (connection ? query(connection) : withConnection(query));
    },
    async createQueue(params: DbCreateQueueParams) {
      await withConnection((connection) => {
        return connection.query(
          `INSERT INTO ${queuesTable()} (id, name, maxRetries, minDelayMs, backoffMultiplier, maxDurationMs) VALUES (?, ?, ?, ?, ?, ?)`,
          [params.id, params.name, params.maxRetries, params.minDelayMs, params.backoffMultiplier, params.maxDurationMs],
        );
      });
    },
    async endPool() {
      await pool.end();
    },
    async getJobById(jobId: string) {
      const [rows] = await withConnection((connection) =>
        connection.query<RowDataPacket[]>(`SELECT * FROM ${jobsTable()} WHERE id = ?`, [jobId]),
      );
      return rows.length ? rows[0] : null;
    },
    async getPendingJobs(queueId: string, batchSize: number) {
      const [rows] = await withConnection((connection) =>
        connection.query<RowDataPacket[]>(
          `SELECT * FROM ${jobsTable()} WHERE status = ? AND queueId = ? AND (startAfter IS NULL OR startAfter <= ?) ORDER BY priority ASC, createdAt ASC LIMIT ? FOR UPDATE SKIP LOCKED`,
          ["pending", queueId, new Date(), batchSize],
        ),
      );
      return rows;
    },
    async getQueueByName(name: string) {
      const [rows] = await withConnection((connection) =>
        connection.query<RowDataPacket[]>(`SELECT * FROM ${queuesTable()} WHERE name = ?`, [name]),
      );
      return rows.length ? rows[0] : null;
    },
    async getQueueIdByName(name: string) {
      const [rows] = await withConnection((connection) =>
        connection.query<RowDataPacket[]>(`SELECT id FROM ${queuesTable()} WHERE name = ?`, [name]),
      );
      return rows.length ? (rows[0] as { id: string }) : null;
    },
    async incrementJobAttempts(connection: PoolConnection, jobId: string, error: string, currentAttempts: number, startAfter: Date) {
      await connection.execute(`UPDATE ${jobsTable()} SET attempts = ?, latestFailureReason = ?, startAfter = ? WHERE id = ?`, [
        currentAttempts + 1,
        error,
        startAfter,
        jobId,
      ]);
    },
    jobsTable,
    async markJobAsCompleted(connection: PoolConnection, jobId: string, currentAttempts: number) {
      await connection.execute(`UPDATE ${jobsTable()} SET attempts = ?, status = ?, completedAt = ? WHERE id = ?`, [
        currentAttempts + 1,
        "completed",
        new Date(),
        jobId,
      ]);
    },
    async markJobAsFailed(connection: PoolConnection, jobId: string, error: string, currentAttempts: number) {
      await connection.execute(`UPDATE ${jobsTable()} SET attempts = ?, status = ?, failedAt = ?, latestFailureReason = ? WHERE id = ?`, [
        currentAttempts + 1,
        "failed",
        new Date(),
        error,
        jobId,
      ]);
    },
    migrationsTable,
    queuesTable,
    async removeAllTables() {
      const connection = await pool.getConnection();
      await connection.beginTransaction();
      try {
        const [rows] = await connection.query<RowDataPacket[]>(`SELECT name FROM ${migrationsTable()}`);
        const appliedMigrations = new Set(rows.map((row) => row.name));
        for (const migration of migrations) {
          if (appliedMigrations.has(migration.name)) {
            await connection.query(migration.down);
            logger.debug(`Applied down migration ${migration.name}`);
          }
        }

        await connection.query(`DROP TABLE IF EXISTS ${migrationsTable()}`);

        await connection.commit();
      } catch (error) {
        await connection.rollback();
        logger.error("Migration down failed", error);
        throw error;
      } finally {
        pool.releaseConnection(connection);
      }
    },
    async runMigrations() {
      const connection = await pool.getConnection();
      await connection.beginTransaction();
      try {
        const [migrationTableRows] = await connection.query<RowDataPacket[]>(`SHOW TABLES like '${migrationsTable()}'`);
        if (!migrationTableRows.length) {
          await connection.query(`
          CREATE TABLE IF NOT EXISTS ${migrationsTable()} (
          id INT AUTO_INCREMENT PRIMARY KEY,
          name VARCHAR(255) NOT NULL UNIQUE,
          applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          INDEX idx_name (name)
        )`);
        }

        const [appliedMigrationsRows] = await connection.query<RowDataPacket[]>(`SELECT name FROM ${migrationsTable()}`);
        const appliedMigrations = new Set(appliedMigrationsRows.map((row) => row.name));

        for (const migration of migrations) {
          if (!appliedMigrations.has(migration.name)) {
            await connection.query(migration.up);
            await connection.query(`INSERT INTO ${migrationsTable()} (name) VALUES (?)`, [migration.name]);
            logger.debug(`Applied up migration ${migration.name}`);
          }
        }

        await connection.commit();
      } catch (error) {
        await connection.rollback();
        logger.error("Migration up failed", error);
        throw error;
      } finally {
        pool.releaseConnection(connection);
      }
      logger.info("Migrations completed");
    },
    async updateQueue(params: DbUpdateQueueParams) {
      await withConnection((connection) => {
        return connection.query(
          `UPDATE ${queuesTable()} SET maxRetries = ?, minDelayMs = ?, backoffMultiplier = ?, maxDurationMs = ? WHERE id = ?`,
          [params.maxRetries, params.minDelayMs, params.backoffMultiplier, params.maxDurationMs, params.id],
        );
      });
    },
    withConnection,
  };

  function migrationsTable() {
    return TABLES_NAME_PREFIX + (options.tablesPrefix || "") + "migrations";
  }
  function queuesTable() {
    return TABLES_NAME_PREFIX + (options.tablesPrefix || "") + "queues";
  }
  function jobsTable() {
    return TABLES_NAME_PREFIX + (options.tablesPrefix || "") + "jobs";
  }
}
