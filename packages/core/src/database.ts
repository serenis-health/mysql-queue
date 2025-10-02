import { createPool, PoolConnection, RowDataPacket } from "mysql2/promise";
import { DbAddJobsParams, DbCreateQueueParams, DbUpdateQueueParams, Session } from "./types";
import { Logger } from "./logger";

const TABLES_NAME_PREFIX = "mysql_queue_";

export type Database = ReturnType<typeof Database>;

export function Database(logger: Logger, options: { uri: string; tablesPrefix?: string }) {
  const pool = createPool({ multipleStatements: true, timezone: "Z", uri: options.uri, waitForConnections: true });

  const migrations = [
    {
      down: `DROP TABLE IF EXISTS ${queuesTable()}`,
      name: "create-queues-table",
      number: 1,
      up: `
        CREATE TABLE ${queuesTable()} (
          id CHAR(36) NOT NULL PRIMARY KEY,
          name VARCHAR(50) NOT NULL UNIQUE,
          maxRetries INT UNSIGNED NOT NULL,
          maxDurationMs INT UNSIGNED NOT NULL,
          minDelayMs INT UNSIGNED NOT NULL,
          backoffMultiplier FLOAT NOT NULL,
          INDEX idx_name (name))`,
    },
    {
      down: `DROP TABLE IF EXISTS ${jobsTable()}`,
      name: "create-jobs-table",
      number: 2,
      up: `
      CREATE TABLE ${jobsTable()} (
        id CHAR(36) NOT NULL PRIMARY KEY,
        name VARCHAR(50) NOT NULL,
        payload JSON NOT NULL,
        status ENUM('pending', 'completed', 'failed') NOT NULL,
        startAfter TIMESTAMP(3) NOT NULL,
        createdAt TIMESTAMP(3) NOT NULL,
        completedAt TIMESTAMP(3) NULL,
        failedAt TIMESTAMP(3) NULL,
        latestFailureReason VARCHAR(100) NULL,
        attempts INT DEFAULT 0 NOT NULL,
        priority INT NOT NULL,
        queueId CHAR(36) NOT NULL,
        FOREIGN KEY (queueId) REFERENCES ${queuesTable()}(id) ON DELETE CASCADE,
        INDEX idx_queueId_status_createdAt_priority_id  (queueId, status, createdAt, priority DESC, id ASC)
      )`,
    },
    {
      down: `
        ALTER TABLE ${queuesTable()} DROP INDEX idx_name_partition;
        ALTER TABLE ${queuesTable()} DROP COLUMN partitionKey;
        ALTER TABLE ${queuesTable()} ADD UNIQUE INDEX name (name);
      `,
      name: "add-partition-key",
      number: 3,
      up: `
        ALTER TABLE ${queuesTable()} ADD COLUMN partitionKey VARCHAR(50) NOT NULL DEFAULT 'default';
        ALTER TABLE ${queuesTable()} DROP INDEX name;
        ALTER TABLE ${queuesTable()} ADD UNIQUE INDEX idx_name_partition (name, partitionKey);
      `,
    },
    {
      down: `
        ALTER TABLE ${jobsTable()} DROP INDEX idx_queue_name_idempotent;
        ALTER TABLE ${jobsTable()} DROP COLUMN idempotentKey;
      `,
      name: "add-idempotent-key",
      number: 4,
      up: `
        ALTER TABLE ${jobsTable()} ADD COLUMN idempotentKey VARCHAR(255) NULL;
        ALTER TABLE ${jobsTable()} ADD UNIQUE INDEX idx_queue_name_idempotent (queueId, name, idempotentKey);
      `,
    },
    {
      down: `
        ALTER TABLE ${jobsTable()} DROP INDEX idx_queue_name_pending_dedup;
        ALTER TABLE ${jobsTable()} DROP COLUMN pendingDedupKey;
      `,
      name: "add-pending-dedup-key",
      number: 5,
      up: `
        ALTER TABLE ${jobsTable()} ADD COLUMN pendingDedupKey VARCHAR(255) NULL;
        ALTER TABLE ${jobsTable()} ADD UNIQUE INDEX idx_queue_name_pending_dedup (queueId, name, pendingDedupKey, status);
      `,
    },
    {
      down: `
        ALTER TABLE ${queuesTable()} DROP COLUMN paused;
      `,
      name: "add-paused-column",
      number: 6,
      up: `
        ALTER TABLE ${queuesTable()} ADD COLUMN paused BOOLEAN NOT NULL DEFAULT FALSE;
      `,
    },
  ];

  async function runWithPoolConnection<T>(cb: (connection: PoolConnection) => Promise<T>) {
    const connection = await pool.getConnection();
    try {
      return await cb(connection);
    } finally {
      pool.releaseConnection(connection);
    }
  }

  return {
    async addJobs(queueName: string, params: DbAddJobsParams, partitionKey: string, session?: Session) {
      if (params.length === 0) return;
      const values = [
        ...params.flatMap((j) => [
          j.id,
          j.name,
          j.payload,
          j.status,
          j.priority,
          j.startAfter,
          j.createdAt,
          j.idempotentKey,
          j.pendingDedupKey,
        ]),
        queueName,
        partitionKey,
      ];

      const sql = `
          INSERT INTO ${jobsTable()} (id, name, payload, status, priority, startAfter, createdAt, idempotentKey, pendingDedupKey, queueId)
          SELECT j.*, q.id FROM (SELECT ? AS id, ? AS name, ? AS payload, ? AS status, ? AS priority, ? AS startAfter, ? AS createdAt, ? AS idempotentKey, ? AS pendingDedupKey ${params
            .slice(1)
            .map(() => "UNION ALL SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?")
            .join(" ")}) AS j
          JOIN ${queuesTable()} q ON q.name = ? AND q.partitionKey = ?
        `;

      let result: object[];
      try {
        result = session ? await session.execute(sql, values) : await runWithPoolConnection((c) => c.query(sql, values));
      } catch (e) {
        if (isMysqlError(e) && e.code === "ER_DUP_ENTRY") return;
        throw e;
      }

      if (!Array.isArray(result)) throw new Error("Session did not return an array");
      if (result.length === 0) throw new Error("Session returned an empty array");
      if (!("affectedRows" in result[0])) throw new Error("Session did not return affected rows");
      if (result[0].affectedRows === 0) throw new Error("Unable to add jobs, maybe queue does not exist");
      return result[0].affectedRows;
    },
    async countJobs(queueName: string, partitionKey: string) {
      const [rows] = await runWithPoolConnection((connection) =>
        connection.query<RowDataPacket[]>(
          `SELECT COUNT(*) as count FROM ${jobsTable()} j 
           JOIN ${queuesTable()} q ON j.queueId = q.id 
           WHERE q.name = ? AND q.partitionKey = ?`,
          [queueName, partitionKey],
        ),
      );
      return rows.length ? (rows[0] as { count: number }).count : 0;
    },
    async createQueue(params: DbCreateQueueParams) {
      await runWithPoolConnection((connection) => {
        return connection.query(
          `INSERT INTO ${queuesTable()} (id, name, maxRetries, minDelayMs, backoffMultiplier, maxDurationMs, partitionKey, paused) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
          [
            params.id,
            params.name,
            params.maxRetries,
            params.minDelayMs,
            params.backoffMultiplier,
            params.maxDurationMs,
            params.partitionKey,
            params.paused,
          ],
        );
      });
    },
    async deleteQueuesByPartition(partitionKey: string) {
      await runWithPoolConnection(async (connection) => {
        await connection.beginTransaction();
        try {
          // NOTE jobs will be automatically deleted via CASCADE constraint
          await connection.query(`DELETE FROM ${queuesTable()} WHERE partitionKey = ?`, [partitionKey]);
          await connection.commit();
        } catch (error) {
          await connection.rollback();
          throw error;
        }
      });
    },
    async endPool() {
      await pool.end();
    },
    async getJobById(connection: PoolConnection, jobId: string) {
      const [rows] = await connection.query<RowDataPacket[]>(`SELECT * FROM ${jobsTable()} WHERE id = ?`, [jobId]);
      return rows.length ? rows[0] : null;
    },
    async getPendingJobs(connection: PoolConnection, queueId: string, batchSize: number) {
      const [rows] = await connection.query<RowDataPacket[]>(
        `SELECT * FROM ${jobsTable()} FORCE INDEX (idx_queueId_status_createdAt_priority_id) WHERE queueId = ? AND status = ? AND startAfter <= ? ORDER BY createdAt ASC, priority DESC LIMIT ? FOR UPDATE SKIP LOCKED`,
        [queueId, "pending", new Date(), batchSize],
      );
      return rows;
    },
    async getQueueByName(name: string, partitionKey: string) {
      const [rows] = await runWithPoolConnection((connection) =>
        connection.query<RowDataPacket[]>(`SELECT * FROM ${queuesTable()} WHERE name = ? AND partitionKey = ?`, [name, partitionKey]),
      );
      return rows.length ? rows[0] : null;
    },
    async getQueueIdByName(name: string, partitionKey?: string) {
      const [rows] = await runWithPoolConnection((connection) =>
        connection.query<RowDataPacket[]>(`SELECT id FROM ${queuesTable()} WHERE name = ? AND partitionKey = ?`, [name, partitionKey]),
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
    async isQueuePaused(queueId: string) {
      const [rows] = await runWithPoolConnection((connection) =>
        connection.query<RowDataPacket[]>(`SELECT paused FROM ${queuesTable()} WHERE id = ?`, [queueId]),
      );
      return rows.length ? (rows[0] as { paused: boolean }).paused : false;
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
    async pauseQueue(queueName: string, partitionKey: string) {
      await runWithPoolConnection((connection) => {
        return connection.query(`UPDATE ${queuesTable()} SET paused = TRUE WHERE name = ? AND partitionKey = ?`, [queueName, partitionKey]);
      });
    },
    queuesTable,

    async removeAllTables() {
      const connection = await pool.getConnection();
      await connection.beginTransaction();
      try {
        const [rows] = await connection.query<RowDataPacket[]>(`SELECT name FROM ${migrationsTable()}`);
        const appliedMigrations = new Set(rows.map((row) => row.name));
        for (const migration of [...migrations].reverse()) {
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
    async resumeQueue(queueName: string, partitionKey: string) {
      await runWithPoolConnection((connection) => {
        return connection.query(`UPDATE ${queuesTable()} SET paused = FALSE WHERE name = ? AND partitionKey = ?`, [
          queueName,
          partitionKey,
        ]);
      });
    },
    async runMigrations() {
      const lockName = `mysql_queue_migrations_${options.tablesPrefix || "default"}`;
      const lockTimeout = 10;

      const connection = await pool.getConnection();
      try {
        const [lockResult] = await connection.query<RowDataPacket[]>("SELECT GET_LOCK(?, ?) as lock_acquired", [lockName, lockTimeout]);
        if (!lockResult[0]?.lock_acquired) {
          logger.info(`Migrations skipped - another instance is currently running migrations`);
          return;
        }

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
          throw error;
        }
      } catch (error) {
        logger.error("Migration up failed", error);
        throw error;
      } finally {
        await connection.query("SELECT RELEASE_LOCK(?)", [lockName]);
        pool.releaseConnection(connection);
      }
      logger.info("Migrations completed");
    },
    runWithPoolConnection,
    async updateQueue(params: DbUpdateQueueParams) {
      await runWithPoolConnection((connection) => {
        return connection.query(
          `UPDATE ${queuesTable()} SET maxRetries = ?, minDelayMs = ?, backoffMultiplier = ?, maxDurationMs = ?, partitionKey = ?, paused = ? WHERE id = ?`,
          [
            params.maxRetries,
            params.minDelayMs,
            params.backoffMultiplier,
            params.maxDurationMs,
            params.partitionKey || null,
            params.paused,
            params.id,
          ],
        );
      });
    },
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

  function isMysqlError(e: unknown): e is { code: string; errno: number } {
    return typeof e === "object" && e !== null && "code" in e && "errno" in e;
  }
}
