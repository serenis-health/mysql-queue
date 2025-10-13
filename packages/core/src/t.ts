import mysql, { PoolConnection } from "mysql2/promise";
import { MysqlQueue, Session } from "./index";

(async () => {
  const instance = MysqlQueue({
    dbUri: "mysql://root:password@localhost:3306/serenis",
  });
  const pool = mysql.createPool({
    uri: "mysql://root:password@localhost:3306/serenis",
    waitForConnections: true,
  });
  const connection = await pool.getConnection();
  const session = createSessionWrapper(connection);

  await instance.globalInitialize();

  await instance.upsertQueue("q");
  await instance.enqueue("q", { name: "test", payload: {} });
  const w = await instance.work("q", async ([j], s, ctx) => {
    await connection.beginTransaction();
    console.log("Processing", j);
    await new Promise((r) => setTimeout(r, 2000));
    //throw new Error("Foo");
    await ctx.markJobsAsCompleted(session);
    await connection.commit();
  });
  void w.start();
})();

function createSessionWrapper(connection: PoolConnection): Session {
  return {
    execute: async (sql: string, parameters: unknown[]) => {
      const [result] = await connection.query(sql, parameters);
      return [result as { affectedRows: number }];
    },
    query: async <TRow = unknown>(sql: string, parameters: unknown[]) => {
      const [rows] = await connection.query(sql, parameters);
      return rows as TRow[];
    },
  };
}
