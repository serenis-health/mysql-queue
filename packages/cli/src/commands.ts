import { mysql } from "./deps.ts";

export function Commands() {
  return {
    async listQueues(uri: string) {
      const client = await mysql.createConnection({ uri });

      const [rows] = await client.query<mysql.RowDataPacket[]>(
        "SELECT * FROM mysql_queue_queues",
      );

      console.log(rows.map((r) => {
        const { id, name, ...rest } = r;
        return {
          id,
          name,
          options: JSON.stringify(rest),
        };
      }));
      await client.end();
    },
  };
}
