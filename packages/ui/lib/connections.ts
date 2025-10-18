import { Connection } from "@/types/connection";
import { env } from "@/lib/env";

export function getConnectionById(connectionId: string): Connection {
  const connection = env.CONNECTIONS.find((c) => c.id === connectionId);
  if (!connection) throw new Error(`Connection with id "${connectionId}" not found`);
  return connection;
}
