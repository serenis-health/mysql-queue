import { env } from "@/lib/env";
import { NextResponse } from "next/server";
import { withErrorHandling } from "@/lib/api-utils";

export const GET = withErrorHandling(async (_req: Request) => {
  const connections = env.CONNECTIONS;
  return NextResponse.json(connections.map((c) => ({ id: c.id, label: c.label })));
});
