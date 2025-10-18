import { getConnectionById } from "@/lib/connections";
import { getDashboardStats } from "@/lib/db";
import { NextResponse } from "next/server";
import { withErrorHandling } from "@/lib/api-utils";

export const GET = withErrorHandling(async (req: Request) => {
  const { searchParams } = new URL(req.url);
  const connectionId = searchParams.get("connectionId");

  if (!connectionId) {
    return NextResponse.json({ error: "connectionId is required" }, { status: 400 });
  }

  const connection = getConnectionById(connectionId);
  const days = parseInt(searchParams.get("days") || "14", 10);
  const stats = await getDashboardStats(connection.dbUri, { days });
  return NextResponse.json(stats);
});
