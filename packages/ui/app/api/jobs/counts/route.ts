import { getConnectionById } from "@/lib/connections";
import { getJobCountsByFilters } from "@/lib/db";
import { NextResponse } from "next/server";
import { withErrorHandling } from "@/lib/api-utils";

export const GET = withErrorHandling(async (req: Request) => {
  const { searchParams } = new URL(req.url);
  const { connectionId, ...params } = Object.fromEntries(searchParams.entries());

  if (!connectionId) {
    return NextResponse.json({ error: "connectionId is required" }, { status: 400 });
  }

  const connection = getConnectionById(connectionId);

  const counts = await getJobCountsByFilters(connection.dbUri, params);
  return NextResponse.json(counts);
});
