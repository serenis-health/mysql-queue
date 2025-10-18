import { getConnectionById } from "@/lib/connections";
import { getPeriodicJobs } from "@/lib/db";
import { NextResponse } from "next/server";
import { withErrorHandling } from "@/lib/api-utils";

export const GET = withErrorHandling(async (req: Request) => {
  const { searchParams } = new URL(req.url);
  const connectionId = searchParams.get("connectionId");

  if (!connectionId) {
    return NextResponse.json({ error: "connectionId is required" }, { status: 400 });
  }

  const connection = getConnectionById(connectionId);
  const periodicJobs = await getPeriodicJobs(connection.dbUri);
  return NextResponse.json(periodicJobs);
});
