import { getConnectionById } from "@/lib/connections";
import { NextResponse } from "next/server";
import { restartJob } from "@/lib/db";
import { withErrorHandling } from "@/lib/api-utils";

export const POST = withErrorHandling(async (req: Request) => {
  const { connectionId, jobId } = await req.json();

  if (!connectionId || !jobId) {
    return NextResponse.json({ error: "connectionId and jobId are required" }, { status: 400 });
  }

  const connection = getConnectionById(connectionId);

  const restarted = await restartJob(connection.dbUri, jobId);
  if (!restarted) {
    return NextResponse.json({ error: "Only failed or completed jobs can be restarted" }, { status: 409 });
  }

  return NextResponse.json({ success: true });
});
