import { getJobs } from "@/lib/db";
import { NextResponse } from "next/server";

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);
  const queueId = searchParams.get("queueId") ?? undefined;

  const jobs = await getJobs({ queueId });
  return NextResponse.json(jobs);
}
