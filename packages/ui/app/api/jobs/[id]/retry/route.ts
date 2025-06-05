import { type NextRequest, NextResponse } from "next/server";
import { getConnection } from "@/lib/db";

export async function POST(request: NextRequest, { params }: { params: { id: string } }) {
  return NextResponse.json({ error: "Not implemented" }, { status: 501 });
  // const jobId = params.id

  // try {
  //   const connection = await getConnection()

  //   // Aggiorna lo stato del lavoro a "waiting" e resetta i campi pertinenti
  //   await connection.execute(
  //     `UPDATE mysql_queue_jobs
  //      SET status = 'waiting',
  //          failedAt = NULL,
  //          latestFailureReason = NULL
  //      WHERE id = ?`,
  //     [jobId],
  //   )

  //   await connection.end()

  //   return NextResponse.json({ success: true })
  // } catch (error) {
  //   console.error("Error retrying job:", error)
  //   return NextResponse.json({ error: "Failed to retry job" }, { status: 500 })
  // }
}
