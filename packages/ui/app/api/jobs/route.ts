import { type NextRequest, NextResponse } from "next/server"
import { getJobs } from "@/lib/db"

export async function GET(request: NextRequest) {
  const searchParams = request.nextUrl.searchParams
  const queueId = searchParams.get("queueId") || undefined
  const status = searchParams.get("status") || undefined
  const searchQuery = searchParams.get("search") || undefined

  try {
    const jobs = await getJobs({ queueId, status, searchQuery })

    // Formatta i dati per il client
    const formattedJobs = (jobs as any[]).map((job) => ({
      id: job.id,
      name: job.name,
      queue: job.queueId,
      queueName: job.queueName,
      status: job.status,
      created: job.createdAt,
      attempts: job.attempts,
      data: JSON.parse(job.payload),
      startAfter: job.startAfter,
      completedAt: job.completedAt,
      failedAt: job.failedAt,
      latestFailureReason: job.latestFailureReason,
      priority: job.priority,
    }))

    return NextResponse.json(formattedJobs)
  } catch (error) {
    console.error("Error fetching jobs:", error)
    return NextResponse.json({ error: "Failed to fetch jobs" }, { status: 500 })
  }
}
