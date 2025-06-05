// app/jobs/page.tsx or similar
import { Suspense } from "react";
import { getQueues, getJobs, getJobCounts } from "@/lib/db";
import { Skeleton } from "@/components/ui/skeleton";
import JobsSystemClient from "@/components/jobs-system-client"; // Assume this is where the client component lives

export default async function JobsSystemPage() {
  const queues = (await getQueues()) as any[];
  const jobs = await getJobs({});
  const jobCounts = await getJobCounts();

  const totalJobs = Object.values(jobCounts).reduce((sum, count) => sum + count, 0);

  const formattedQueues = queues.map((queue) => ({
    id: queue.id,
    name: queue.name,
    count: queue.count,
  }));

  return (
    <Suspense
      fallback={
        <div className="flex h-screen items-center justify-center">
          <Skeleton className="h-[500px] w-[900px]" />
        </div>
      }
    >
      <JobsSystemClient queues={formattedQueues} jobs={jobs} jobCounts={jobCounts} totalJobs={totalJobs} />
    </Suspense>
  );
}
