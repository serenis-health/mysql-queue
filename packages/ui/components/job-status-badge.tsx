import { IconCalendarFilled, IconCircleCheckFilled, IconCircleXFilled, IconLoader, IconProgress } from "@tabler/icons-react";
import { Badge } from "@/components/ui/badge";
import { Job } from "@/types/job";
import { LiveTime } from "@/components/live-time";

export function JobStatusBadge({ job }: { job: Job }) {
  function getStatusIcon(status: Job["status"]) {
    switch (status) {
      case "completed":
        return <IconCircleCheckFilled className="fill-green-500 dark:fill-green-400" />;
      case "pending":
        return <IconLoader className="animate-spin" />;
      case "failed":
        return <IconCircleXFilled className="fill-red-500 dark:fill-red-400 w-6 h-6" />;
      case "running":
        return <IconProgress className="animate-spin text-blue-500" />;
      case "scheduled":
        return <IconCalendarFilled className="text-yellow-500" />;
    }
  }

  function getTime(job: Job) {
    switch (job.status) {
      case "completed":
        return job.completedAt!;
      case "pending":
        return job.startAfter!;
      case "failed":
        return job.failedAt!;
      case "running":
        return job.runningAt!;
      case "scheduled":
        return job.startAfter!;
      default:
        throw new Error("Status not handled");
    }
  }

  return (
    <Badge variant="outline" className="text-muted-foreground px-1.5">
      {getStatusIcon(job.status)}
      {job.status}
      <LiveTime date={new Date(getTime(job))} />
    </Badge>
  );
}
