"use client";

import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import {
  IconBrandGithub,
  IconCalendarClock,
  IconCircleCheckFilled,
  IconCopy,
  IconExclamationCircleFilled,
  IconRecharging,
  IconStopwatch,
} from "@tabler/icons-react";
import { Job, Queue } from "@/lib/db";
import { useEffect, useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { formatDistanceToNowStrict } from "date-fns";
import { toast } from "sonner";

export default function Dashboard({ initialJobs, queues }: { initialJobs: Job[]; queues: Queue[] }) {
  const [jobs, setJobs] = useState<Job[]>(initialJobs);
  const [selectedQueue, setSelectedQueue] = useState<Queue | null>(null);

  async function fetchJobs(queueId: string) {
    const res = await fetch(`/api/jobs${queueId ? `?queueId=${queueId}` : ""}`);
    setJobs(await res.json());
  }

  useEffect(() => {
    if (selectedQueue) {
      fetchJobs(selectedQueue.id);
    }
  }, [selectedQueue]);

  const [selectedJob, setSelectedJob] = useState<Job | null>(null);

  function copyJobId(job: Job) {
    navigator.clipboard.writeText(job.id);
    toast("id copied", { icon: <IconCopy /> });
  }

  function copyJobPayload(job: Job) {
    navigator.clipboard.writeText(JSON.stringify(job.payload, null, 2));
    toast("payload copied", { icon: <IconCopy /> });
  }

  function getStatusBadge(status: Job["status"]) {
    switch (status) {
      case "failed":
        return (
          <Badge className="bg-red-300">
            <IconExclamationCircleFilled />
            failed
          </Badge>
        );
      case "pending":
        return (
          <Badge className="bg-yellow-300">
            <IconStopwatch />
            pending
          </Badge>
        );
      case "scheduled":
        return (
          <Badge className="bg-indigo-300">
            <IconCalendarClock />
            scheduled
          </Badge>
        );
      case "completed":
        return (
          <Badge className="bg-green-300">
            <IconCircleCheckFilled />
            completed
          </Badge>
        );
    }
  }

  return (
    <div className="flex h-screen">
      <div className="w-64 p-2 bg-background space-y-2 overflow-y-auto flex flex-col">
        <h2 className="text-xl font-bold tracking-tight border-b pb-2 mb-4">Queues</h2>
        {queues.map((queue) => (
          <Card
            key={queue.name}
            className="px-4 py-2 shadow-sm border-muted bg-background cursor-pointer hover:bg-muted transition"
            onClick={() => setSelectedQueue(queue)}
          >
            <CardContent className="p-0 flex items-center justify-between">
              <span className="text-sm font-medium">{queue.name}</span>
              <Badge variant="outline" className="text-xs px-2 py-0.5 rounded-full">
                {queue.jobsCount}
              </Badge>
            </CardContent>
          </Card>
        ))}
        <div className="mt-auto pt-4 mb-4 ">
          <div className="mt-3 text-xs text-muted-foreground select-text">
            <Button variant="ghost" size="sm" onClick={() => window.open("https://github.com/serenis-health/mysql-queue", "_blank")}>
              <IconBrandGithub className="h-4 w-4" />
              github
            </Button>
          </div>
        </div>
      </div>
      <div className="flex-1 p-4 bg-background space-y-4 overflow-y-auto">
        {jobs.map((job) => {
          return (
            <Card key={job.id} className="py-4 min-h-[102px] cursor-pointer hover:bg-muted transition" onClick={() => setSelectedJob(job)}>
              <CardHeader className="flex justify-between items-center">
                <div>
                  <div className="font-semibold">{job.name}</div>
                  <div className="text-sm text-muted-foreground">
                    <span className="font-medium">{job.queueName}</span> • <span className="font-mono">{job.id}</span>
                  </div>
                  <div className="text-xs text-muted-foreground mt-4">
                    {job.createdAt} ({formatDistanceToNowStrict(job.createdAt, { addSuffix: true })}) • {job.attempts} attempts • completed
                    in {job.durationMs} ms
                  </div>
                </div>
                {getStatusBadge(job.status)}
              </CardHeader>
            </Card>
          );
        })}
      </div>
      <div className="flex-1 p-4 bg-background text-foreground flex flex-col">
        <h2 className="text-xl font-bold tracking-tight border-b border-border pb-2 mb-4">Job Detail</h2>
        {selectedJob ? (
          <div className="flex flex-col h-full  overflow-y-auto space-y-4">
            <div>
              <div className="font-semibold mb-1">Name</div>
              <div>{selectedJob.name}</div>
            </div>

            <div>
              <div className="font-semibold mb-1">Id</div>
              <div>{selectedJob.id}</div>
            </div>

            <div className="flex space-x-2">
              <div className="flex-1">
                <div className="font-semibold mb-1">Queue</div>
                <div>{selectedJob.queueName}</div>
              </div>
              <div className="flex-1">
                <div className="font-semibold mb-1">Status</div>
                {getStatusBadge(selectedJob.status)}
              </div>
            </div>

            <div className="flex space-x-2">
              <div className="flex-1">
                <div className="font-semibold mb-1">Created</div>
                <div>
                  {selectedJob.createdAt} ({formatDistanceToNowStrict(selectedJob.createdAt, { addSuffix: true })})
                </div>
              </div>
              <div className="flex-1">
                <div className="font-semibold mb-1">Priority</div>
                <div>{selectedJob.priority}</div>
              </div>
            </div>

            <div>
              <div className="font-semibold mb-1">Scheduled for</div>
              <div>
                {selectedJob.scheduledFor ? (
                  <>
                    {selectedJob.scheduledFor} ({formatDistanceToNowStrict(selectedJob.scheduledFor, { addSuffix: true })})
                  </>
                ) : (
                  "Not scheduled"
                )}
              </div>
            </div>

            <div>
              <div className="font-semibold mb-1">Completed</div>
              <div>
                {selectedJob.completedAt ? (
                  <>
                    {selectedJob.completedAt} ({formatDistanceToNowStrict(selectedJob.completedAt, { addSuffix: true })})
                  </>
                ) : (
                  "Not completed"
                )}
              </div>
            </div>

            <div>
              <div className="font-semibold mb-1">Duration</div>
              <div>{selectedJob.durationMs ? <>{selectedJob.durationMs} ms</> : "Not completed"}</div>
            </div>

            <div>
              <div className="font-semibold mb-1">Failed</div>
              <div>{selectedJob.failedAt || "Not failed"}</div>
            </div>

            <div>
              <div className="font-semibold mb-1">Attempts</div>
              <div>{selectedJob.attempts}</div>
            </div>

            <div>
              <div className="font-semibold mb-1">Latest failure reason</div>
              <div>
                {selectedJob.latestFailureReason ? (
                  <span className="text-red-600">{selectedJob.latestFailureReason}</span>
                ) : (
                  "No failure reason"
                )}
              </div>
            </div>

            <div>
              <div className="font-semibold mb-1">Payload</div>
              <pre className="bg-neutral-900 p-2 rounded text-sm overflow-x-auto whitespace-pre-wrap">
                {JSON.stringify(selectedJob.payload, null, 2)}
              </pre>
            </div>

            <div className="mt-auto pt-4 mb-4 ">
              <div className="flex items-center space-x-2">
                <Button variant="outline" size="sm" onClick={() => copyJobId(selectedJob)}>
                  <IconCopy className="h-4 w-4" />
                  copy id
                </Button>
                <Button variant="outline" size="sm" onClick={() => copyJobPayload(selectedJob)}>
                  <IconCopy className="h-4 w-4" />
                  copy payload
                </Button>
                <AlertDialog>
                  <AlertDialogTrigger asChild>
                    <Button variant="default" size="sm">
                      <IconRecharging className="h-4 w-4" />
                      restart
                    </Button>
                  </AlertDialogTrigger>
                  <AlertDialogContent>
                    <AlertDialogHeader>
                      <AlertDialogTitle>Are you absolutely sure?</AlertDialogTitle>
                      <AlertDialogDescription>
                        This action cannot be undone. The job will return to the <i>pending</i> state and <b>will be executed again</b>.
                      </AlertDialogDescription>
                    </AlertDialogHeader>
                    <AlertDialogFooter>
                      <AlertDialogCancel>Cancel</AlertDialogCancel>
                      <AlertDialogAction onClick={() => toast("Not yet implemented", {})}>Continue</AlertDialogAction>
                    </AlertDialogFooter>
                  </AlertDialogContent>
                </AlertDialog>
              </div>
            </div>
          </div>
        ) : (
          <div>Select a job to see details</div>
        )}
      </div>
    </div>
  );
}
