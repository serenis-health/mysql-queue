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
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { IconCopy, IconRecharging } from "@tabler/icons-react";
import { Button } from "@/components/ui/button";
import { formatDistanceToNowStrict } from "date-fns";
import { Job } from "@/types/job";
import { JobStatusBadge } from "@/components/job-status-badge";
import { toast } from "sonner";

interface JobDetailModalProps {
  job: Job | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function JobDetailModal({ job, open, onOpenChange }: JobDetailModalProps) {
  if (!job) return null;

  function copyJobId(job: Job) {
    navigator.clipboard.writeText(job.id);
    toast("id copied", { icon: <IconCopy /> });
  }

  function copyJobPayload(job: Job) {
    // Note: payload doesn't exist in current schema, but keeping for future
    navigator.clipboard.writeText(JSON.stringify(job, null, 2));
    toast("job data copied", { icon: <IconCopy /> });
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl max-h-[100vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>Job Detail</DialogTitle>
          <DialogDescription>View and manage job details</DialogDescription>
        </DialogHeader>

        <div className="flex flex-col space-y-4">
          <div>
            <div className="font-semibold mb-1">Id</div>
            <div className="font-mono text-sm">{job.id}</div>
          </div>

          <div className="flex space-x-2">
            <div className="flex-1">
              <div className="font-semibold mb-1">Queue</div>
              <div>{job.queueName}</div>
            </div>
            <div className="flex-1">
              <div className="font-semibold mb-1">Status</div>
              <JobStatusBadge job={job} />
            </div>
          </div>

          <div className="flex space-x-2">
            <div className="flex-1">
              <div className="font-semibold mb-1">Created</div>
              <div className="text-sm">
                {job.createdAt.toLocaleString()} ({formatDistanceToNowStrict(job.createdAt, { addSuffix: true })})
              </div>
            </div>
          </div>

          <div>
            <div className="font-semibold mb-1">Scheduled for</div>
            <div className="text-sm">
              {job.startAfter ? (
                <>
                  {job.startAfter.toLocaleString()} ({formatDistanceToNowStrict(job.startAfter, { addSuffix: true })})
                </>
              ) : (
                "Not scheduled"
              )}
            </div>
          </div>

          <div>
            <div className="font-semibold mb-1">Completed</div>
            <div className="text-sm">
              {job.completedAt ? (
                <>
                  {job.completedAt.toLocaleString()} ({formatDistanceToNowStrict(job.completedAt, { addSuffix: true })})
                </>
              ) : (
                "Not completed"
              )}
            </div>
          </div>

          <div>
            <div className="font-semibold mb-1">Duration</div>
            <div className="text-sm">{job.duration !== null ? `${job.duration} ms` : "Not ended"}</div>
          </div>

          <div>
            <div className="font-semibold mb-1">Failed At</div>
            <div className="text-sm">{job.failedAt ? job.failedAt.toLocaleString() : "Not failed"}</div>
          </div>

          <div>
            <div className="font-semibold mb-1">Running At</div>
            <div className="text-sm">{job.runningAt ? job.runningAt.toLocaleString() : "Not running"}</div>
          </div>

          <div>
            <div className="font-semibold mb-1">Payload</div>
            <pre className="bg-neutral-900 p-2 rounded text-sm overflow-x-auto whitespace-pre-wrap">
              {JSON.stringify(job.payload, null, 2)}
            </pre>
          </div>

          <div>
            <div className="font-semibold mb-1">Errors</div>
            <pre className="bg-neutral-900 p-2 rounded text-sm overflow-x-auto whitespace-pre-wrap">
              {JSON.stringify(job.errors, null, 2)}
            </pre>
          </div>

          <div className="flex items-center space-x-2 pt-4">
            <Button variant="outline" size="sm" onClick={() => copyJobId(job)}>
              <IconCopy className="h-4 w-4" />
              copy id
            </Button>
            <Button variant="outline" size="sm" onClick={() => copyJobPayload(job)}>
              <IconCopy className="h-4 w-4" />
              copy data
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
      </DialogContent>
    </Dialog>
  );
}
