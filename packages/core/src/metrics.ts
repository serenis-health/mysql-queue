import { metrics } from "@opentelemetry/api";

export function createMetrics() {
  const meter = metrics.getMeter("mysql-queue");

  const jobsEnqueuedCounter = meter.createCounter("mysql_queue_jobs_enqueued_total", {
    description: "Total number of jobs enqueued",
  });
  const jobsClaimedCounter = meter.createCounter("mysql_queue_jobs_claimed_total", {
    description: "Total number of jobs claimed for processing",
  });
  const jobsCompletedCounter = meter.createCounter("mysql_queue_jobs_completed_total", {
    description: "Total number of jobs completed successfully",
  });
  const jobsFailedCounter = meter.createCounter("mysql_queue_jobs_failed_total", {
    description: "Total number of jobs that permanently failed",
  });
  const jobsRetriedCounter = meter.createCounter("mysql_queue_jobs_retried_total", {
    description: "Total number of jobs retried after failure",
  });
  const jobsRescuedCounter = meter.createCounter("mysql_queue_jobs_rescued_total", {
    description: "Total number of stuck jobs rescued",
  });
  const workersActiveCounter = meter.createUpDownCounter("mysql_queue_workers_active", {
    description: "Number of currently active workers",
  });
  const jobQueueWaitHistogram = meter.createHistogram("mysql_queue_job_queue_wait_seconds", {
    description: "Time from job eligibility (max(createdAt, startAfter)) to claim",
  });
  const jobExecutionHistogram = meter.createHistogram("mysql_queue_job_execution_seconds", {
    description: "Time from claim to completion/failure per job",
  });
  const jobProcessingHistogram = meter.createHistogram("mysql_queue_job_processing_seconds", {
    description: "Duration of job batch processing in seconds",
  });

  return {
    jobExecutionTime: (queue: string, jobName: string, seconds: number) =>
      jobExecutionHistogram.record(seconds, { job_name: jobName, queue }),
    jobProcessingDuration: (queue: string, seconds: number) => jobProcessingHistogram.record(seconds, { queue }),
    jobQueueWaitTime: (queue: string, jobName: string, seconds: number) =>
      jobQueueWaitHistogram.record(seconds, { job_name: jobName, queue }),
    jobsClaimed: (queue: string, jobName: string, count: number) => jobsClaimedCounter.add(count, { job_name: jobName, queue }),
    jobsCompleted: (queue: string, jobName: string, count: number) => jobsCompletedCounter.add(count, { job_name: jobName, queue }),
    jobsEnqueued: (queue: string, jobName: string, count: number) => jobsEnqueuedCounter.add(count, { job_name: jobName, queue }),
    jobsFailed: (queue: string, jobName: string, count: number) => jobsFailedCounter.add(count, { job_name: jobName, queue }),
    jobsRescued: (count: number) => jobsRescuedCounter.add(count),
    jobsRetried: (queue: string, jobName: string, count: number) => jobsRetriedCounter.add(count, { job_name: jobName, queue }),
    workerStarted: (queue: string) => workersActiveCounter.add(1, { queue }),
    workerStopped: (queue: string) => workersActiveCounter.add(-1, { queue }),
  };
}

export type Metrics = ReturnType<typeof createMetrics>;
