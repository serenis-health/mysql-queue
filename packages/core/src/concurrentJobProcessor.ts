import { chunk } from "./utils";
import { Job } from "./types";

export async function executeJobsConcurrently(
  jobs: Job[],
  callbackBatchSize: number,
  workerAbortSignal: AbortSignal,
  executeFn: ExecuteJobChunkCallback,
): Promise<JobExecutionResult> {
  const chunks = chunk(jobs, callbackBatchSize);
  const chunkAbortControllers = chunks.map(() => new AbortController());

  function abortAllChunks() {
    chunkAbortControllers.forEach((controller) => controller.abort());
  }
  workerAbortSignal.addEventListener("abort", abortAllChunks);

  try {
    const results = await Promise.allSettled(
      chunks.map((chunkJobs, index) =>
        executeFn(
          chunkJobs,
          chunkJobs.map((j) => j.id),
          chunkAbortControllers[index],
        ),
      ),
    );

    return groupResultsByStatus(chunks, results);
  } finally {
    workerAbortSignal.removeEventListener("abort", abortAllChunks);
  }
}

function groupResultsByStatus(chunks: Job[][], results: PromiseSettledResult<{ shouldMarkAsCompleted: boolean }>[]): JobExecutionResult {
  const successful: { jobs: Job[]; ids: string[] } = { ids: [], jobs: [] };
  const manuallyCompleted: { jobs: Job[]; ids: string[] } = { ids: [], jobs: [] };
  const failed: Array<{ jobs: Job[]; ids: string[]; error: Error }> = [];

  results.forEach((result, i) => {
    const chunkJobs = chunks[i];
    const chunkJobIds = chunkJobs.map((j) => j.id);

    if (result.status === "fulfilled") {
      if (result.value.shouldMarkAsCompleted) {
        successful.jobs.push(...chunkJobs);
        successful.ids.push(...chunkJobIds);
      } else {
        manuallyCompleted.jobs.push(...chunkJobs);
        manuallyCompleted.ids.push(...chunkJobIds);
      }
    } else {
      failed.push({
        error: result.reason as Error,
        ids: chunkJobIds,
        jobs: chunkJobs,
      });
    }
  });

  return { failed, manuallyCompleted, successful };
}

export type JobExecutionResult = {
  successful: {
    jobs: Job[];
    ids: string[];
  };
  manuallyCompleted: {
    jobs: Job[];
    ids: string[];
  };
  failed: {
    jobs: Job[];
    ids: string[];
    error: Error;
  }[];
};

export type ExecuteJobChunkCallback = (
  jobs: Job[],
  jobIds: string[],
  abortController: AbortController,
) => Promise<{ shouldMarkAsCompleted: boolean }>;
