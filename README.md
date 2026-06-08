# MySQL Queue

mysql-queue is a job queue built with Node.js on top of MySql in order to provide background processing and reliable
asynchronous execution to Node.js applications.

mysql-queue relies
on [SKIP LOCKED](https://dev.mysql.com/blog-archive/mysql-8-0-1-using-skip-locked-and-nowait-to-handle-hot-rows/), a
feature built specifically for message queues to resolve record locking challenges
inherent with relational databases. This provides exactly-once delivery and the safety of guaranteed atomic commits to
asynchronous job processing.

### Motivation

Many applications require background job processing for tasks such as sending emails, processing payments, or executing
long-running operations. Traditional message brokers like RabbitMQ or Redis introduce operational complexity and can
lead to consistency issues when coordinating with a relational database.

**mysql-queue** ensures **100% consistency and atomicity** by leveraging MySQL transactions. Jobs are created and
processed within the same transactional context as the rest of the application’s data, guaranteeing that:

- **No ghost jobs** – Jobs are inserted within the same transaction as application data and become visible only after
  commit, ensuring consistency.
- **No job is executed more than once** – By using **SKIP LOCKED**, workers fetch jobs in a way that prevents duplicate
  processing, even under concurrent load.
- **Scalable concurrency** – Multiple workers can safely process jobs in parallel without conflicts.

This approach eliminates the need for two-phase commits, external locking mechanisms, or additional infrastructure,
providing a simple yet powerful solution for applications that require strong consistency guarantees.

### Database as a Queue: antipattern? It depends.

What do you need? What do you mean by "job"? Do you need to process in real-time? Can you afford to lose one of these
jobs?

mysql-queue is suitable for you if

- You need guaranteed consistency and atomicity by processing jobs within the same transactional context as your
  application data
- You don't need to process jobs in real-time (e.g., within milliseconds).
- You don't need to scale to millions of jobs per second.

Many companies and startups use a db-based job queue system like this in
production, [such as hey.com](https://x.com/dhh/status/1735724818052604024), handling millions
of jobs per day. If it's good enough for them, it's probably good enough for you too.

- https://github.com/rails/solid_queue -> [blog post](https://dev.37signals.com/solid-queue-v1-0/)
- https://github.com/timgit/pg-boss -> [blog post](https://wasp.sh/blog/2022/06/15/jobs-feature-announcement), [interview](https://wasp.sh/blog/2024/09/03/OS-builders-interview-with-tim-jones-pgboss)

### Features

- **_Job long retry with delay_**: Enhance retry behavior by introducing long retry intervals, allowing retries to be spaced
  out over a longer period, especially for jobs that may need more time before they can succeed.
- **_Job timeout_**: Set a timeout for jobs to ensure they do not run indefinitely. If a job exceeds the specified time, it
  should be terminated or retried depending on the configuration.
- **_Raw sql to enable adding jobs from different systems_**: Allow adding jobs from different systems by
  providing a raw SQL interface to insert jobs into the queue. This can be useful for integrating with other
  applications or systems that do not use mysql2 (e.g Prisma).
- **_Options on queue level_** (e.g., Max Retries, Retry Delay, etc.): Add configuration options at the queue level to
  control job execution behavior. This includes settings like maximum retries, retry delays, and other parameters that
  affect how jobs are handled within the queue.
- **_Job priority_**: Implement job priority, allowing certain jobs to be processed before others based on their priority
  level. This could be useful for ensuring critical jobs are processed more urgently.
- **_Job delay_**: Implement the ability to schedule jobs with a delay, meaning the job will not be processed immediately
  but will instead wait until a specified time before being executed.
- **_Dead letter queue_**: (Not yet implemented) A queue where jobs that fail multiple times (or meet other failure
  criteria) are moved for further investigation or to prevent them from continuously retrying. This ensures failed jobs
  don't clog the main job queue.

### How to

```typescript
const mysqlQueue = MysqlQueue({ dbUri: "mysql://root:password@localhost:3306/serenis" });

await mysqlQueue.initialize();

const queue = "emails";
const q = await mysqlQueue.upsertQueue({ name: "emails" });

console.log(q); // { id: '4dcc1f2b-6752-4a55-98a5-eb71aef19ffd', name: 'emails', backoffMultiplier: 2, maxRetries: 3, minDelayMs: 1000, maxDurationMs: 5000 }

await mysqlQueue.enqueue(queue, { name: "sendEmail", payload: { to: "hello@serenis.it" } });

await mysqlQueue.enqueue(queue, [{ name: "sendEmail", payload: { to: "hello@serenis.it" } }]);

const worker = await mysqlQueue.work(queue, async ([job]: Job[], signal: AbortSignal, ctx: CallbackContext) => {
  // job contains all job data
  // signal is an AbortSignal that can be called due to a timeout or a worker stop
  // connection has an active transaction that can be used to perform additional operations in the same transaction
  await emailService.send(job.payload, signal, { idempotenceKey: job.id });
});

void worker.start(); // start consuming jobs

await worker.stop(); // stop consuming jobs

await mysqlQueue.dispose(); // gracefully shutdown database and workers
await mysqlQueue.destroy(); // remove all queues and jobs and drop the database
```

### Worker options (`work`)

The third argument to `work()` is optional. Besides tuning polling and batch sizes (`pollingIntervalMs`, `pollingBatchSize`, `callbackBatchSize`), you can attach hooks:

- **`onJobClaimed`** — Called once per job after it has been **claimed** for processing (same shape as `onJobProcessed`: a `JobWithQueueName`), **before** your handler runs. Supports async functions. Errors thrown from this hook are caught and logged without blocking job processing.

- **`onJobProcessed`** — Called once per job after it has been successfully processed. Supports async functions. Errors thrown from this hook are caught and logged.

- **`onJobFailed`** — Called when a job has exhausted retries and is considered permanently failed (receives the error and `{ id, queueName }`). Supports async functions. The hooks are called **after** the database transaction closes to avoid holding connections. Errors thrown from this hook are caught and logged.

#### Processing jobs in batches (`pollingBatchSize > 1`)

By default a worker picks up one job at a time. For high-throughput queues you can have it grab several at once with two options:

- **`pollingBatchSize`** — how many jobs the worker fetches on each poll.
- **`callbackBatchSize`** — how many of those jobs are passed to a single call of your handler. The fetched jobs are split into groups of this size, and the groups run **in parallel**.

Your handler always receives an **array** of jobs. Example: `pollingBatchSize: 50, callbackBatchSize: 10` fetches 50 jobs and runs your handler 5 times in parallel, each call getting 10 jobs.

```typescript
const worker = await mysqlQueue.work(
  "emails",
  async (jobs: Job[], signal: AbortSignal, ctx: CallbackContext) => {
    // `jobs` is a group of up to callbackBatchSize jobs — process them together
    await emailService.sendBulk(
      jobs.map((j) => j.payload),
      signal,
    );
  },
  { pollingBatchSize: 50, callbackBatchSize: 10 },
);
```

Things to keep in mind:

- **Process every job in the array**, not just `jobs[0]` — e.g. a bulk insert or a bulk send.
- **Retries happen per group, not per job.** If your handler throws, every job in that group is retried together. Use `callbackBatchSize: 1` if you need each job to succeed or fail on its own; use a larger value for bulk efficiency.
- **Make your handler idempotent.** Since the whole group retries together, jobs that already succeeded before a later one failed will run again on retry. Use `idempotentKey` when enqueuing and/or make the side effect safe to repeat.
- **The job timeout (`maxDurationMs`) applies to one handler call.** With larger groups, give it enough time to process them all, otherwise the call is aborted and retried.
- **Larger batches recover more slowly from a crash.** If a worker dies mid-batch, those jobs are only retried after `rescuerRescueAfterMs` (default 1h). Lower it if you use large batches and want faster recovery.

> **⚠️ Watch your own resources.** A worker runs your handler in parallel: it makes about `pollingBatchSize / callbackBatchSize` simultaneous calls (e.g. `50 / 10` = 5), multiplied by the number of workers you run. If your handler hits a shared resource with limited capacity — your app's database connection pool, an HTTP client pool, a rate-limited API — that concurrency can exhaust it. Size those pools/limits for the peak number of handler calls you'll have in flight, or keep the batch sizes modest. This is independent of mysql-queue's own [connection pool](#connection-pool).

### Connection pool

mysql-queue keeps a pool of MySQL connections (shared by all your workers and the internal background tasks). By default the pool allows **10** connections. Each worker needs a connection only for the brief moments it reads and updates jobs in the database — **not** while your handler runs — so even long-running jobs don't tie up the pool.

If you run a lot of workers at once, 10 connections may not be enough. When they're all busy, new requests **wait in line** rather than failing, so the symptom isn't an error — it's jobs being picked up more slowly. If you see that, raise the pool size. A good starting point:

```
poolConnectionLimit ≈ (number of workers running at once) + a few spare
```

The spare ones leave room for the background tasks (retries of stuck jobs, periodic jobs, cleanup).

You can tune the pool when creating the instance:

```typescript
const mysqlQueue = MysqlQueue({
  dbUri: "mysql://...",
  poolConnectionLimit: 20, // max simultaneous connections (default: 10)
  poolQueueLimit: 0, // how many requests may wait when the pool is full; 0 = unlimited (default)
  poolEnableKeepAlive: true, // keep idle connections alive so they aren't dropped after quiet periods (default: true)
});
```

`poolEnableKeepAlive` is on by default because it prevents a common gotcha: a connection that sat idle gets silently closed by MySQL or a proxy, and the next job using it fails with a connection error. The keep-alive probes keep those connections healthy.

_Created with inspiration from [pg-boss](https://github.com/timgit/pg-boss), thanks Tim!_
