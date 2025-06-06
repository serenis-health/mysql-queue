import { getJobs, getQueues } from "@/lib/db";
import Dashboard from "./dashboard";

export const dynamic = "force-dynamic";

export default async function Home() {
  const jobs = await getJobs({});
  const queues = await getQueues();

  return <Dashboard initialJobs={jobs} queues={queues} />;
}
