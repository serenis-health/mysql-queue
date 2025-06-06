import { getJobs, getQueues } from "@/lib/db";
import Dashboard from "./dashboard";

export default async function Home() {
  const jobs = await getJobs({});
  const queues = await getQueues();

  return <Dashboard initialJobs={jobs} queues={queues} />;
}
