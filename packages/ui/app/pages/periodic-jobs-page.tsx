"use client";

import { useEffect, useState } from "react";
import { PeriodicJob } from "@/types/periodic-job";
import { PeriodicJobsTable } from "@/components/periodic-jobs-table";
import { useConnection } from "@/contexts/connection-context";

export default function PeriodicJobsPage() {
  const { activeConnection } = useConnection();
  const [periodicJobs, setPeriodicJobs] = useState<PeriodicJob[]>([]);
  const [isLoading, setIsLoading] = useState(true);

  async function fetchPeriodicJobs() {
    if (!activeConnection) return;

    try {
      setIsLoading(true);
      const params = new URLSearchParams({
        connectionId: activeConnection.id,
      });
      const res = await fetch(`/api/periodic-jobs?${params.toString()}`);
      const data = await res.json();
      setPeriodicJobs(data);
    } catch (error) {
      console.error("Error fetching periodic jobs:", error);
    } finally {
      setIsLoading(false);
    }
  }

  useEffect(() => {
    fetchPeriodicJobs();
  }, [activeConnection]);

  return (
    <div className="flex-1">
      <PeriodicJobsTable periodicJobs={periodicJobs} totalCount={periodicJobs.length} isLoading={isLoading} />
    </div>
  );
}
