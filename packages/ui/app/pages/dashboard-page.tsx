"use client";

import { useEffect, useState } from "react";
import { ChartAreaInteractive } from "@/components/chart-area-interactive";
import { useConnection } from "@/contexts/connection-context";

interface DashboardData {
  totalJobs: Array<{ date: string; jobsCount: number }>;
  failures: Array<{ date: string; failures: number; retries: number }>;
}

export default function DashboardPage() {
  const { activeConnection } = useConnection();
  const [data, setData] = useState<DashboardData>({ totalJobs: [], failures: [] });
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function fetchDashboardData() {
      if (!activeConnection) return;

      try {
        const params = new URLSearchParams({
          connectionId: activeConnection.id,
          days: "14",
        });
        const response = await fetch(`/api/dashboard?${params.toString()}`);
        const result = await response.json();
        setData(result);
      } catch (error) {
        console.error("Failed to fetch dashboard data:", error);
      } finally {
        setLoading(false);
      }
    }
    void fetchDashboardData();
  }, [activeConnection]);

  if (loading) {
    return (
      <div className="grid gap-4 md:grid-cols-2">
        <div className="h-[400px] animate-pulse rounded-lg bg-muted" />
        <div className="h-[400px] animate-pulse rounded-lg bg-muted" />
      </div>
    );
  }

  return (
    <div className="grid gap-4 md:grid-cols-2">
      <ChartAreaInteractive
        title="Total Jobs"
        description="Jobs processed over time"
        data={data.totalJobs}
        series={[{ key: "jobsCount", label: "Jobs Count" }]}
        defaultTimeRange="7d"
      />
      <ChartAreaInteractive
        title="Failures"
        description="Failed and retried jobs over time"
        data={data.failures}
        series={[
          { key: "failures", label: "Failures", color: "var(--color-red-200)" },
          { key: "retries", label: "Retries", color: "var(--color-yellow-200)" },
        ]}
        defaultTimeRange="7d"
      />
    </div>
  );
}
