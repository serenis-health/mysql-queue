"use client";

import { ColumnDef } from "@tanstack/react-table";
import { DataTable } from "./data-table";
import { LiveTime } from "@/components/live-time";
import { parseCronExpression } from "@/lib/utils";
import { PeriodicJob } from "@/types/periodic-job";

const periodicJobColumns: ColumnDef<PeriodicJob>[] = [
  {
    accessorKey: "name",
    header: "Name",
    cell: ({ row }) => <span className="font-medium">{row.original.name}</span>,
  },
  {
    accessorKey: "cronExpression",
    header: "Schedule",
    cell: ({ row }) => (
      <div className="flex flex-row gap-2">
        <pre className="text-muted-foreground">{row.original.cronExpression || ""}</pre>
        <span className="">({row.original.cronExpression ? parseCronExpression(row.original.cronExpression) : "N/A"})</span>
      </div>
    ),
  },
  {
    accessorKey: "lastEnqueuedAt",
    header: "Last Enqueued",
    cell: ({ row }) => (
      <span className="text-xs">{row.original.lastEnqueuedAt ? <LiveTime date={new Date(row.original.lastEnqueuedAt)} /> : "Never"}</span>
    ),
  },
  {
    accessorKey: "nextRunAt",
    header: "Next Run",
    cell: ({ row }) => (
      <span className="text-xs">
        <LiveTime date={new Date(row.original.nextRunAt)} />
      </span>
    ),
  },
];

interface PeriodicJobsTableProps {
  periodicJobs: PeriodicJob[];
  totalCount: number;
  isLoading: boolean;
  pagination?: { pageIndex: number; pageSize: number };
  onPaginationChange?: (pagination: { pageIndex: number; pageSize: number }) => void;
}

export function PeriodicJobsTable({ periodicJobs, totalCount, isLoading, pagination, onPaginationChange }: PeriodicJobsTableProps) {
  return (
    <DataTable
      columns={periodicJobColumns}
      data={periodicJobs}
      totalCount={totalCount}
      isLoading={isLoading}
      pagination={pagination}
      onPaginationChange={onPaginationChange}
      entityName="periodic job"
    />
  );
}
