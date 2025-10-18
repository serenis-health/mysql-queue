"use client";

import * as React from "react";
import { IconCircleCheck, IconCircleDashed, IconCircleX, IconClock, IconPlayerPlay } from "@tabler/icons-react";
import { Badge } from "./ui/badge";
import { ColumnDef } from "@tanstack/react-table";
import { DataTable } from "./data-table";
import { formatDuration } from "@/lib/utils";
import { Job } from "@/types/job";
import { JobDetailModal } from "./job-detail-modal";
import { JobStatusBadge } from "@/components/job-status-badge";
import { LiveTime } from "@/components/live-time";
import { useState } from "react";

const statuses = [
  {
    value: "pending",
    label: "Pending",
    icon: IconCircleDashed,
  },
  {
    value: "running",
    label: "Running",
    icon: IconPlayerPlay,
  },
  {
    value: "completed",
    label: "Completed",
    icon: IconCircleCheck,
  },
  {
    value: "failed",
    label: "Failed",
    icon: IconCircleX,
  },
  {
    value: "scheduled",
    label: "Scheduled",
    icon: IconClock,
  },
];

interface JobsTableProps {
  jobs: Job[];
  totalCount: number;
  isLoading: boolean;
  filterCounts: {
    byStatus: Record<string, number>;
    byQueue: Record<string, number>;
    byName: Record<string, number>;
  };
  searchQuery?: string;
  onSearchChange?: (value: string) => void;
  dateFrom?: Date;
  dateTo?: Date;
  onDateFromChange?: (date: Date | undefined) => void;
  onDateToChange?: (date: Date | undefined) => void;
  statusFilter?: string[];
  onStatusFilterChange?: (values: string[]) => void;
  queueFilter?: string[];
  onQueueFilterChange?: (values: string[]) => void;
  nameFilter?: string[];
  onNameFilterChange?: (values: string[]) => void;
  pagination?: { pageIndex: number; pageSize: number };
  onPaginationChange?: (pagination: { pageIndex: number; pageSize: number }) => void;
  autoReloadEnabled?: boolean;
  onAutoReloadEnabledChange?: (enabled: boolean) => void;
  autoReloadInterval?: 5 | 15 | 30;
  onAutoReloadIntervalChange?: (interval: 5 | 15 | 30) => void;
}

export function JobsTable({
  jobs,
  totalCount,
  isLoading,
  filterCounts,
  searchQuery,
  onSearchChange,
  dateFrom,
  dateTo,
  onDateFromChange,
  onDateToChange,
  statusFilter,
  onStatusFilterChange,
  queueFilter,
  onQueueFilterChange,
  nameFilter,
  onNameFilterChange,
  pagination,
  onPaginationChange,
  autoReloadEnabled,
  onAutoReloadEnabledChange,
  autoReloadInterval,
  onAutoReloadIntervalChange,
}: JobsTableProps) {
  const [selectedJob, setSelectedJob] = useState<Job | null>(null);
  const [modalOpen, setModalOpen] = useState(false);

  function handleRowClick(job: Job) {
    setSelectedJob(job);
    setModalOpen(true);
  }

  const jobColumns: ColumnDef<Job>[] = [
    {
      accessorKey: "id",
      header: "ID",
      cell: ({ row }) => (
        <div className="flex flex-row gap-2">
          <pre className="text-xs">{row.original.id}</pre>{" "}
          {row.original.payload["_periodic"] ? <Badge variant="outline">periodic</Badge> : ""}
        </div>
      ),
    },
    {
      accessorKey: "name",
      header: "Name",
      cell: ({ row }) => <pre className="text-xs">{row.original.name}</pre>,
    },
    {
      accessorKey: "queueName",
      header: "Queue",
      cell: ({ row }) => row.original.queueName,
    },
    {
      accessorKey: "status",
      header: "Status",
      cell: ({ row }) => <JobStatusBadge job={row.original} />,
    },
    {
      accessorKey: "createdAt",
      header: "Enqueued",
      cell: ({ row }) => <span className="text-xs">{<LiveTime date={new Date(row.original.createdAt)} />}</span>,
    },
    {
      accessorKey: "attempts",
      header: "Attempts",
      cell: ({ row }) => (
        <span className="text-sm">
          {row.original.attempts}/{row.original.maxRetries}
        </span>
      ),
    },
    {
      accessorKey: "completeInMs",
      header: "Completed in",
      cell: ({ row }) => <span className="text-sm">{row.original.completedInMs ? formatDuration(row.original.completedInMs) : "-"}</span>,
    },
  ];

  return (
    <>
      <DataTable
        columns={jobColumns}
        data={jobs}
        totalCount={totalCount}
        isLoading={isLoading}
        onRowClick={handleRowClick}
        searchPlaceholder="Search by ID or payload..."
        globalFilterValue={searchQuery}
        onGlobalFilterChange={onSearchChange}
        dateFrom={dateFrom}
        dateTo={dateTo}
        onDateFromChange={onDateFromChange}
        onDateToChange={onDateToChange}
        pagination={pagination}
        onPaginationChange={onPaginationChange}
        autoReloadEnabled={autoReloadEnabled}
        onAutoReloadEnabledChange={onAutoReloadEnabledChange}
        autoReloadInterval={autoReloadInterval}
        onAutoReloadIntervalChange={onAutoReloadIntervalChange}
        filters={[
          {
            column: "name",
            title: "Name",
            options: Object.keys(filterCounts.byName).map((name) => ({
              label: name,
              value: name,
              count: filterCounts.byName[name] || 0,
            })),
            value: nameFilter,
            onChange: onNameFilterChange,
          },
          {
            column: "status",
            title: "Status",
            options: statuses.map((status) => ({
              ...status,
              count: filterCounts.byStatus[status.value] || 0,
            })),
            value: statusFilter,
            onChange: onStatusFilterChange,
          },
          {
            column: "queueName",
            title: "Queue",
            options: Object.keys(filterCounts.byQueue).map((queueName) => ({
              label: queueName,
              value: queueName,
              count: filterCounts.byQueue[queueName] || 0,
            })),
            value: queueFilter,
            onChange: onQueueFilterChange,
          },
        ]}
      />
      <JobDetailModal job={selectedJob} open={modalOpen} onOpenChange={setModalOpen} />
    </>
  );
}
