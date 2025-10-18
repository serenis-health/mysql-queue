"use client";

import { CheckCircle, Clock, XCircle } from "lucide-react";
import { IconPlayerPauseFilled, IconProgress } from "@tabler/icons-react";
import { Tooltip, TooltipContent, TooltipTrigger } from "./ui/tooltip";
import { Badge } from "@/components/ui/badge";
import { ColumnDef } from "@tanstack/react-table";
import { DataTable } from "./data-table";
import { formatNumber } from "@/lib/utils";
import { Queue } from "@/types/queue";

const queueColumns: ColumnDef<Queue>[] = [
  {
    accessorKey: "id",
    header: "ID",
    cell: ({ row }) => <span className="font-mono text-xs">{row.original.id}</span>,
  },
  {
    accessorKey: "name",
    header: "Queue Name",
    cell: ({ row }) => <span className="font-medium">{row.original.name}</span>,
  },
  {
    accessorKey: "jobsCount",
    header: "Total Jobs",
    cell: ({ row }) => (
      <Badge variant="outline" className="px-2">
        {formatNumber(row.original.jobsCount)}
      </Badge>
    ),
  },
  {
    id: "statusCounts",
    header: "Stats",
    cell: ({ row }) => (
      <div className="flex gap-2">
        <Tooltip>
          <TooltipTrigger asChild>
            <Badge variant="outline" className="flex items-center gap-1 px-2">
              <Clock className="h-4 w-4 text-yellow-500" />
              {formatNumber(row.original.scheduledCount)}
            </Badge>
          </TooltipTrigger>
          <TooltipContent>Scheduled</TooltipContent>
        </Tooltip>

        <Tooltip>
          <TooltipTrigger asChild>
            <Badge variant="outline" className="flex items-center gap-1 px-2">
              <XCircle className="h-4 w-4 text-red-500" />
              {formatNumber(row.original.failedCount)}
            </Badge>
          </TooltipTrigger>
          <TooltipContent>Failed</TooltipContent>
        </Tooltip>

        <Tooltip>
          <TooltipTrigger asChild>
            <Badge variant="outline" className="flex items-center gap-1 px-2">
              <CheckCircle className="h-4 w-4 text-green-500" />
              {formatNumber(row.original.completedCount)}
            </Badge>
          </TooltipTrigger>
          <TooltipContent>Completed</TooltipContent>
        </Tooltip>
      </div>
    ),
  },
  {
    accessorKey: "maxRetries",
    header: "Max Retries",
    cell: ({ row }) => <span className="text-sm">{row.original.maxRetries}</span>,
  },
  {
    accessorKey: "minDelayMs",
    header: "Min Delay (ms)",
    cell: ({ row }) => <span className="text-sm">{row.original.minDelayMs}</span>,
  },
  {
    accessorKey: "maxDurationMs",
    header: "Max Duration (ms)",
    cell: ({ row }) => <span className="text-sm">{row.original.maxDurationMs}</span>,
  },
  {
    accessorKey: "backoffMultiplier",
    header: "Backoff",
    cell: ({ row }) => <span className="text-sm">{row.original.backoffMultiplier ?? "N/A"}</span>,
  },
  {
    accessorKey: "partitionKey",
    header: "Partition Key",
    cell: ({ row }) => <span className="text-sm">{row.original.partitionKey ?? "N/A"}</span>,
  },
  {
    accessorKey: "isPaused",
    header: "Queue Status",
    cell: ({ row }) => (
      <Badge variant="outline" className="px-2">
        {row.original.isPaused ? <IconPlayerPauseFilled /> : <IconProgress className="animate-spin text-blue-500" />}
        {row.original.isPaused ? "paused" : "active"}
      </Badge>
    ),
  },
];

interface QueuesTableProps {
  queues: Queue[];
  totalCount: number;
  isLoading: boolean;
  pagination?: { pageIndex: number; pageSize: number };
  onPaginationChange?: (pagination: { pageIndex: number; pageSize: number }) => void;
}

export function QueuesTable({ queues, totalCount, isLoading, pagination, onPaginationChange }: QueuesTableProps) {
  return (
    <DataTable
      columns={queueColumns}
      data={queues}
      totalCount={totalCount}
      isLoading={isLoading}
      pagination={pagination}
      onPaginationChange={onPaginationChange}
      entityName="queue"
    />
  );
}
