"use client";

import { useEffect, useState } from "react";
import { Job } from "@/types/job";
import { JobsTable } from "@/components/jobs-table";
import { useConnection } from "@/contexts/connection-context";

export default function JobsPage() {
  const { activeConnection } = useConnection();
  const [jobs, setJobs] = useState<Job[] | null>(null);
  const [totalCount, setTotalCount] = useState<number>(0);
  const [filterCounts, setFilterCounts] = useState<{
    byStatus: Record<string, number>;
    byQueue: Record<string, number>;
    byName: Record<string, number>;
  }>({ byStatus: {}, byQueue: {}, byName: {} });
  const [searchQuery, setSearchQuery] = useState("");
  const [dateFrom, setDateFrom] = useState<Date | undefined>(undefined);
  const [dateTo, setDateTo] = useState<Date | undefined>(undefined);
  const [statusFilter, setStatusFilter] = useState<string[]>([]);
  const [queueFilter, setQueueFilter] = useState<string[]>([]);
  const [nameFilter, setNameFilter] = useState<string[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [pagination, setPagination] = useState({ pageIndex: 0, pageSize: 20 });
  const [autoReloadEnabled, setAutoReloadEnabled] = useState(false);
  const [autoReloadInterval, setAutoReloadInterval] = useState<5 | 15 | 30>(5);

  async function fetchJobs(
    search: string,
    from?: Date,
    to?: Date,
    statuses?: string[],
    queues?: string[],
    names?: string[],
    pageIndex?: number,
    pageSize?: number,
  ) {
    if (!activeConnection) return;

    setIsLoading(true);
    try {
      const params = new URLSearchParams({
        connectionId: activeConnection.id,
      });
      if (search) {
        params.set("searchQuery", search);
      }
      if (from) {
        params.set("createdAtFrom", from.toISOString());
      }
      if (to) {
        const endOfDay = new Date(to);
        endOfDay.setHours(23, 59, 59, 999);
        params.set("createdAtTo", endOfDay.toISOString());
      }
      if (statuses && statuses.length > 0) {
        statuses.forEach((status) => params.append("status", status));
      }
      if (queues && queues.length > 0) {
        queues.forEach((queue) => params.append("queueName", queue));
      }
      if (names && names.length > 0) {
        names.forEach((name) => params.append("name", name));
      }
      if (pageSize !== undefined) {
        params.set("limit", pageSize.toString());
      }
      if (pageIndex !== undefined && pageSize !== undefined) {
        params.set("offset", (pageIndex * pageSize).toString());
      }
      const res = await fetch(`/api/jobs?${params.toString()}`);
      const data = await res.json();
      setJobs(data.jobs);
      setTotalCount(data.total);
    } finally {
      setIsLoading(false);
    }
  }

  async function fetchCounts(search: string, from?: Date, to?: Date, statuses?: string[], queues?: string[], names?: string[]) {
    if (!activeConnection) return;

    const params = new URLSearchParams({
      connectionId: activeConnection.id,
    });
    if (search) {
      params.set("searchQuery", search);
    }
    if (from) {
      params.set("createdAtFrom", from.toISOString());
    }
    if (to) {
      const endOfDay = new Date(to);
      endOfDay.setHours(23, 59, 59, 999);
      params.set("createdAtTo", endOfDay.toISOString());
    }
    if (statuses && statuses.length > 0) {
      statuses.forEach((status) => params.append("status", status));
    }
    if (queues && queues.length > 0) {
      queues.forEach((queue) => params.append("queueName", queue));
    }
    if (names && names.length > 0) {
      names.forEach((name) => params.append("name", name));
    }
    const res = await fetch(`/api/jobs/counts?${params.toString()}`);
    const counts = await res.json();
    setFilterCounts(counts);
  }

  useEffect(() => {
    void fetchJobs(searchQuery, dateFrom, dateTo, statusFilter, queueFilter, nameFilter, pagination.pageIndex, pagination.pageSize);
    void fetchCounts(searchQuery, dateFrom, dateTo, statusFilter, queueFilter, nameFilter);
  }, [searchQuery, dateFrom, dateTo, statusFilter, queueFilter, nameFilter, pagination.pageIndex, pagination.pageSize, activeConnection]);

  useEffect(() => {
    if (!autoReloadEnabled) return;

    function fetchJobsAndCounts() {
      void fetchJobs(searchQuery, dateFrom, dateTo, statusFilter, queueFilter, nameFilter, pagination.pageIndex, pagination.pageSize);
      void fetchCounts(searchQuery, dateFrom, dateTo, statusFilter, queueFilter, nameFilter);
    }

    const interval = setInterval(() => {
      fetchJobsAndCounts();
    }, autoReloadInterval * 1000);
    fetchJobsAndCounts();

    return () => clearInterval(interval);
  }, [
    autoReloadEnabled,
    autoReloadInterval,
    searchQuery,
    dateFrom,
    dateTo,
    statusFilter,
    queueFilter,
    nameFilter,
    pagination.pageIndex,
    pagination.pageSize,
  ]);

  return jobs ? (
    <JobsTable
      jobs={jobs}
      totalCount={totalCount}
      isLoading={isLoading}
      filterCounts={filterCounts}
      searchQuery={searchQuery}
      onSearchChange={setSearchQuery}
      dateFrom={dateFrom}
      dateTo={dateTo}
      onDateFromChange={setDateFrom}
      onDateToChange={setDateTo}
      statusFilter={statusFilter}
      onStatusFilterChange={setStatusFilter}
      queueFilter={queueFilter}
      onQueueFilterChange={setQueueFilter}
      nameFilter={nameFilter}
      onNameFilterChange={setNameFilter}
      pagination={pagination}
      onPaginationChange={setPagination}
      autoReloadEnabled={autoReloadEnabled}
      onAutoReloadEnabledChange={setAutoReloadEnabled}
      autoReloadInterval={autoReloadInterval}
      onAutoReloadIntervalChange={setAutoReloadInterval}
    />
  ) : (
    "Loading"
  );
}
