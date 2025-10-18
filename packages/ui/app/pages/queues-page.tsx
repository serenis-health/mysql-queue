import { useEffect, useState } from "react";
import { Queue } from "@/types/queue";
import { QueuesTable } from "@/components/queues-table";
import { useConnection } from "@/contexts/connection-context";

export default function QueuesPage() {
  const { activeConnection } = useConnection();
  const [queues, setQueues] = useState<Queue[] | null>(null);
  const [totalCount, setTotalCount] = useState<number>(0);
  const [isLoading, setIsLoading] = useState(false);
  const [pagination, setPagination] = useState({ pageIndex: 0, pageSize: 20 });

  async function fetchQueues(pageIndex?: number, pageSize?: number) {
    if (!activeConnection) return;

    setIsLoading(true);
    try {
      const params = new URLSearchParams({
        connectionId: activeConnection.id,
      });
      if (pageSize !== undefined) {
        params.set("limit", pageSize.toString());
      }
      if (pageIndex !== undefined && pageSize !== undefined) {
        params.set("offset", (pageIndex * pageSize).toString());
      }
      const res = await fetch(`/api/queues?${params.toString()}`);
      const data = await res.json();
      setQueues(data.queues);
      setTotalCount(data.total);
    } finally {
      setIsLoading(false);
    }
  }

  useEffect(() => {
    void fetchQueues(pagination.pageIndex, pagination.pageSize);
  }, [pagination.pageIndex, pagination.pageSize, activeConnection]);

  return queues ? (
    <QueuesTable queues={queues} totalCount={totalCount} isLoading={isLoading} pagination={pagination} onPaginationChange={setPagination} />
  ) : (
    "Loading"
  );
}
