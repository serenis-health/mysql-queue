"use client";

import { AlertCircle, CheckCircle, Clock, Loader2 } from "lucide-react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Job, Queue } from "@/lib/db";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { formatDistanceToNowStrict } from "date-fns";
import type React from "react";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { useRouter } from "next/navigation";
import { useState } from "react";

const jobStatuses: Record<Job["status"], { color: string; icon: React.FC<React.SVGProps<SVGSVGElement>>; label: string }> = {
  completed: { color: "bg-green-500", icon: CheckCircle, label: "Completed" },
  failed: { color: "bg-red-500", icon: AlertCircle, label: "Failed" },
  pending: { color: "bg-yellow-500", icon: Clock, label: "Pending" },
  scheduled: { color: "bg-blue-500", icon: Clock, label: "Scheduled" },
};

function getStatusInfo(status: Job["status"]) {
  return jobStatuses[status as keyof typeof jobStatuses];
}

export default function JobsSystemClient({
  queues: initialQueues,
  jobs: initialJobs,
  jobCounts,
  totalJobs,
}: {
  queues: Queue[];
  jobs: Job[];
  jobCounts: Record<string, number>;
  totalJobs: number;
}) {
  const router = useRouter();
  const [selectedQueue, setSelectedQueue] = useState<string | null>(null);
  const [selectedJob, setSelectedJob] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [jobs, setJobs] = useState<Job[]>(initialJobs);
  const [activeTab, setActiveTab] = useState("all");

  const filteredJobs = jobs.filter((job) => {
    const matchesQueue = selectedQueue ? job.queueName === selectedQueue : true;
    const matchesSearch = job.name.toLowerCase().includes(searchQuery.toLowerCase());
    const matchesStatus = activeTab === "all" ? true : job.status === activeTab;
    return matchesQueue && matchesSearch && matchesStatus;
  });

  const jobDetail = selectedJob ? jobs.find((job) => job.id === selectedJob) : null;

  async function fetchFilteredJobs() {
    setIsLoading(true);
    try {
      const params = new URLSearchParams();
      if (selectedQueue) params.append("queueId", selectedQueue);
      if (activeTab !== "all") params.append("status", activeTab);
      if (searchQuery) params.append("search", searchQuery);

      const response = await fetch(`/api/jobs?${params.toString()}`);
      if (response.ok) {
        const data = await response.json();
        setJobs(data);
      }
    } catch (error) {
      console.error("Error fetching jobs:", error);
    } finally {
      setIsLoading(false);
    }
  }

  function handleQueueSelect(queueId: string | null) {
    setSelectedQueue(queueId);
    setSelectedJob(null);
    router.refresh();
  }

  function handleTabChange(value: string) {
    setActiveTab(value);
    router.refresh();
  }

  function handleSearch(e: React.ChangeEvent<HTMLInputElement>) {
    setSearchQuery(e.target.value);
    // Implementa la ricerca lato client per una migliore UX
  }

  async function handleRetryJob(jobId: string) {
    try {
      const response = await fetch(`/api/jobs/${jobId}/retry`, {
        method: "POST",
      });
      if (response.ok) {
        router.refresh();
      }
    } catch (error) {
      console.error("Error retrying job:", error);
    }
  }

  return (
    <div className="flex h-screen bg-background">
      {/* Queues List - Left Sidebar */}
      <div className="w-64 border-r bg-muted/40">
        <div className="p-4">
          <h2 className="mb-2 text-lg font-semibold">Queues</h2>
          <Button
            variant={selectedQueue === null ? "secondary" : "outline"}
            className="w-full justify-start mb-2"
            onClick={() => handleQueueSelect(null)}
          >
            All Queues
            <Badge className="ml-auto" variant="secondary">
              {totalJobs}
            </Badge>
          </Button>
          <ScrollArea className="h-[calc(100vh-120px)]">
            <div className="space-y-1">
              {initialQueues.map((queue) => (
                <Button
                  key={queue.id}
                  variant={selectedQueue === queue.id ? "secondary" : "ghost"}
                  className="w-full justify-start"
                  onClick={() => handleQueueSelect(queue.id)}
                >
                  {queue.name}
                  <Badge className="ml-auto" variant="outline">
                    {queue.count}
                  </Badge>
                </Button>
              ))}
            </div>
          </ScrollArea>
        </div>
      </div>

      {/* Jobs List - Middle Section */}
      <div className="flex-1 border-r">
        <div className="p-4">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-lg font-semibold">
              Jobs {selectedQueue && `- ${initialQueues.find((q) => q.id === selectedQueue)?.name}`}
            </h2>
          </div>

          <Tabs defaultValue="all" value={activeTab} onValueChange={handleTabChange}>
            <TabsList className="mb-4">
              <TabsTrigger value="all">
                All
                <Badge variant="outline" className="ml-2">
                  {totalJobs}
                </Badge>
              </TabsTrigger>
              <TabsTrigger value="completed">
                Completed
                <Badge variant="outline" className="ml-2">
                  {jobCounts.completed || 0}
                </Badge>
              </TabsTrigger>
              <TabsTrigger value="failed">
                Failed
                <Badge variant="outline" className="ml-2">
                  {jobCounts.failed || 0}
                </Badge>
              </TabsTrigger>
              <TabsTrigger value="waiting">
                Pending
                <Badge variant="outline" className="ml-2">
                  {jobCounts.pending || 0}
                </Badge>
              </TabsTrigger>
            </TabsList>

            <TabsContent value={activeTab} className="m-0">
              {isLoading ? (
                <div className="flex justify-center items-center h-40">
                  <Loader2 className="h-8 w-8 animate-spin text-primary" />
                </div>
              ) : (
                <ScrollArea className="h-[calc(100vh-180px)]">
                  <div className="space-y-2">
                    {filteredJobs.length > 0 ? (
                      filteredJobs.map((job) => {
                        const statusInfo = getStatusInfo(job.status);
                        return (
                          <Card
                            key={job.id}
                            className={`cursor-pointer ${selectedJob === job.id ? "border-primary" : ""}`}
                            onClick={() => setSelectedJob(job.id)}
                          >
                            <CardHeader className="p-4 pb-2">
                              <div className="flex items-center justify-between">
                                <CardTitle className="text-base">{job.name}</CardTitle>
                                <Badge className={`${statusInfo.color} text-white`}>
                                  <statusInfo.icon className="mr-1 h-3 w-3" />
                                  {statusInfo.label}
                                </Badge>
                              </div>
                              <CardDescription>
                                Queue: {job.queueName} • ID: {job.id}
                              </CardDescription>
                            </CardHeader>
                            <CardContent className="p-4 pt-0">
                              <div className="text-sm text-muted-foreground">
                                {formatDistanceToNowStrict(job.createdAt)} ago
                                {!!job.attempts && ` • Attempts: ${job.attempts}`}
                              </div>
                            </CardContent>
                          </Card>
                        );
                      })
                    ) : (
                      <div className="flex flex-col items-center justify-center h-40 text-muted-foreground">
                        <p>No jobs found</p>
                      </div>
                    )}
                  </div>
                </ScrollArea>
              )}
            </TabsContent>
          </Tabs>
        </div>
      </div>

      {/* Job Detail - Right Panel */}
      <div className="w-1/3 bg-background">
        {jobDetail ? (
          <div className="p-4">
            <h2 className="text-lg font-semibold mb-4">Job Details</h2>

            <div className="space-y-4">
              <div>
                <h3 className="text-sm font-medium text-muted-foreground">Name</h3>
                <p className="text-base">{jobDetail.name}</p>
              </div>

              <div>
                <h3 className="text-sm font-medium text-muted-foreground">ID</h3>
                <p className="text-base font-mono text-sm">{jobDetail.id}</p>
              </div>

              <div className="flex space-x-4">
                <div>
                  <h3 className="text-sm font-medium text-muted-foreground">Queue</h3>
                  <p className="text-base">{jobDetail.queueName}</p>
                </div>

                <div>
                  <h3 className="text-sm font-medium text-muted-foreground">Status</h3>
                  <div className="flex items-center">
                    <Badge className={`${getStatusInfo(jobDetail.status).color} text-white`}>{getStatusInfo(jobDetail.status).label}</Badge>
                  </div>
                </div>
              </div>

              <div className="flex space-x-4">
                <div>
                  <h3 className="text-sm font-medium text-muted-foreground">Created</h3>
                  <p className="text-base">{jobDetail.createdAt.toLocaleString()} ({formatDistanceToNowStrict(jobDetail.createdAt)} ago)</p>
                </div>

                <div>
                  <h3 className="text-sm font-medium text-muted-foreground">Priority</h3>
                  <p className="text-base">{jobDetail.priority}</p>
                </div>
              </div>

              {jobDetail.startAfter && (
                <div>
                  <h3 className="text-sm font-medium text-muted-foreground">Start After</h3>
                  <p className="text-base">{new Date(jobDetail.startAfter).toLocaleString()} ({formatDistanceToNowStrict(jobDetail.startAfter)} from now)</p>
                </div>
              )}

              {jobDetail.completedAt && (
                <div>
                  <h3 className="text-sm font-medium text-muted-foreground">Completed At</h3>
                  <p className="text-base">{new Date(jobDetail.completedAt).toLocaleString()}</p>
                </div>
              )}

              {jobDetail.failedAt && (
                <div>
                  <h3 className="text-sm font-medium text-muted-foreground">Failed At</h3>
                  <p className="text-base">{new Date(jobDetail.failedAt).toLocaleString()}</p>
                </div>
              )}

              <div>
                <h3 className="text-sm font-medium text-muted-foreground">Attempts</h3>
                <p className="text-base">{jobDetail.attempts}</p>
              </div>

              {jobDetail.latestFailureReason && (
                <div>
                  <h3 className="text-sm font-medium text-muted-foreground">Failure Reason</h3>
                  <p className="text-base text-red-500">{jobDetail.latestFailureReason}</p>
                </div>
              )}

              <Separator />

              <div>
                <h3 className="text-sm font-medium text-muted-foreground mb-2">Data</h3>
                <pre className="bg-muted p-4 rounded-md overflow-auto text-sm">{JSON.stringify(jobDetail.payload, null, 2)}</pre>
              </div>

              <div className="flex space-x-2">
                {jobDetail.status === "failed" && (
                  <Button variant="outline" size="sm" onClick={() => handleRetryJob(jobDetail.id)}>
                    Retry
                  </Button>
                )}
                <Button variant="outline" size="sm">
                  Delete
                </Button>
              </div>
            </div>
          </div>
        ) : (
          <div className="flex flex-col items-center justify-center h-full text-muted-foreground">
            <p>Select a job to view details</p>
          </div>
        )}
      </div>
    </div>
  );
}
