"use client";

import { Breadcrumb, BreadcrumbItem, BreadcrumbList, BreadcrumbPage } from "@/components/ui/breadcrumb";
import { ConnectionProvider, useConnection } from "@/contexts/connection-context";
import { NavigationProvider, useNavigation } from "@/contexts/navigation-context";
import { SidebarInset, SidebarProvider, SidebarTrigger } from "@/components/ui/sidebar";
import { Suspense, useEffect, useState } from "react";
import { AppSidebar } from "@/components/app-sidebar";
import { Connection } from "@/types/connection";
import DashboardPage from "@/app/pages/dashboard-page";
import JobsPage from "@/app/pages/jobs-page";
import { Leader } from "@/types/leader";
import { Loading } from "@/components/loading";
import PeriodicJobsPage from "@/app/pages/periodic-jobs-page";
import QueuesPage from "@/app/pages/queues-page";
import { Separator } from "@/components/ui/separator";
import { User } from "@/types/user";

function PageContent() {
  const { activePage } = useNavigation();
  const { activeConnection } = useConnection();
  const [user, setUser] = useState<User | null>(null);
  const [leader, setLeader] = useState<Leader | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  async function fetchUser() {
    const res = await fetch(`/api/user`);
    setUser(await res.json());
  }
  async function fetchLeader() {
    if (!activeConnection) return;

    const params = new URLSearchParams({
      connectionId: activeConnection.id,
    });
    const res = await fetch(`/api/leader?${params.toString()}`);
    if (res.status === 200) setLeader(await res.json());
  }

  useEffect(() => {
    setIsLoading(true);
    Promise.all([fetchUser(), fetchLeader()]).then(() => setIsLoading(false));
  }, [activeConnection]);

  function renderPage() {
    switch (activePage) {
      case "Dashboard":
        return <DashboardPage />;
      case "Jobs":
        return <JobsPage />;
      case "Queues":
        return <QueuesPage />;
      case "Periodic jobs":
        return <PeriodicJobsPage />;
      default:
        return <DashboardPage />;
    }
  }

  if (isLoading) {
    return <Loading />;
  }

  return (
    <SidebarProvider>
      <AppSidebar user={user!} leader={leader} />
      <SidebarInset>
        <header className="flex h-16 shrink-0 items-center gap-2 transition-[width,height] ease-linear group-has-data-[collapsible=icon]/sidebar-wrapper:h-12">
          <div className="flex items-center gap-2 px-4">
            <SidebarTrigger className="-ml-1" />
            <Separator orientation="vertical" className="mr-2 data-[orientation=vertical]:h-4" />
            <Breadcrumb>
              <BreadcrumbList>
                <BreadcrumbItem>
                  <BreadcrumbPage>{activePage}</BreadcrumbPage>
                </BreadcrumbItem>
              </BreadcrumbList>
            </Breadcrumb>
          </div>
        </header>
        <div className="flex flex-1 flex-col gap-4 p-4 pt-0">{renderPage()}</div>
      </SidebarInset>
    </SidebarProvider>
  );
}

function AppWrapper() {
  const [connections, setConnections] = useState<Connection[] | null>(null);

  useEffect(() => {
    async function fetchConnections() {
      const res = await fetch(`/api/connections`);
      setConnections(await res.json());
    }
    fetchConnections();
  }, []);

  if (!connections) {
    return <Loading />;
  }

  return (
    <ConnectionProvider initialConnections={connections}>
      <PageContent />
    </ConnectionProvider>
  );
}

export default function Page() {
  return (
    <Suspense fallback={<div>Loading...</div>}>
      <NavigationProvider>
        <AppWrapper />
      </NavigationProvider>
    </Suspense>
  );
}
