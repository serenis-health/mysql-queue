"use client";

import * as React from "react";
import { CalendarRange, ChartArea, ListCheck, Rows4 } from "lucide-react";
import { Sidebar, SidebarContent, SidebarFooter, SidebarHeader, SidebarRail } from "@/components/ui/sidebar";
import { ConnectionSwitcher } from "@/components/connection-switcher";
import { Leader } from "@/types/leader";
import { NavLeader } from "@/components/nav-leader";
import { NavMain } from "@/components/nav-main";
import { NavUser } from "@/components/nav-user";
import { User } from "@/types/user";

export function AppSidebar({ user, leader, ...props }: React.ComponentProps<typeof Sidebar> & { user: User; leader: Leader | null }) {
  return (
    <Sidebar collapsible="icon" {...props}>
      <SidebarHeader>
        <ConnectionSwitcher />
      </SidebarHeader>
      <SidebarContent>
        <NavMain
          items={[
            { title: "Dashboard", url: "#", icon: ChartArea },
            { title: "Jobs", url: "#", icon: ListCheck },
            { title: "Queues", url: "#", icon: Rows4 },
            { title: "Periodic jobs", url: "#", icon: CalendarRange },
          ]}
        />
      </SidebarContent>
      <SidebarFooter>
        <NavLeader leader={leader} />
        <NavUser user={user} />
      </SidebarFooter>
      <SidebarRail />
    </Sidebar>
  );
}
