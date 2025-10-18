"use client";

import { SidebarMenu, SidebarMenuButton, SidebarMenuItem, useSidebar } from "@/components/ui/sidebar";
import { Leader } from "@/types/leader";
import { LiveTime } from "@/components/live-time";

export function NavLeader({ leader }: { leader: Leader | null }) {
  const { state } = useSidebar();

  if (!leader) return;

  const isExpired = new Date(leader.expiresAt) < new Date();
  const shortLeaderId = leader.leaderId.substring(0, 8);

  return (
    <SidebarMenu>
      <SidebarMenuItem>
        <SidebarMenuButton size="lg" tooltip="Leader">
          {isExpired ? (
            <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-destructive/10">
              <div className="h-3 w-3 rounded-full bg-destructive"></div>
            </div>
          ) : (
            <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-green-500/10">
              <div className="relative flex h-3 w-3">
                <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-green-400 opacity-75"></span>
                <span className="relative inline-flex h-3 w-3 rounded-full bg-green-500"></span>
              </div>
            </div>
          )}
          <div className="grid flex-1 text-left text-sm leading-tight">
            <span className="truncate font-medium">Leader</span>
            <span className="truncate font-mono text-xs text-muted-foreground">{shortLeaderId}</span>
          </div>
        </SidebarMenuButton>
        {state === "expanded" && (
          <div className="px-4 pb-2">
            <div className="space-y-1.5 text-xs">
              <div className="flex justify-between gap-2">
                <span className="text-muted-foreground">ID:</span>
                <span className="font-mono text-right">{shortLeaderId}</span>
              </div>
              <div className="flex justify-between gap-2">
                <span className="text-muted-foreground">Elected:</span>
                <span className="text-right">
                  <LiveTime date={new Date(leader.electedAt)} />
                </span>
              </div>
              <div className="flex justify-between gap-2">
                <span className="text-muted-foreground">Expires:</span>
                <span className="text-right">
                  <LiveTime date={new Date(leader.expiresAt)} alwaysUpdate />
                </span>
              </div>
            </div>
          </div>
        )}
      </SidebarMenuItem>
    </SidebarMenu>
  );
}
