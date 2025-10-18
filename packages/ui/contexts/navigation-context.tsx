"use client";

import React, { createContext, useContext, useEffect, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";

interface NavigationContextType {
  activePage: string;
  setActivePage: (page: string) => void;
}

const NavigationContext = createContext<NavigationContextType | undefined>(undefined);

export function NavigationProvider({ children }: { children: React.ReactNode }) {
  const searchParams = useSearchParams();
  const router = useRouter();
  const pageParam = searchParams.get("page");
  const [activePage, setActivePageState] = useState(pageParam || "Dashboard");

  useEffect(() => {
    if (pageParam && pageParam !== activePage) {
      setActivePageState(pageParam);
    }
  }, [pageParam, activePage]);

  function setActivePage(page: string) {
    setActivePageState(page);
    router.push(`?page=${encodeURIComponent(page)}`);
  }

  return <NavigationContext.Provider value={{ activePage, setActivePage }}>{children}</NavigationContext.Provider>;
}

export function useNavigation() {
  const context = useContext(NavigationContext);
  if (context === undefined) {
    throw new Error("useNavigation must be used within a NavigationProvider");
  }
  return context;
}
