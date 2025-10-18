"use client";

import React, { createContext, useContext, useState } from "react";
import { Connection } from "@/types/connection";

interface ConnectionContextType {
  activeConnection: Connection | null;
  setActiveConnection: (connection: Connection) => void;
  connections: Connection[];
}

const ConnectionContext = createContext<ConnectionContextType | undefined>(undefined);

export function ConnectionProvider({ children, initialConnections }: { children: React.ReactNode; initialConnections: Connection[] }) {
  const [activeConnection, setActiveConnection] = useState<Connection | null>(initialConnections[0] || null);
  const [connections] = useState<Connection[]>(initialConnections);

  return <ConnectionContext.Provider value={{ activeConnection, setActiveConnection, connections }}>{children}</ConnectionContext.Provider>;
}

export function useConnection() {
  const context = useContext(ConnectionContext);
  if (context === undefined) {
    throw new Error("useConnection must be used within a ConnectionProvider");
  }
  return context;
}
