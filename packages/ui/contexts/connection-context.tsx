"use client";

import { getLocalStorage, setLocalStorage } from "@/utils/local-storage";
import React, { createContext, useContext, useState } from "react";
import { Connection } from "@/types/connection";

interface ConnectionContextType {
  activeConnection: Connection | null;
  setActiveConnection: (connection: Connection) => void;
  connections: Connection[];
}

const ConnectionContext = createContext<ConnectionContextType | undefined>(undefined);

const STORAGE_KEY = "active_connection";

export function ConnectionProvider({ children, initialConnections }: { children: React.ReactNode; initialConnections: Connection[] }) {
  const [connections] = useState<Connection[]>(initialConnections);

  const [activeConnection, setActiveConnectionState] = useState<Connection | null>(() => {
    const savedConnectionId = getLocalStorage(STORAGE_KEY);
    if (savedConnectionId) {
      const savedConnection = initialConnections.find((conn) => conn.id === savedConnectionId);
      if (savedConnection) {
        return savedConnection;
      }
    }

    return initialConnections[0] || null;
  });

  function setActiveConnection(connection: Connection) {
    setActiveConnectionState(connection);
    setLocalStorage(STORAGE_KEY, connection.id);
  }

  return <ConnectionContext.Provider value={{ activeConnection, setActiveConnection, connections }}>{children}</ConnectionContext.Provider>;
}

export function useConnection() {
  const context = useContext(ConnectionContext);
  if (context === undefined) {
    throw new Error("useConnection must be used within a ConnectionProvider");
  }
  return context;
}
