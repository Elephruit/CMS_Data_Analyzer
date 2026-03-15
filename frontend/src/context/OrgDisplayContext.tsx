import React, { createContext, useContext, useState, useCallback } from 'react';

export interface OrgDisplayConfig {
  displayName?: string;
  color?: string;
}

interface OrgDisplayContextType {
  configs: Record<string, OrgDisplayConfig>;
  getDisplayName: (rawName: string) => string;
  getColor: (rawName: string, fallback: string) => string;
  setConfig: (rawName: string, patch: Partial<OrgDisplayConfig>) => void;
  resetConfig: (rawName: string) => void;
}

const STORAGE_KEY = 'org_display_configs';

const OrgDisplayContext = createContext<OrgDisplayContextType | null>(null);

function loadFromStorage(): Record<string, OrgDisplayConfig> {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    return raw ? JSON.parse(raw) : {};
  } catch {
    return {};
  }
}

export const OrgDisplayProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [configs, setConfigs] = useState<Record<string, OrgDisplayConfig>>(loadFromStorage);

  const persist = useCallback((next: Record<string, OrgDisplayConfig>) => {
    setConfigs(next);
    localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
  }, []);

  const setConfig = useCallback((rawName: string, patch: Partial<OrgDisplayConfig>) => {
    persist({ ...configs, [rawName]: { ...configs[rawName], ...patch } });
  }, [configs, persist]);

  const resetConfig = useCallback((rawName: string) => {
    const next = { ...configs };
    delete next[rawName];
    persist(next);
  }, [configs, persist]);

  const getDisplayName = useCallback((rawName: string): string => {
    const custom = configs[rawName]?.displayName?.trim();
    return custom || rawName;
  }, [configs]);

  const getColor = useCallback((rawName: string, fallback: string): string => {
    return configs[rawName]?.color || fallback;
  }, [configs]);

  return (
    <OrgDisplayContext.Provider value={{ configs, getDisplayName, getColor, setConfig, resetConfig }}>
      {children}
    </OrgDisplayContext.Provider>
  );
};

export function useOrgDisplay(): OrgDisplayContextType {
  const ctx = useContext(OrgDisplayContext);
  if (!ctx) throw new Error('useOrgDisplay must be used within OrgDisplayProvider');
  return ctx;
}
