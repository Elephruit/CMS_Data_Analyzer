import React, { createContext, useCallback, useContext, useState, useEffect, type ReactNode } from 'react';

export interface AvailableMonth {
  year: number;
  month: number;
}

export interface FilterState {
  analysisMonth: string; // YYYY-MM
  dateRange: [string, string];
  states: string[];
  counties: string[];
  parentOrgs: string[];
  contracts: string[];
  plans: string[];
  planTypes: string[];
  eghp: boolean | null;
  snp: boolean | null;
  search: string;
}

interface FilterContextType {
  filters: FilterState;
  setFilters: React.Dispatch<React.SetStateAction<FilterState>>;
  resetFilters: () => void;
  updateFilter: <K extends keyof FilterState>(key: K, value: FilterState[K]) => void;
  availableMonths: AvailableMonth[];
  refreshAvailableMonths: () => Promise<void>;
}

const defaultFilters: FilterState = {
  analysisMonth: '',
  dateRange: ['', ''],
  states: [],
  counties: [],
  parentOrgs: [],
  contracts: [],
  plans: [],
  planTypes: [],
  eghp: null,
  snp: null,
  search: '',
};

const FilterContext = createContext<FilterContextType | undefined>(undefined);

export const FilterProvider: React.FC<{ children: ReactNode }> = ({ children }) => {
  const [filters, setFilters] = useState<FilterState>(defaultFilters);
  const [availableMonths, setAvailableMonths] = useState<AvailableMonth[]>([]);

  const refreshAvailableMonths = useCallback(async () => {
    try {
      const res = await fetch('http://127.0.0.1:3000/api/data/months');
      const data = await res.json() as AvailableMonth[];
      setAvailableMonths(data);
      
      setFilters(prev => {
        if (prev.analysisMonth || data.length === 0) return prev;

        const sorted = [...data].sort((a, b) => (a.year * 100 + a.month) - (b.year * 100 + b.month));
        const latest = sorted[sorted.length - 1];
        const prior = sorted[sorted.length - 2] ?? latest;

        return {
          ...prev,
          analysisMonth: `${latest.year}-${latest.month.toString().padStart(2, '0')}`,
          dateRange: [
            `${prior.year}-${prior.month.toString().padStart(2, '0')}`,
            `${latest.year}-${latest.month.toString().padStart(2, '0')}`,
          ],
        };
      });
    } catch (e) {
      console.error(e);
    }
  }, []);

  useEffect(() => {
    void Promise.resolve().then(refreshAvailableMonths);
  }, [refreshAvailableMonths]);

  const resetFilters = () => {
    const sorted = [...availableMonths].sort((a, b) => (a.year * 100 + a.month) - (b.year * 100 + b.month));
    const latest = sorted[sorted.length - 1];
    const prior = sorted[sorted.length - 2] ?? latest;

    setFilters({
      ...defaultFilters,
      analysisMonth: latest ? `${latest.year}-${latest.month.toString().padStart(2, '0')}` : '',
      dateRange: latest && prior
        ? [
            `${prior.year}-${prior.month.toString().padStart(2, '0')}`,
            `${latest.year}-${latest.month.toString().padStart(2, '0')}`,
          ]
        : ['', ''],
    });
  };

  const updateFilter = <K extends keyof FilterState>(key: K, value: FilterState[K]) => {
    setFilters((prev) => ({ ...prev, [key]: value }));
  };

  return (
    <FilterContext.Provider value={{ filters, setFilters, resetFilters, updateFilter, availableMonths, refreshAvailableMonths }}>
      {children}
    </FilterContext.Provider>
  );
};

// eslint-disable-next-line react-refresh/only-export-components
export const useFilters = () => {
  const context = useContext(FilterContext);
  if (context === undefined) {
    throw new Error('useFilters must be used within a FilterProvider');
  }
  return context;
};
