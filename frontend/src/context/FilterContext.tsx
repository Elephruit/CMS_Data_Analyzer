import React, { createContext, useContext, useState, useEffect, type ReactNode } from 'react';

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
  availableMonths: any[];
  refreshAvailableMonths: () => Promise<void>;
}

const defaultFilters: FilterState = {
  analysisMonth: '2025-02',
  dateRange: ['2025-01', '2025-02'],
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
  const [availableMonths, setAvailableMonths] = useState<any[]>([]);

  const refreshAvailableMonths = async () => {
    try {
      const res = await fetch('http://127.0.0.1:3000/api/data/months');
      const data = await res.json();
      setAvailableMonths(data);
      
      // If no analysis month set, set to latest
      if (data.length > 0 && !filters.analysisMonth) {
        const latest = data[data.length - 1];
        setFilters(prev => ({ 
          ...prev, 
          analysisMonth: `${latest.year}-${latest.month.toString().padStart(2, '0')}` 
        }));
      }
    } catch (e) {
      console.error(e);
    }
  };

  useEffect(() => {
    refreshAvailableMonths();
  }, []);

  const resetFilters = () => setFilters(defaultFilters);

  const updateFilter = <K extends keyof FilterState>(key: K, value: FilterState[K]) => {
    setFilters((prev) => ({ ...prev, [key]: value }));
  };

  return (
    <FilterContext.Provider value={{ filters, setFilters, resetFilters, updateFilter, availableMonths, refreshAvailableMonths }}>
      {children}
    </FilterContext.Provider>
  );
};

export const useFilters = () => {
  const context = useContext(FilterContext);
  if (context === undefined) {
    throw new Error('useFilters must be used within a FilterProvider');
  }
  return context;
};
