import React, { createContext, useContext, useState, type ReactNode } from 'react';

export interface FilterState {
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
}

const defaultFilters: FilterState = {
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

  const resetFilters = () => setFilters(defaultFilters);

  const updateFilter = <K extends keyof FilterState>(key: K, value: FilterState[K]) => {
    setFilters((prev) => ({ ...prev, [key]: value }));
  };

  return (
    <FilterContext.Provider value={{ filters, setFilters, resetFilters, updateFilter }}>
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
