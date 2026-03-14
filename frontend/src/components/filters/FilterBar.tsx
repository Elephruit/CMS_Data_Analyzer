import React, { useEffect, useState } from 'react';
import { FilterDropdown } from './FilterDropdown';
import { useFilters } from '../../context/FilterContext';
import { RotateCcw } from 'lucide-react';

interface Option {
  label: string;
  value: string;
  count?: number;
}

interface FilterOptions {
  states: Option[];
  counties: Option[];
  parentOrgs: Option[];
  contracts: Option[];
  plans: Option[];
}

export const FilterBar: React.FC = () => {
  const { filters, updateFilter, resetFilters } = useFilters();
  const [options, setOptions] = useState<FilterOptions>({
    states: [],
    counties: [],
    parentOrgs: [],
    contracts: [],
    plans: [],
  });
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    const fetchOptions = async () => {
      setLoading(true);
      try {
        const response = await fetch('http://127.0.0.1:3000/api/query/filter-options', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(filters),
        });
        const data = await response.json();
        setOptions(data);
      } catch (error) {
        console.error('Failed to fetch filter options:', error);
      } finally {
        setLoading(false);
      }
    };

    fetchOptions();
  }, [filters.states, filters.counties, filters.parentOrgs]); // Refetch when filters change

  const planTypeOptions = [
    { label: 'HMO', value: 'HMO' },
    { label: 'PPO', value: 'PPO' },
    { label: 'PFFS', value: 'PFFS' },
  ];

  return (
    <div className="bg-slate-900 border-b border-slate-800 px-6 py-4 flex items-end gap-4 overflow-x-auto no-scrollbar">
      <FilterDropdown
        label="State"
        options={options.states}
        selectedValues={filters.states}
        onChange={(vals) => updateFilter('states', vals)}
        placeholder="All States"
        loading={loading}
      />
      
      <FilterDropdown
        label="County"
        options={options.counties}
        selectedValues={filters.counties}
        onChange={(vals) => updateFilter('counties', vals)}
        placeholder="All Counties"
        loading={loading}
      />

      <FilterDropdown
        label="Parent Org"
        options={options.parentOrgs}
        selectedValues={filters.parentOrgs}
        onChange={(vals) => updateFilter('parentOrgs', vals)}
        placeholder="All Organizations"
        loading={loading}
      />

      <FilterDropdown
        label="Contract"
        options={options.contracts}
        selectedValues={filters.contracts}
        onChange={(vals) => updateFilter('contracts', vals)}
        placeholder="All Contracts"
        loading={loading}
      />

      <FilterDropdown
        label="Plan"
        options={options.plans}
        selectedValues={filters.plans}
        onChange={(vals) => updateFilter('plans', vals)}
        placeholder="All Plans"
        loading={loading}
      />

      <FilterDropdown
        label="Plan Type"
        options={planTypeOptions}
        selectedValues={[]}
        onChange={() => {}}
        placeholder="All Types"
      />

      <div className="h-9 w-px bg-slate-800 mx-2 mb-1"></div>

      <button
        onClick={resetFilters}
        className="flex items-center gap-2 px-3 py-2 text-xs font-medium text-slate-400 hover:text-white hover:bg-slate-800 rounded-lg transition-colors mb-0.5"
      >
        <RotateCcw className="w-3.5 h-3.5" />
        Reset
      </button>
    </div>
  );
};
