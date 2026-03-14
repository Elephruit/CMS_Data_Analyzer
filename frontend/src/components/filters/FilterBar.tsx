import React, { useEffect, useState, useMemo } from 'react';
import { FilterDropdown } from './FilterDropdown';
import { BooleanFilter } from './BooleanFilter';
import { useFilters } from '../../context/FilterContext';
import { RotateCcw, CalendarDays } from 'lucide-react';

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
  planTypes: Option[];
}

export const FilterBar: React.FC = () => {
  const { filters, updateFilter, resetFilters } = useFilters();
  const [options, setOptions] = useState<FilterOptions>({
    states: [],
    counties: [],
    parentOrgs: [],
    contracts: [],
    plans: [],
    planTypes: [],
  });
  const [availableMonths, setAvailableMonths] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    fetch('http://127.0.0.1:3000/api/data/months')
      .then(res => res.json())
      .then(data => setAvailableMonths(data))
      .catch(err => console.error(err));
  }, []);

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
  }, [filters.states, filters.counties, filters.parentOrgs, filters.contracts, filters.plans, filters.planTypes, filters.eghp, filters.snp]); 

  const monthOptions = useMemo(() => {
    return availableMonths.map(m => {
      const label = new Date(m.year, m.month - 1).toLocaleString('default', { month: 'short', year: 'numeric' });
      const value = `${m.year}-${m.month.toString().padStart(2, '0')}`;
      return { label, value };
    }).reverse();
  }, [availableMonths]);

  return (
    <div className="bg-slate-900 border-b border-slate-800 px-6 py-4 flex items-end gap-4 z-30 relative overflow-visible">
      {/* Analysis Month Selector */}
      <div className="flex flex-col gap-1.5 min-w-[140px]">
        <label className="text-[10px] font-bold uppercase tracking-wider text-sky-500 ml-1 flex items-center gap-1.5">
          <CalendarDays className="w-3 h-3" />
          Analysis Month
        </label>
        <select
          value={filters.analysisMonth}
          onChange={(e) => updateFilter('analysisMonth', e.target.value)}
          className="bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-sm text-white focus:border-sky-500 outline-none transition-all cursor-pointer"
        >
          {monthOptions.map(opt => (
            <option key={opt.value} value={opt.value}>{opt.label}</option>
          ))}
        </select>
      </div>

      <div className="h-9 w-px bg-slate-800 mx-1 mb-1"></div>

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
        options={options.planTypes}
        selectedValues={filters.planTypes}
        onChange={(vals) => updateFilter('planTypes', vals)}
        placeholder="All Types"
        loading={loading}
      />

      <BooleanFilter 
        label="EGWP"
        value={filters.eghp}
        onChange={(val) => updateFilter('eghp', val)}
      />

      <BooleanFilter 
        label="SNP"
        value={filters.snp}
        onChange={(val) => updateFilter('snp', val)}
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
