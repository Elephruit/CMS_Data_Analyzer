import React, { useState, useRef, useEffect, useMemo } from 'react';
import { ChevronDown, Search, X, Check } from 'lucide-react';
import { clsx, type ClassValue } from 'clsx';
import { twMerge } from 'tailwind-merge';

function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

interface Option {
  label: string;
  value: string;
  count?: number;
}

interface FilterDropdownProps {
  label: string;
  options: Option[];
  selectedValues: string[];
  onChange: (values: string[]) => void;
  placeholder?: string;
  loading?: boolean;
}

export const FilterDropdown: React.FC<FilterDropdownProps> = ({
  label,
  options,
  selectedValues,
  onChange,
  placeholder = 'Select...',
  loading = false,
}) => {
  const [isOpen, setIsOpen] = useState(false);
  const [searchQuery, setSearchSearchQuery] = useState('');
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(event.target as Node)) {
        setIsOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const filteredOptions = useMemo(() => options.filter((option) =>
    option.label.toLowerCase().includes(searchQuery.toLowerCase())
  ), [options, searchQuery]);

  const toggleOption = (value: string) => {
    const newValues = selectedValues.includes(value)
      ? selectedValues.filter((v) => v !== value)
      : [...selectedValues, value];
    onChange(newValues);
  };

  const toggleAll = () => {
    if (selectedValues.length === options.length && options.length > 0) {
      onChange([]);
    } else {
      onChange(options.map(o => o.value));
    }
  };

  const clearSelection = (e: React.MouseEvent) => {
    e.stopPropagation();
    onChange([]);
  };

  const isAllSelected = options.length > 0 && selectedValues.length === options.length;

  const labelText = useMemo(() => {
    if (selectedValues.length === 0) return placeholder;
    if (isAllSelected) return `All ${label}s`;
    if (selectedValues.length === 1) {
      const opt = options.find(o => o.value === selectedValues[0]);
      return opt ? opt.label : selectedValues[0];
    }
    return `${selectedValues.length} Selected`;
  }, [selectedValues, options, isAllSelected, label, placeholder]);

  return (
    <div className="relative" ref={containerRef}>
      <div className="flex flex-col gap-1.5">
        <label className="text-[10px] font-bold uppercase tracking-wider text-slate-500 ml-1">
          {label}
        </label>
        <button
          onClick={() => setIsOpen(!isOpen)}
          className={cn(
            "flex items-center justify-between gap-2 px-3 py-2 bg-slate-800 border rounded-lg text-sm transition-all min-w-[160px] max-w-[220px]",
            isOpen ? "border-sky-500 ring-2 ring-sky-500/20" : "border-slate-700 hover:border-slate-600",
            selectedValues.length > 0 ? "text-white" : "text-slate-400"
          )}
        >
          <span className="truncate flex-1 text-left">
            {labelText}
          </span>
          <div className="flex items-center gap-1 shrink-0 ml-2">
            {selectedValues.length > 0 && (
              <X className="w-3 h-3 hover:text-white text-slate-500" onClick={clearSelection} />
            )}
            <ChevronDown className={cn("w-4 h-4 transition-transform text-slate-500", isOpen && "rotate-180")} />
          </div>
        </button>
      </div>

      {isOpen && (
        <div className="absolute z-50 mt-2 w-72 bg-slate-800 border border-slate-700 rounded-xl shadow-2xl shadow-black/50 overflow-hidden animate-in fade-in zoom-in-95 duration-100">
          <div className="p-2 border-b border-slate-700 space-y-2 bg-slate-800/50">
            <div className="relative">
              <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-slate-500" />
              <input
                type="text"
                placeholder="Search..."
                value={searchQuery}
                onChange={(e) => setSearchSearchQuery(e.target.value)}
                className="w-full bg-slate-900 border-none rounded-md pl-8 pr-3 py-1.5 text-xs text-slate-200 outline-none focus:ring-1 focus:ring-sky-500/50"
                autoFocus
              />
            </div>
            {!searchQuery && options.length > 0 && (
              <button
                onClick={toggleAll}
                className="w-full flex items-center gap-2 px-3 py-1.5 rounded-md text-[10px] font-black bg-slate-700/50 text-slate-300 hover:bg-slate-700 hover:text-white transition-colors uppercase tracking-widest"
              >
                <div className={cn(
                  "w-3.5 h-3.5 rounded border flex items-center justify-center shrink-0",
                  isAllSelected ? "bg-sky-500 border-sky-500" : "border-slate-500"
                )}>
                  {isAllSelected && <Check className="w-2.5 h-2.5 text-white" />}
                </div>
                {isAllSelected ? 'Deselect All' : 'Select All'}
              </button>
            )}
          </div>
          <div className="max-h-80 overflow-y-auto p-1 custom-scrollbar bg-slate-900/20">
            {filteredOptions.length === 0 ? (
              <div className="py-8 text-center text-xs text-slate-500 italic">
                {loading ? 'Syncing...' : 'No matches found'}
              </div>
            ) : (
              <>
                {loading && (
                  <div className="absolute top-1 right-1 px-2 py-0.5 bg-sky-500/10 rounded text-[8px] font-black text-sky-400 uppercase tracking-widest animate-pulse border border-sky-500/20 z-10">
                    Syncing
                  </div>
                )}
                {filteredOptions.map((option) => (
                  <button
                    key={option.value}
                    onClick={(e) => {
                      e.preventDefault();
                      toggleOption(option.value);
                    }}
                    className={cn(
                      "w-full flex items-center justify-between px-3 py-2 rounded-md text-xs transition-colors mb-0.5",
                      selectedValues.includes(option.value)
                        ? "bg-sky-500/10 text-sky-400 font-bold"
                        : "text-slate-400 hover:bg-slate-700/50 hover:text-slate-200"
                    )}
                  >
                    <div className="flex items-center gap-2 truncate flex-1">
                      <div className={cn(
                        "w-3.5 h-3.5 rounded border flex items-center justify-center shrink-0 transition-all",
                        selectedValues.includes(option.value) ? "bg-sky-500 border-sky-500 scale-110" : "border-slate-600"
                      )}>
                        {selectedValues.includes(option.value) && <Check className="w-2.5 h-2.5 text-white" />}
                      </div>
                      <span className="truncate">{option.label}</span>
                    </div>
                    {option.count !== undefined && (
                      <span className="text-[9px] font-mono text-slate-600 ml-2 bg-slate-800/50 px-1 rounded">
                        {option.count.toLocaleString()}
                      </span>
                    )}
                  </button>
                ))}
              </>
            )}
          </div>
        </div>
      )}
    </div>
  );
};
