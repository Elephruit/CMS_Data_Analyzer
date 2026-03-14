import React, { useState, useRef, useEffect, useMemo } from 'react';
import { ChevronDown, CalendarDays, Check } from 'lucide-react';
import { clsx, type ClassValue } from 'clsx';
import { twMerge } from 'tailwind-merge';

function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

interface Option {
  label: string;
  value: string;
}

interface AnalysisMonthSelectorProps {
  options: Option[];
  selectedValue: string;
  onChange: (value: string) => void;
}

export const AnalysisMonthSelector: React.FC<AnalysisMonthSelectorProps> = ({
  options,
  selectedValue,
  onChange,
}) => {
  const [isOpen, setIsOpen] = useState(false);
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

  const selectedLabel = useMemo(() => {
    return options.find(o => o.value === selectedValue)?.label || selectedValue;
  }, [options, selectedValue]);

  return (
    <div className="relative" ref={containerRef}>
      <div className="flex flex-col gap-1.5">
        <label className="text-[10px] font-bold uppercase tracking-wider text-sky-500 ml-1 flex items-center gap-1.5">
          <CalendarDays className="w-3 h-3" />
          Analysis Month
        </label>
        <button
          onClick={() => setIsOpen(!isOpen)}
          className={cn(
            "flex items-center justify-between gap-2 px-3 py-2 bg-slate-800 border rounded-lg text-sm transition-all min-w-[160px] border-slate-700 hover:border-slate-600 text-white shadow-lg shadow-sky-500/5",
            isOpen && "border-sky-500 ring-2 ring-sky-500/20"
          )}
        >
          <span className="truncate font-bold">{selectedLabel}</span>
          <ChevronDown className={cn("w-4 h-4 transition-transform text-slate-500", isOpen && "rotate-180")} />
        </button>
      </div>

      {isOpen && (
        <div className="absolute z-50 mt-2 w-56 bg-slate-800 border border-slate-700 rounded-xl shadow-2xl shadow-black/50 overflow-hidden animate-in fade-in zoom-in-95 duration-100">
          <div className="max-h-80 overflow-y-auto p-1 custom-scrollbar bg-slate-900/20">
            {options.length === 0 ? (
              <div className="py-8 text-center text-xs text-slate-500 italic">No months available</div>
            ) : (
              options.map((option) => (
                <button
                  key={option.value}
                  onClick={() => {
                    onChange(option.value);
                    setIsOpen(false);
                  }}
                  className={cn(
                    "w-full flex items-center justify-between px-3 py-2.5 rounded-md text-xs transition-colors mb-0.5",
                    selectedValue === option.value
                      ? "bg-sky-500/10 text-sky-400 font-bold"
                      : "text-slate-400 hover:bg-slate-700/50 hover:text-slate-200"
                  )}
                >
                  <span className="truncate">{option.label}</span>
                  {selectedValue === option.value && (
                    <Check className="w-3.5 h-3.5 text-sky-500" />
                  )}
                </button>
              ))
            )}
          </div>
        </div>
      )}
    </div>
  );
};
