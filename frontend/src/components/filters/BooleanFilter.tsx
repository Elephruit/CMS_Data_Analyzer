import React from 'react';
import { clsx, type ClassValue } from 'clsx';
import { twMerge } from 'tailwind-merge';

function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

interface BooleanFilterProps {
  label: string;
  value: boolean | null;
  onChange: (value: boolean | null) => void;
}

export const BooleanFilter: React.FC<BooleanFilterProps> = ({ label, value, onChange }) => {
  return (
    <div className="flex flex-col gap-1.5 min-w-[100px]">
      <label className="text-[10px] font-bold uppercase tracking-wider text-slate-500 ml-1">
        {label}
      </label>
      <div className="flex bg-slate-800 rounded-lg p-1 border border-slate-700">
        <button
          onClick={() => onChange(value === true ? null : true)}
          className={cn(
            "flex-1 px-3 py-1 rounded-md text-[10px] font-bold transition-all",
            value === true ? "bg-sky-500 text-white shadow-lg shadow-sky-500/20" : "text-slate-400 hover:text-white"
          )}
        >
          YES
        </button>
        <button
          onClick={() => onChange(value === false ? null : false)}
          className={cn(
            "flex-1 px-3 py-1 rounded-md text-[10px] font-bold transition-all",
            value === false ? "bg-slate-600 text-white" : "text-slate-400 hover:text-white"
          )}
        >
          NO
        </button>
      </div>
    </div>
  );
};
