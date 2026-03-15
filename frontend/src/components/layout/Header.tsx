import React from 'react';
import { Database } from 'lucide-react';

export const Header: React.FC = () => {
  return (
    <header className="h-14 bg-slate-900 border-b border-slate-800 flex items-center justify-between px-6 shrink-0">
      <div>
        <h1 className="text-base font-bold text-white tracking-tight">Executive Overview</h1>
        <p className="text-[11px] text-slate-500 leading-none mt-0.5">Market-wide enrollment metrics and trends</p>
      </div>
      <div className="flex items-center gap-2 px-3 py-1.5 bg-slate-800 rounded-full border border-slate-700">
        <Database className="w-3 h-3 text-sky-500" />
        <span className="text-slate-300 uppercase tracking-widest text-[10px] font-bold">Local Analytical Store</span>
      </div>
    </header>
  );
};
