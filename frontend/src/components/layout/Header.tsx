import React from 'react';
import { Bell, Info, Search as SearchIcon } from 'lucide-react';

export const Header: React.FC = () => {
  return (
    <header className="h-16 bg-slate-900 border-b border-slate-800 flex items-center justify-between px-6 shrink-0">
      <div className="flex items-center gap-6 flex-1">
        <div className="relative max-w-md w-full">
          <SearchIcon className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500" />
          <input 
            type="text" 
            placeholder="Quick search plans, organizations..." 
            className="w-full bg-slate-800 border-none rounded-lg pl-10 pr-4 py-2 text-sm text-slate-200 placeholder:text-slate-500 focus:ring-2 focus:ring-sky-500/50 outline-none"
          />
        </div>
        
        <div className="flex items-center gap-4 text-xs font-medium">
          <div className="flex items-center gap-2 px-3 py-1 bg-slate-800 rounded-full border border-slate-700">
            <span className="w-2 h-2 bg-green-500 rounded-full animate-pulse"></span>
            <span className="text-slate-300">Dataset: Jan 2025 - Feb 2025</span>
          </div>
        </div>
      </div>
      
      <div className="flex items-center gap-3">
        <button className="p-2 text-slate-400 hover:text-white hover:bg-slate-800 rounded-lg transition-colors">
          <Info className="w-5 h-5" />
        </button>
        <button className="p-2 text-slate-400 hover:text-white hover:bg-slate-800 rounded-lg transition-colors relative">
          <Bell className="w-5 h-5" />
          <span className="absolute top-2 right-2 w-2 h-2 bg-sky-500 rounded-full border-2 border-slate-900"></span>
        </button>
        <div className="w-8 h-8 rounded-full bg-gradient-to-br from-sky-500 to-blue-600 flex items-center justify-center text-xs font-bold text-white ml-2 cursor-pointer shadow-lg shadow-sky-500/20">
          JD
        </div>
      </div>
    </header>
  );
};
