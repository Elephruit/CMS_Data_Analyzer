import React, { useState } from 'react';
import { Bell, Info, Search as SearchIcon, Database } from 'lucide-react';
import { useNavigate } from 'react-router-dom';

export const Header: React.FC = () => {
  const [searchQuery, setSearchSearchQuery] = useState('');
  const navigate = useNavigate();

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    if (searchQuery.includes('|') || searchQuery.match(/^[HS]\d{4}/)) {
      navigate('/plans');
      // In a real app, we'd pass the search query to the PlanDetail page via state or URL
    }
  };

  return (
    <header className="h-16 bg-slate-900 border-b border-slate-800 flex items-center justify-between px-6 shrink-0">
      <div className="flex items-center gap-6 flex-1">
        <form onSubmit={handleSearch} className="relative max-w-md w-full">
          <SearchIcon className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500" />
          <input 
            type="text" 
            value={searchQuery}
            onChange={(e) => setSearchSearchQuery(e.target.value)}
            placeholder="Quick search plans (e.g. H5425|087)..." 
            className="w-full bg-slate-800 border-none rounded-lg pl-10 pr-4 py-2 text-sm text-slate-200 placeholder:text-slate-500 focus:ring-2 focus:ring-sky-500/50 outline-none"
          />
        </form>
        
        <div className="flex items-center gap-4 text-xs font-medium">
          <div className="flex items-center gap-2 px-3 py-1 bg-slate-800 rounded-full border border-slate-700">
            <Database className="w-3 h-3 text-sky-500" />
            <span className="text-slate-300 uppercase tracking-widest text-[10px] font-bold">Local Analytical Store</span>
          </div>
        </div>
      </div>
    </header>
  );
};
