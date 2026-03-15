import React from 'react';
import { useLocation } from 'react-router-dom';
import { Database } from 'lucide-react';

const ROUTE_TITLES: Record<string, { title: string; subtitle?: string }> = {
  '/':            { title: 'Dashboard', subtitle: 'Market-wide enrollment metrics and trends' },
  '/explorer':    { title: 'Enrollment Explorer' },
  '/organizations': { title: 'Parent Organizations' },
  '/plans':       { title: 'Plans' },
  '/geography':   { title: 'Geography' },
  '/growth':      { title: 'Growth & AEP' },
  '/data':        { title: 'Data Management' },
  '/exports':     { title: 'Exports' },
};

export const Header: React.FC = () => {
  const location = useLocation();
  const meta = ROUTE_TITLES[location.pathname] ?? { title: 'MA Store' };

  return (
    <header className="h-14 bg-slate-900 border-b border-slate-800 flex items-center justify-between px-6 shrink-0">
      <div>
        <h1 className="text-base font-bold text-white tracking-tight leading-tight">{meta.title}</h1>
        {meta.subtitle && (
          <p className="text-[11px] text-slate-500 leading-none mt-0.5">{meta.subtitle}</p>
        )}
      </div>
      <div className="flex items-center gap-2 px-3 py-1.5 bg-slate-800 rounded-full border border-slate-700">
        <Database className="w-3 h-3 text-sky-500" />
        <span className="text-slate-300 uppercase tracking-widest text-[10px] font-bold">Local Analytical Store</span>
      </div>
    </header>
  );
};
