import React, { useState } from 'react';
import { NavLink } from 'react-router-dom';
import {
  LayoutDashboard,
  Search,
  Building2,
  FileText,
  TrendingUp,
  Database,
  Download,
  Settings2,
} from 'lucide-react';
import { clsx, type ClassValue } from 'clsx';
import { twMerge } from 'tailwind-merge';
import { OrgConfigPanel } from '../OrgConfigPanel';

function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

const navItems = [
  { icon: LayoutDashboard, label: 'Dashboard', path: '/' },
  { icon: Search, label: 'Enrollment Explorer', path: '/explorer' },
  { icon: Building2, label: 'Parent Organizations', path: '/organizations' },
  { icon: FileText, label: 'Plans', path: '/plans' },
  { icon: TrendingUp, label: 'Growth & AEP', path: '/growth' },
  { icon: Database, label: 'Data Management', path: '/data' },
  { icon: Download, label: 'Exports', path: '/exports' },
];

export const NavRail: React.FC = () => {
  const [showConfig, setShowConfig] = useState(false);

  return (
    <>
      <nav className="w-64 bg-slate-900 border-r border-slate-800 flex flex-col shrink-0">
        <div className="p-6 flex items-center gap-3">
          <div className="w-8 h-8 bg-sky-500 rounded-lg flex items-center justify-center font-bold text-white">MA</div>
          <span className="font-bold text-xl tracking-tight text-white">MA Store</span>
        </div>

        <div className="flex-1 px-3 py-4 space-y-1">
          {navItems.map((item) => (
            <NavLink
              key={item.path}
              to={item.path}
              className={({ isActive }) => cn(
                "flex items-center gap-3 px-3 py-2 rounded-lg transition-colors text-sm font-medium",
                isActive
                  ? "bg-sky-500/10 text-sky-400"
                  : "text-slate-400 hover:text-white hover:bg-slate-800"
              )}
            >
              <item.icon className="w-5 h-5" />
              {item.label}
            </NavLink>
          ))}
        </div>

        <div className="p-4 border-t border-slate-800 space-y-2">
          <button
            onClick={() => setShowConfig(true)}
            className="w-full flex items-center gap-2.5 px-3 py-2 rounded-lg text-sm font-medium text-slate-400 hover:text-white hover:bg-slate-800 transition-colors"
          >
            <Settings2 className="w-5 h-5 text-sky-500" />
            Configure Display
          </button>
          <div className="text-xs text-slate-500 font-medium px-3 uppercase tracking-wider">v0.1.0-alpha</div>
        </div>
      </nav>

      {showConfig && <OrgConfigPanel onClose={() => setShowConfig(false)} />}
    </>
  );
};
