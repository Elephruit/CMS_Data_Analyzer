import React, { useEffect, useState, useCallback } from 'react';
import { useFilters } from '../context/FilterContext';
import { useOrgDisplay } from '../context/OrgDisplayContext';
import { Settings2, X, RotateCcw } from 'lucide-react';

const DEFAULT_COLORS = [
  '#0ea5e9', '#10b981', '#f59e0b', '#8b5cf6', '#ec4899',
  '#14b8a6', '#f97316', '#6366f1', '#22c55e', '#e11d48',
];

const SWATCH_PALETTE = [
  '#0ea5e9', '#38bdf8', '#06b6d4', '#14b8a6', '#10b981',
  '#22c55e', '#84cc16', '#eab308', '#f97316', '#ef4444',
  '#e11d48', '#ec4899', '#a855f7', '#8b5cf6', '#6366f1',
  '#f59e0b', '#64748b', '#94a3b8',
];

interface OrgItem {
  name: string;
  marketShare: number;
}

interface OrgConfigPanelProps {
  onClose: () => void;
}

export const OrgConfigPanel: React.FC<OrgConfigPanelProps> = ({ onClose }) => {
  const { filters } = useFilters();
  const { configs, getDisplayName, setConfig, resetConfig } = useOrgDisplay();
  const [orgs, setOrgs] = useState<OrgItem[]>([]);
  const [localNames, setLocalNames] = useState<Record<string, string>>({});

  useEffect(() => {
    fetch('http://127.0.0.1:3000/api/query/organization-analysis', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(filters),
    })
      .then(r => r.json())
      .then(data => {
        const items: OrgItem[] = (data.organizations || []).map((o: any) => ({
          name: o.name,
          marketShare: o.marketShare,
        }));
        setOrgs(items);
        setLocalNames(
          Object.fromEntries(items.map(o => [o.name, configs[o.name]?.displayName ?? '']))
        );
      })
      .catch(() => {});
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filters]);

  const handleNameBlur = useCallback((rawName: string) => {
    setConfig(rawName, { displayName: localNames[rawName] });
  }, [localNames, setConfig]);

  const handleColorPick = useCallback((rawName: string, color: string) => {
    setConfig(rawName, { color });
  }, [setConfig]);

  const handleReset = useCallback((rawName: string) => {
    resetConfig(rawName);
    setLocalNames(prev => ({ ...prev, [rawName]: '' }));
  }, [resetConfig]);

  const hasCustomization = useCallback((rawName: string) => {
    const c = configs[rawName];
    return !!(c?.displayName?.trim() || c?.color);
  }, [configs]);

  return (
    <>
      {/* Backdrop */}
      <div
        className="fixed inset-0 bg-black/50 z-40 backdrop-blur-[2px]"
        onClick={onClose}
      />

      {/* Drawer */}
      <div className="fixed right-0 top-0 bottom-0 w-[460px] bg-slate-950 border-l border-slate-800 z-50 flex flex-col shadow-2xl">

        {/* Header */}
        <div className="flex items-start justify-between p-5 border-b border-slate-800 shrink-0">
          <div>
            <div className="flex items-center gap-2 mb-1">
              <Settings2 className="w-4 h-4 text-sky-500" />
              <h2 className="text-sm font-bold text-white tracking-tight">Organization Display Settings</h2>
            </div>
            <p className="text-[11px] text-slate-500 leading-relaxed max-w-xs">
              Customize display names and brand colors. Changes apply live across all charts and tables. Source data is never modified.
            </p>
          </div>
          <button
            onClick={onClose}
            className="p-1.5 rounded-lg text-slate-500 hover:text-white hover:bg-slate-800 transition-colors"
          >
            <X className="w-4 h-4" />
          </button>
        </div>

        {/* Org list */}
        <div className="flex-1 overflow-y-auto">
          {orgs.length === 0 ? (
            <div className="flex items-center justify-center h-full text-slate-600 text-sm italic">
              No organizations loaded for current filters.
            </div>
          ) : (
            <div className="divide-y divide-slate-800/60">
              {orgs.map((org, i) => {
                const defaultColor = DEFAULT_COLORS[i % DEFAULT_COLORS.length];
                const currentColor = configs[org.name]?.color || defaultColor;
                const isCustomized = hasCustomization(org.name);
                const displayName = getDisplayName(org.name);

                return (
                  <div key={org.name} className="p-4 group hover:bg-slate-900/40 transition-colors">
                    {/* Row header */}
                    <div className="flex items-center justify-between mb-3">
                      <div className="flex items-center gap-2.5 min-w-0">
                        <div
                          className="w-3 h-3 rounded-full shrink-0 ring-2 ring-offset-2 ring-offset-slate-950"
                          style={{ backgroundColor: currentColor }}
                        />
                        <div className="min-w-0">
                          <div className="text-xs font-semibold text-white truncate">{displayName}</div>
                          {org.name !== displayName ? (
                            <div className="text-[10px] text-slate-600 truncate" title={org.name}>{org.name}</div>
                          ) : (
                            <div className="text-[10px] text-slate-700 truncate">source name</div>
                          )}
                        </div>
                      </div>
                      <div className="flex items-center gap-2 shrink-0">
                        <span className="text-[10px] text-slate-600 font-mono">
                          {org.marketShare.toFixed(1)}% share
                        </span>
                        {isCustomized && (
                          <button
                            onClick={() => handleReset(org.name)}
                            className="flex items-center gap-1 text-[10px] text-slate-500 hover:text-rose-400 transition-colors"
                            title="Reset to defaults"
                          >
                            <RotateCcw className="w-3 h-3" />
                            Reset
                          </button>
                        )}
                      </div>
                    </div>

                    {/* Display name input */}
                    <div className="mb-3">
                      <label className="text-[10px] font-bold text-slate-500 uppercase tracking-widest block mb-1">
                        Display Name
                      </label>
                      <input
                        type="text"
                        value={localNames[org.name] ?? ''}
                        placeholder={org.name}
                        onChange={(e) => setLocalNames(prev => ({ ...prev, [org.name]: e.target.value }))}
                        onBlur={() => handleNameBlur(org.name)}
                        className="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-1.5 text-xs text-slate-200 placeholder:text-slate-600 focus:ring-1 focus:ring-sky-500/50 focus:border-sky-500/50 outline-none transition-colors"
                      />
                    </div>

                    {/* Color picker */}
                    <div>
                      <label className="text-[10px] font-bold text-slate-500 uppercase tracking-widest block mb-1.5">
                        Brand Color
                      </label>
                      <div className="flex flex-wrap gap-1.5">
                        {SWATCH_PALETTE.map(color => (
                          <button
                            key={color}
                            onClick={() => handleColorPick(org.name, color)}
                            className="w-5 h-5 rounded-full transition-transform hover:scale-110 focus:outline-none"
                            style={{ backgroundColor: color }}
                            title={color}
                          >
                            {currentColor === color && (
                              <span className="flex items-center justify-center w-full h-full">
                                <span className="w-1.5 h-1.5 bg-white rounded-full opacity-90" />
                              </span>
                            )}
                          </button>
                        ))}
                        <div className="flex items-center gap-1 ml-1">
                          <div
                            className="w-5 h-5 rounded-full border border-slate-700 shrink-0"
                            style={{ backgroundColor: currentColor }}
                          />
                          <input
                            type="text"
                            value={currentColor}
                            placeholder="#hex"
                            onChange={(e) => {
                              const val = e.target.value;
                              if (/^#[0-9a-fA-F]{6}$/.test(val)) {
                                handleColorPick(org.name, val);
                              }
                            }}
                            className="w-20 bg-slate-900 border border-slate-700 rounded px-2 py-0.5 text-[10px] font-mono text-slate-300 outline-none focus:border-sky-500/50"
                          />
                        </div>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="p-4 border-t border-slate-800 shrink-0">
          <p className="text-[10px] text-slate-600">
            Settings are saved automatically and persist across sessions.
          </p>
        </div>
      </div>
    </>
  );
};
