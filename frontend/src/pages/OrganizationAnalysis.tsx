import React, { useEffect, useState, useMemo, useCallback } from 'react';
import { useFilters } from '../context/FilterContext';
import { useOrgDisplay } from '../context/OrgDisplayContext';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
  LineChart,
  Line,
  Legend,
} from 'recharts';
import {
  Building2,
  TrendingUp,
  PieChart as PieChartIcon,
  ArrowUpRight,
  Download,
  Settings2,
  X,
  RotateCcw,
} from 'lucide-react';
import { formatEnrollment } from '../utils/formatters';

// ── Default palette ────────────────────────────────────────────────────────────
const DEFAULT_COLORS = [
  '#0ea5e9', '#10b981', '#f59e0b', '#8b5cf6', '#ec4899',
  '#14b8a6', '#f97316', '#6366f1', '#22c55e', '#e11d48',
];

// Preset swatches shown in the config panel color picker
const SWATCH_PALETTE = [
  '#0ea5e9', '#38bdf8', '#06b6d4', '#14b8a6', '#10b981',
  '#22c55e', '#84cc16', '#eab308', '#f97316', '#ef4444',
  '#e11d48', '#ec4899', '#a855f7', '#8b5cf6', '#6366f1',
  '#f59e0b', '#64748b', '#94a3b8',
];

// ── Types ──────────────────────────────────────────────────────────────────────
interface OrgTrendPoint { month: number; value: number; }
interface Organization {
  name: string;
  enrollment: number;
  marketShare: number;
  trend: OrgTrendPoint[];
}
interface OrgAnalysisData {
  totalMarketEnrollment: number;
  latestMonth: number;
  organizations: Organization[];
}

// ── Config panel ───────────────────────────────────────────────────────────────
interface OrgConfigPanelProps {
  orgs: Organization[];
  onClose: () => void;
}

const OrgConfigPanel: React.FC<OrgConfigPanelProps> = ({ orgs, onClose }) => {
  const { configs, getDisplayName, setConfig, resetConfig } = useOrgDisplay();
  // Local display-name state so we only push to context on blur, avoiding
  // re-rendering every chart on each keystroke.
  const [localNames, setLocalNames] = useState<Record<string, string>>(() =>
    Object.fromEntries(orgs.map(o => [o.name, configs[o.name]?.displayName ?? '']))
  );

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
                        {/* Color indicator */}
                        <div
                          className="w-3 h-3 rounded-full shrink-0 ring-2 ring-offset-2 ring-offset-slate-950"
                          style={{ backgroundColor: currentColor, outlineColor: currentColor }}
                        />
                        <div className="min-w-0">
                          <div className="text-xs font-semibold text-white truncate">{displayName}</div>
                          {org.name !== displayName && (
                            <div className="text-[10px] text-slate-600 truncate" title={org.name}>
                              {org.name}
                            </div>
                          )}
                          {org.name === displayName && (
                            <div className="text-[10px] text-slate-700 truncate" title={org.name}>
                              source name
                            </div>
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
                        {/* Custom hex input */}
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

// ── Main page ──────────────────────────────────────────────────────────────────
export const OrganizationAnalysis: React.FC = () => {
  const { filters } = useFilters();
  const { getDisplayName, getColor } = useOrgDisplay();
  const [data, setData] = useState<OrgAnalysisData | null>(null);
  const [loading, setLoading] = useState(true);
  const [showConfig, setShowConfig] = useState(false);

  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      try {
        const response = await fetch('http://127.0.0.1:3000/api/query/organization-analysis', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(filters),
        });
        setData(await response.json());
      } catch (error) {
        console.error('Failed to fetch org analysis:', error);
      } finally {
        setLoading(false);
      }
    };
    fetchData();
  }, [filters]);

  const top10 = useMemo(() => data?.organizations.slice(0, 10) || [], [data]);
  const top5 = useMemo(() => data?.organizations.slice(0, 5) || [], [data]);

  const concentration = useMemo(() => {
    if (!data) return { top3: 0, top5: 0, top10: 0 };
    const total = data.totalMarketEnrollment;
    const sum = (orgs: Organization[]) => orgs.reduce((s, o) => s + o.enrollment, 0);
    return {
      top3: (sum(data.organizations.slice(0, 3)) / total) * 100,
      top5: (sum(data.organizations.slice(0, 5)) / total) * 100,
      top10: (sum(data.organizations.slice(0, 10)) / total) * 100,
    };
  }, [data]);

  // Bar chart data — use display names as the axis label
  const barData = useMemo(() =>
    top10.map((org, i) => ({
      rawName: org.name,
      displayName: getDisplayName(org.name),
      marketShare: org.marketShare,
      enrollment: org.enrollment,
      color: getColor(org.name, DEFAULT_COLORS[i % DEFAULT_COLORS.length]),
    })),
  [top10, getDisplayName, getColor]);

  // Line chart trend data — keyed by raw name, displayed via Line name prop
  const trendData = useMemo(() => {
    if (!data || top5.length === 0) return [];
    const months = Array.from(new Set(top5.flatMap(o => o.trend.map(t => t.month)))).sort();
    return months.map(m => {
      const point: Record<string, any> = {
        month: m.toString().replace(/(\d{4})(\d{2})/, '$1-$2'),
      };
      top5.forEach(o => {
        const t = o.trend.find(tp => tp.month === m);
        point[o.name] = t ? t.value : 0;
      });
      return point;
    });
  }, [data, top5]);

  const exportToCSV = () => {
    if (!data) return;
    const headers = ['Display Name', 'Source Name', 'Enrollment', 'Market Share %'];
    const csvContent = [
      headers.join(','),
      ...data.organizations.map(org => [
        `"${getDisplayName(org.name)}"`,
        `"${org.name}"`,
        org.enrollment,
        org.marketShare.toFixed(2),
      ].join(','))
    ].join('\n');
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    link.setAttribute('href', URL.createObjectURL(blob));
    link.setAttribute('download', `org_analysis_${new Date().toISOString().split('T')[0]}.csv`);
    link.style.visibility = 'hidden';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="flex flex-col items-center gap-4">
          <div className="w-12 h-12 border-4 border-sky-500 border-t-transparent rounded-full animate-spin" />
          <span className="text-slate-400 font-bold uppercase tracking-widest text-xs">Analyzing Market Structure...</span>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6 max-w-[1600px] mx-auto pb-12">

      {/* Page header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white tracking-tight">Parent Organization Analysis</h1>
          <p className="text-slate-400 text-sm mt-1">Market dominance and organizational growth trends.</p>
        </div>
        <div className="flex items-center gap-3">
          <div className="px-4 py-2 bg-slate-900 border border-slate-800 rounded-xl flex items-center gap-3">
            <div className="w-2 h-2 bg-sky-500 rounded-full animate-pulse" />
            <span className="text-xs font-bold text-slate-300 uppercase tracking-wider">
              Market Size: {(data?.totalMarketEnrollment || 0).toLocaleString()}
            </span>
          </div>
          <button
            onClick={() => setShowConfig(true)}
            className="flex items-center gap-2 px-4 py-2 bg-slate-900 border border-slate-700 hover:border-sky-500/50 hover:bg-slate-800 rounded-xl text-xs font-bold text-slate-300 hover:text-white transition-all"
          >
            <Settings2 className="w-3.5 h-3.5 text-sky-500" />
            Configure Display
          </button>
        </div>
      </div>

      {/* Concentration tiles */}
      <div className="grid grid-cols-3 gap-6">
        {[
          { label: 'Top 3 Org Share', value: concentration.top3, color: 'text-sky-400', icon: Building2 },
          { label: 'Top 5 Org Share', value: concentration.top5, color: 'text-emerald-400', icon: PieChartIcon },
          { label: 'Top 10 Org Share', value: concentration.top10, color: 'text-violet-400', icon: TrendingUp },
        ].map((stat) => (
          <div key={stat.label} className="bg-slate-900 border border-slate-800 p-6 rounded-2xl flex items-center gap-6">
            <div className={`w-12 h-12 rounded-xl bg-slate-800 flex items-center justify-center ${stat.color}`}>
              <stat.icon className="w-6 h-6" />
            </div>
            <div>
              <div className="text-[10px] font-bold text-slate-500 uppercase tracking-widest mb-1">{stat.label}</div>
              <div className="text-3xl font-bold text-white">{stat.value.toFixed(1)}%</div>
            </div>
          </div>
        ))}
      </div>

      {/* Charts row */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">

        {/* Bar chart: Market Share % */}
        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-6 h-[450px] flex flex-col">
          <h2 className="text-xs font-bold text-slate-400 uppercase tracking-widest mb-6">
            Market Share %
          </h2>
          <div className="flex-1 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={barData} layout="vertical" margin={{ left: 0, right: 48, top: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" horizontal={false} vertical={true} />
                <XAxis
                  type="number"
                  axisLine={false}
                  tickLine={false}
                  tick={{ fill: '#64748b', fontSize: 10 }}
                  tickFormatter={(v) => v.toFixed(1) + '%'}
                />
                <YAxis
                  dataKey="displayName"
                  type="category"
                  axisLine={false}
                  tickLine={false}
                  tick={{ fill: '#94a3b8', fontSize: 10, fontWeight: 600 }}
                  width={148}
                />
                <Tooltip
                  cursor={{ fill: '#1e293b' }}
                  contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #334155', borderRadius: '12px' }}
                  itemStyle={{ color: '#f1f5f9', fontSize: '12px' }}
                  labelStyle={{ color: '#94a3b8', fontSize: '10px', fontWeight: 'bold', marginBottom: '4px' }}
                  formatter={(value: any, _name: any, props: any) => [
                    `${value?.toFixed(2)}%  ·  ${props.payload.enrollment.toLocaleString()} members`,
                    'Market Share',
                  ]}
                />
                <Bar dataKey="marketShare" radius={[0, 4, 4, 0]} barSize={22}>
                  {barData.map((entry, i) => (
                    <Cell key={`cell-${i}`} fill={entry.color} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Line chart: Membership trend */}
        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-6 h-[450px] flex flex-col">
          <h2 className="text-xs font-bold text-slate-400 uppercase tracking-widest mb-6">
            Membership by Top 5 Parent Organizations
          </h2>
          <div className="flex-1 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={trendData} margin={{ top: 4, right: 16, bottom: 0, left: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
                <XAxis
                  dataKey="month"
                  axisLine={false}
                  tickLine={false}
                  tick={{ fill: '#64748b', fontSize: 10 }}
                  dy={10}
                />
                <YAxis
                  axisLine={false}
                  tickLine={false}
                  tick={{ fill: '#64748b', fontSize: 10 }}
                  tickFormatter={(val) => formatEnrollment(val)}
                  width={52}
                />
                <Tooltip
                  contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #334155', borderRadius: '12px' }}
                  itemStyle={{ fontSize: '11px' }}
                  formatter={(val: any, rawKey: any) => [
                    (val as number)?.toLocaleString(),
                    getDisplayName(rawKey as string),
                  ]}
                />
                <Legend
                  iconType="circle"
                  wrapperStyle={{ fontSize: '10px', paddingTop: '16px' }}
                  formatter={(rawKey: string) => getDisplayName(rawKey)}
                />
                {top5.map((org, i) => (
                  <Line
                    key={org.name}
                    type="monotone"
                    dataKey={org.name}
                    name={org.name}
                    stroke={getColor(org.name, DEFAULT_COLORS[i % DEFAULT_COLORS.length])}
                    strokeWidth={2.5}
                    dot={{ r: 3, strokeWidth: 2, fill: '#0f172a' }}
                    activeDot={{ r: 5 }}
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      {/* Data table */}
      <div className="bg-slate-900 border border-slate-800 rounded-2xl overflow-hidden shadow-xl">
        <div className="p-5 border-b border-slate-800 flex items-center justify-between">
          <h2 className="text-xs font-bold text-slate-400 uppercase tracking-widest">Organizational Deep-Dive</h2>
          <button
            onClick={exportToCSV}
            className="flex items-center gap-2 px-3 py-1.5 bg-slate-800 hover:bg-slate-700 text-slate-300 text-[10px] font-bold rounded-lg border border-slate-700 transition-all"
          >
            <Download className="w-3.5 h-3.5" />
            EXPORT CSV
          </button>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-left border-collapse">
            <thead>
              <tr className="bg-slate-900/50 border-b border-slate-800">
                <th className="px-6 py-4 text-[10px] font-bold text-slate-500 uppercase tracking-widest">Organization</th>
                <th className="px-6 py-4 text-[10px] font-bold text-slate-500 uppercase tracking-widest">Enrollment</th>
                <th className="px-6 py-4 text-[10px] font-bold text-slate-500 uppercase tracking-widest">Market Share</th>
                <th className="px-6 py-4 text-[10px] font-bold text-slate-500 uppercase tracking-widest">MoM Trend</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/50">
              {data?.organizations.map((org, i) => {
                const color = getColor(org.name, DEFAULT_COLORS[i % DEFAULT_COLORS.length]);
                const displayName = getDisplayName(org.name);
                return (
                  <tr key={i} className="hover:bg-slate-800/30 transition-colors group cursor-pointer">
                    <td className="px-6 py-4">
                      <div className="flex items-center gap-3">
                        <div
                          className="w-2.5 h-2.5 rounded-full shrink-0"
                          style={{ backgroundColor: color }}
                        />
                        <div>
                          <div className="text-sm font-bold text-white group-hover:text-sky-400 transition-colors">
                            {displayName}
                          </div>
                          {displayName !== org.name && (
                            <div className="text-[10px] text-slate-600 mt-0.5 truncate max-w-xs" title={org.name}>
                              {org.name}
                            </div>
                          )}
                        </div>
                      </div>
                    </td>
                    <td className="px-6 py-4 text-sm font-mono text-slate-300">
                      {org.enrollment.toLocaleString()}
                    </td>
                    <td className="px-6 py-4">
                      <div className="flex items-center gap-3">
                        <div className="flex-1 h-1.5 bg-slate-800 rounded-full overflow-hidden max-w-[100px]">
                          <div
                            className="h-full rounded-full"
                            style={{ width: `${org.marketShare}%`, backgroundColor: color }}
                          />
                        </div>
                        <span className="text-xs font-bold text-slate-400">{org.marketShare.toFixed(2)}%</span>
                      </div>
                    </td>
                    <td className="px-6 py-4">
                      <div className="flex items-center gap-1 text-emerald-400 font-bold text-xs">
                        <ArrowUpRight className="w-3.5 h-3.5" />
                        STABLE
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Config panel */}
      {showConfig && (
        <OrgConfigPanel
          orgs={data?.organizations || []}
          onClose={() => setShowConfig(false)}
        />
      )}
    </div>
  );
};
