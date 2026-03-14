import React, { useEffect, useState } from 'react';
import { useFilters } from '../context/FilterContext';
import { 
  LineChart, 
  Line, 
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip, 
  ResponsiveContainer,
  AreaChart,
  Area
} from 'recharts';
import { ArrowUpRight, ArrowDownRight, Minus } from 'lucide-react';

interface DashboardSummary {
  totalEnrollment: number;
  planCount: number;
  countyCount: number;
  orgCount: number;
}

interface TrendPoint {
  month: string;
  enrollment: number;
}

interface Mover {
  contract_id: string;
  plan_id: string;
  plan_name: string;
  change: number;
}

export const Dashboard: React.FC = () => {
  const { filters } = useFilters();
  const [summary, setSummary] = useState<DashboardSummary | null>(null);
  const [trend, setTrend] = useState<TrendPoint[]>([]);
  const [movers, setMovers] = useState<Mover[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      try {
        const [summaryRes, trendRes, moversRes] = await Promise.all([
          fetch('http://127.0.0.1:3000/api/query/dashboard-summary', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(filters),
          }),
          fetch('http://127.0.0.1:3000/api/query/global-trend', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(filters),
          }),
          fetch('http://127.0.0.1:3000/api/query/top-movers', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              ...filters,
              from: '2025-01',
              to: '2025-02',
              limit: 5
            }),
          })
        ]);

        const summaryData = await summaryRes.json();
        const trendDataRaw = await trendRes.json();
        const moversData = await moversRes.json();

        setSummary(summaryData);
        setTrend(trendDataRaw.map(([m, val]: [number, number]) => ({
          month: m.toString().replace(/(\d{4})(\d{2})/, '$1-$2'),
          enrollment: val
        })));
        setMovers(moversData.map(([cid, pid, name, change]: any) => ({
          contract_id: cid,
          plan_id: pid,
          plan_name: name,
          change
        })));
      } catch (error) {
        console.error('Failed to fetch dashboard data:', error);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [filters]);

  const kpis = [
    { label: 'Total Enrollment', value: summary ? (summary.totalEnrollment / 1000000).toFixed(2) + 'M' : '0', change: '+1.2%' },
    { label: 'Parent Orgs', value: summary ? summary.orgCount.toLocaleString() : '0', change: '0' },
    { label: 'Total Plans', value: summary ? summary.planCount.toLocaleString() : '0', change: '+36' },
    { label: 'Counties', value: summary ? summary.countyCount.toLocaleString() : '0', change: '0' },
  ];

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold tracking-tight text-white">Dashboard</h1>
        <div className="text-sm text-slate-400">Analysis period: Jan 2025 - Feb 2025</div>
      </div>
      
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {kpis.map((kpi) => (
          <div key={kpi.label} className="p-6 bg-slate-900 border border-slate-800 rounded-xl relative group">
            <div className="text-sm font-medium text-slate-400 mb-1">{kpi.label}</div>
            <div className="flex items-baseline gap-2">
              <div className="text-2xl font-bold text-white group-hover:text-sky-400 transition-colors">{kpi.value}</div>
              <div className="text-xs font-medium text-emerald-400">{kpi.change}</div>
            </div>
          </div>
        ))}
      </div>
      
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-2 p-6 bg-slate-900 border border-slate-800 rounded-xl flex flex-col min-h-[400px]">
          <div className="flex items-center justify-between mb-6">
            <h2 className="text-sm font-semibold text-slate-300 uppercase tracking-wider">Enrollment Trend</h2>
            <div className="flex gap-2">
              <div className="flex items-center gap-1.5 px-2 py-1 bg-sky-500/10 rounded border border-sky-500/20 text-[10px] font-bold text-sky-400">
                TOTAL ENROLLMENT
              </div>
            </div>
          </div>
          <div className="flex-1 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={trend}>
                <defs>
                  <linearGradient id="colorEnroll" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#0ea5e9" stopOpacity={0.3}/>
                    <stop offset="95%" stopColor="#0ea5e9" stopOpacity={0}/>
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
                <XAxis 
                  dataKey="month" 
                  axisLine={false} 
                  tickLine={false} 
                  tick={{fill: '#64748b', fontSize: 10}} 
                  dy={10}
                />
                <YAxis 
                  axisLine={false} 
                  tickLine={false} 
                  tick={{fill: '#64748b', fontSize: 10}}
                  tickFormatter={(val) => (val / 1000000).toFixed(1) + 'M'}
                />
                <Tooltip 
                  contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #334155', borderRadius: '8px' }}
                  itemStyle={{ color: '#f1f5f9', fontSize: '12px' }}
                  labelStyle={{ color: '#94a3b8', fontSize: '10px', marginBottom: '4px' }}
                />
                <Area 
                  type="monotone" 
                  dataKey="enrollment" 
                  stroke="#0ea5e9" 
                  strokeWidth={2}
                  fillOpacity={1} 
                  fill="url(#colorEnroll)" 
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>

        <div className="p-6 bg-slate-900 border border-slate-800 rounded-xl flex flex-col min-h-[400px]">
          <h2 className="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-6">Top Growth Plans (Jan-Feb)</h2>
          <div className="flex-1 space-y-4">
            {movers.length === 0 ? (
              <div className="h-full flex items-center justify-center text-slate-600 text-sm">No movers found</div>
            ) : (
              movers.map((mover, i) => (
                <div key={i} className="flex items-center justify-between group">
                  <div className="min-w-0 flex-1">
                    <div className="text-xs font-bold text-white truncate group-hover:text-sky-400 transition-colors">{mover.plan_name}</div>
                    <div className="text-[10px] text-slate-500">{mover.contract_id}|{mover.plan_id}</div>
                  </div>
                  <div className="flex items-center gap-1.5 ml-4">
                    <span className={`text-xs font-mono font-bold ${mover.change >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                      {mover.change >= 0 ? '+' : ''}{mover.change.toLocaleString()}
                    </span>
                    {mover.change > 0 ? (
                      <ArrowUpRight className="w-3.5 h-3.5 text-emerald-500" />
                    ) : mover.change < 0 ? (
                      <ArrowDownRight className="w-3.5 h-3.5 text-rose-500" />
                    ) : (
                      <Minus className="w-3.5 h-3.5 text-slate-500" />
                    )}
                  </div>
                </div>
              ))
            )}
          </div>
          <button className="mt-6 w-full py-2 bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs font-bold rounded-lg transition-colors border border-slate-700">
            VIEW ALL MOVERS
          </button>
        </div>
      </div>
    </div>
  );
};
