import React, { useEffect, useState } from 'react';
import { useFilters } from '../context/FilterContext';
import { Card, PageHeader, StatCard } from '../components/ui/Primitives';
import { 
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip, 
  ResponsiveContainer,
  AreaChart,
  Area
} from 'recharts';
import { ArrowUpRight, ArrowDownRight, Minus, LayoutDashboard, Building2, Users, MapPin } from 'lucide-react';

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

  return (
    <div className="space-y-8 max-w-[1600px] mx-auto pb-12">
      <PageHeader 
        title="Executive Overview" 
        subtitle="Market-wide enrollment metrics and top-line trends."
      />
      
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard 
          label="Total Enrollment" 
          value={summary ? (summary.totalEnrollment / 1000000).toFixed(2) + 'M' : '0'} 
          change="+1.2% MoM"
          icon={LayoutDashboard}
          loading={loading}
        />
        <StatCard 
          label="Parent Organizations" 
          value={summary ? summary.orgCount.toLocaleString() : '0'} 
          icon={Building2}
          loading={loading}
        />
        <StatCard 
          label="Total Plans" 
          value={summary ? summary.planCount.toLocaleString() : '0'} 
          change="+36 New"
          icon={Users}
          loading={loading}
        />
        <StatCard 
          label="Geographies" 
          value={summary ? summary.countyCount.toLocaleString() : '0'} 
          icon={MapPin}
          loading={loading}
        />
      </div>
      
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <Card className="lg:col-span-2 flex flex-col min-h-[450px]">
          <div className="flex items-center justify-between mb-8">
            <h2 className="text-sm font-bold text-slate-300 uppercase tracking-widest text-center">Market Enrollment Trend</h2>
            <div className="text-[10px] font-mono text-sky-500 font-bold px-2 py-1 bg-sky-500/10 rounded">LIVE DATA</div>
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
                  contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #334155', borderRadius: '12px' }}
                  itemStyle={{ color: '#f1f5f9', fontSize: '12px' }}
                  labelStyle={{ color: '#94a3b8', fontSize: '10px', marginBottom: '4px' }}
                />
                <Area 
                  type="monotone" 
                  dataKey="enrollment" 
                  stroke="#0ea5e9" 
                  strokeWidth={3}
                  fillOpacity={1} 
                  fill="url(#colorEnroll)" 
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </Card>

        <Card className="flex flex-col h-full min-h-[450px]">
          <h2 className="text-sm font-bold text-slate-300 uppercase tracking-widest mb-8">Top Growth Plans</h2>
          <div className="flex-1 space-y-6">
            {movers.length === 0 ? (
              <div className="h-full flex items-center justify-center text-slate-600 text-sm italic">No movers detected in range.</div>
            ) : (
              movers.map((mover, i) => (
                <div key={i} className="flex items-center justify-between group cursor-pointer">
                  <div className="min-w-0 flex-1">
                    <div className="text-xs font-bold text-white truncate group-hover:text-sky-400 transition-colors">{mover.plan_name}</div>
                    <div className="text-[10px] text-slate-500 font-mono mt-0.5">{mover.contract_id}|{mover.plan_id}</div>
                  </div>
                  <div className="flex items-center gap-2 ml-4">
                    <span className={`text-xs font-mono font-bold ${mover.change >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                      {mover.change >= 0 ? '+' : ''}{mover.change.toLocaleString()}
                    </span>
                    {mover.change > 0 ? (
                      <ArrowUpRight className="w-3.5 h-3.5 text-emerald-500" />
                    ) : (
                      <ArrowDownRight className="w-3.5 h-3.5 text-rose-500" />
                    )}
                  </div>
                </div>
              ))
            )}
          </div>
          <button className="mt-8 w-full py-2.5 bg-slate-800 hover:bg-slate-700 text-slate-300 text-[10px] font-black uppercase tracking-widest rounded-xl border border-slate-700 transition-all shadow-lg">
            View All Insights
          </button>
        </Card>
      </div>
    </div>
  );
};
