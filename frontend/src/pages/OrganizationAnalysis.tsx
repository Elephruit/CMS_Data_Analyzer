import React, { useEffect, useState, useMemo } from 'react';
import { useFilters } from '../context/FilterContext';
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
  Legend
} from 'recharts';
import { Building2, TrendingUp, PieChart as PieChartIcon, ArrowUpRight } from 'lucide-react';

interface OrgTrendPoint {
  month: number;
  value: number;
}

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

export const OrganizationAnalysis: React.FC = () => {
  const { filters } = useFilters();
  const [data, setData] = useState<OrgAnalysisData | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      try {
        const response = await fetch('http://127.0.0.1:3000/api/query/organization-analysis', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(filters),
        });
        const result = await response.json();
        setData(result);
      } catch (error) {
        console.error('Failed to fetch org analysis:', error);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [filters]);

  const top10 = useMemo(() => data?.organizations.slice(0, 10) || [], [data]);
  
  const concentration = useMemo(() => {
    if (!data) return { top3: 0, top5: 0, top10: 0 };
    const total = data.totalMarketEnrollment;
    const top3 = data.organizations.slice(0, 3).reduce((sum, o) => sum + o.enrollment, 0);
    const top5 = data.organizations.slice(0, 5).reduce((sum, o) => sum + o.enrollment, 0);
    const top10 = data.organizations.slice(0, 10).reduce((sum, o) => sum + o.enrollment, 0);
    
    return {
      top3: (top3 / total) * 100,
      top5: (top5 / total) * 100,
      top10: (top10 / total) * 100
    };
  }, [data]);

  const trendData = useMemo(() => {
    if (!data || data.organizations.length === 0) return [];
    
    const top5 = data.organizations.slice(0, 5);
    const months = Array.from(new Set(top5.flatMap(o => o.trend.map(t => t.month)))).sort();
    
    return months.map(m => {
      const point: any = { month: m.toString().replace(/(\d{4})(\d{2})/, '$1-$2') };
      top5.forEach(o => {
        const t = o.trend.find(tp => tp.month === m);
        point[o.name] = t ? t.value : 0;
      });
      return point;
    });
  }, [data]);

  const COLORS = ['#0ea5e9', '#10b981', '#f59e0b', '#8b5cf6', '#ec4899', '#64748b'];

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="flex flex-col items-center gap-4">
          <div className="w-12 h-12 border-4 border-sky-500 border-t-transparent rounded-full animate-spin"></div>
          <span className="text-slate-400 font-bold uppercase tracking-widest text-xs">Analyzing Market Structure...</span>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-8 max-w-[1600px] mx-auto pb-12">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white tracking-tight">Parent Organization Analysis</h1>
          <p className="text-slate-400 text-sm mt-1">Deep dive into market dominance and organizational growth trends.</p>
        </div>
        <div className="px-4 py-2 bg-slate-900 border border-slate-800 rounded-xl flex items-center gap-3">
          <div className="w-2 h-2 bg-sky-500 rounded-full animate-pulse"></div>
          <span className="text-xs font-bold text-slate-300 uppercase tracking-wider">
            Market Size: {(data?.totalMarketEnrollment || 0).toLocaleString()}
          </span>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
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

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-6 h-[450px] flex flex-col">
          <h2 className="text-sm font-bold text-slate-300 uppercase tracking-widest mb-8">Leaderboard: Market Share %</h2>
          <div className="flex-1 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={top10} layout="vertical" margin={{ left: 40, right: 40 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" horizontal={true} vertical={false} />
                <XAxis type="number" hide />
                <YAxis 
                  dataKey="name" 
                  type="category" 
                  axisLine={false} 
                  tickLine={false} 
                  tick={{fill: '#94a3b8', fontSize: 10, fontWeight: 600}} 
                  width={120}
                />
                <Tooltip 
                  cursor={{fill: '#1e293b'}}
                  contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #334155', borderRadius: '12px' }}
                  itemStyle={{ color: '#f1f5f9', fontSize: '12px' }}
                  labelStyle={{ color: '#0ea5e9', fontSize: '10px', fontWeight: 'bold', marginBottom: '4px' }}
                  formatter={(value: number) => [value.toFixed(2) + '%', 'Market Share']}
                />
                <Bar dataKey="marketShare" radius={[0, 4, 4, 0]} barSize={24} fill="#0ea5e9">
                  {top10.map((_, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-6 h-[450px] flex flex-col">
          <h2 className="text-sm font-bold text-slate-300 uppercase tracking-widest mb-8">Growth Trajectory: Top 5 Orgs</h2>
          <div className="flex-1 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={trendData}>
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
                  tickFormatter={(val) => (val / 1000).toFixed(0) + 'k'}
                />
                <Tooltip 
                  contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #334155', borderRadius: '12px' }}
                  itemStyle={{ fontSize: '11px' }}
                />
                <Legend iconType="circle" wrapperStyle={{ fontSize: '10px', paddingTop: '20px' }} />
                {data?.organizations.slice(0, 5).map((org, i) => (
                  <Line 
                    key={org.name}
                    type="monotone" 
                    dataKey={org.name} 
                    stroke={COLORS[i % COLORS.length]} 
                    strokeWidth={3}
                    dot={{ r: 4, strokeWidth: 2, fill: '#0f172a' }}
                    activeDot={{ r: 6 }}
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      <div className="bg-slate-900 border border-slate-800 rounded-2xl overflow-hidden shadow-xl">
        <div className="p-6 border-b border-slate-800">
          <h2 className="text-sm font-bold text-slate-300 uppercase tracking-widest">Organizational Deep-Dive</h2>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-left border-collapse">
            <thead>
              <tr className="bg-slate-900/50 border-b border-slate-800">
                <th className="px-6 py-4 text-[10px] font-bold text-slate-500 uppercase tracking-widest">Organization Name</th>
                <th className="px-6 py-4 text-[10px] font-bold text-slate-500 uppercase tracking-widest">Latest Enrollment</th>
                <th className="px-6 py-4 text-[10px] font-bold text-slate-500 uppercase tracking-widest">Market Share</th>
                <th className="px-6 py-4 text-[10px] font-bold text-slate-500 uppercase tracking-widest">MoM Trend</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/50">
              {data?.organizations.map((org, i) => (
                <tr key={i} className="hover:bg-slate-800/30 transition-colors group cursor-pointer">
                  <td className="px-6 py-4">
                    <div className="text-sm font-bold text-white group-hover:text-sky-400 transition-colors">{org.name}</div>
                  </td>
                  <td className="px-6 py-4 text-sm font-mono text-slate-300">
                    {org.enrollment.toLocaleString()}
                  </td>
                  <td className="px-6 py-4">
                    <div className="flex items-center gap-3">
                      <div className="flex-1 h-1.5 bg-slate-800 rounded-full overflow-hidden max-w-[100px]">
                        <div 
                          className="h-full bg-sky-500 rounded-full" 
                          style={{ width: `${org.marketShare}%` }}
                        ></div>
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
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};
