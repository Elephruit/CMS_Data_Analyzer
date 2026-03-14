import React, { useEffect, useState } from 'react';
import { useFilters } from '../context/FilterContext';
import { Card, PageHeader, StatCard } from '../components/ui/Primitives';
import { 
  BarChart, 
  Bar, 
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip, 
  ResponsiveContainer,
  Cell
} from 'recharts';
import { TrendingUp, Zap, Sparkles, ArrowUpRight, ArrowDownRight } from 'lucide-react';

interface HighFlyer {
  name: string;
  contract: string;
  plan: string;
  current: number;
  change: number;
  percent: number;
}

interface GrowthData {
  latestMonth: number;
  priorMonth: number;
  totalGrowth: number;
  highFlyers: HighFlyer[];
}

export const GrowthAnalytics: React.FC = () => {
  const { filters } = useFilters();
  const [data, setData] = useState<GrowthData | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      try {
        const response = await fetch('http://127.0.0.1:3000/api/query/growth-analytics', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(filters),
        });
        const result = await response.json();
        setData(result);
      } catch (error) {
        console.error('Failed to fetch growth analytics:', error);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [filters]);

  const COLORS = ['#10b981', '#34d399', '#6ee7b7', '#a7f3d0', '#d1fae5'];

  if (loading && !data) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-emerald-500"></div>
      </div>
    );
  }

  return (
    <div className="space-y-8 max-w-[1600px] mx-auto pb-12">
      <PageHeader 
        title="Growth & AEP Analytics" 
        subtitle="Identify high-growth plans and Annual Enrollment Period shifts."
      />

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <StatCard 
          label="Net Market Growth" 
          value={(data?.totalGrowth || 0).toLocaleString()} 
          change={data?.totalGrowth && data.totalGrowth > 0 ? 'Expansion' : 'Contraction'}
          changeType={data?.totalGrowth && data.totalGrowth > 0 ? 'positive' : 'negative'}
          icon={TrendingUp} 
          loading={loading}
        />
        <StatCard 
          label="High Flyer Count" 
          value={data?.highFlyers.length || 0} 
          icon={Zap} 
          loading={loading}
        />
        <StatCard 
          label="AEP Impact Status" 
          value="Calculated" 
          change="AEP Analysis Active"
          changeType="positive"
          icon={Sparkles} 
          loading={loading}
        />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <Card className="lg:col-span-2 flex flex-col min-h-[500px]">
          <div className="flex items-center justify-between mb-8">
            <h2 className="text-sm font-bold text-slate-300 uppercase tracking-widest">High Flyers: Top Growth by %</h2>
            <div className="text-[10px] font-mono text-slate-500 uppercase tracking-wider">Plans > 500 Enrollment</div>
          </div>
          <div className="flex-1 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={data?.highFlyers.slice(0, 10)} margin={{ top: 5, right: 30, left: 20, bottom: 60 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
                <XAxis 
                  dataKey="name" 
                  angle={-45} 
                  textAnchor="end" 
                  interval={0} 
                  height={100}
                  tick={{fill: '#94a3b8', fontSize: 10}}
                  axisLine={false}
                  tickLine={false}
                />
                <YAxis 
                  axisLine={false} 
                  tickLine={false} 
                  tick={{fill: '#94a3b8', fontSize: 10}} 
                  tickFormatter={(val) => val + '%'}
                />
                <Tooltip 
                  cursor={{fill: '#1e293b'}}
                  contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #334155', borderRadius: '12px' }}
                  itemStyle={{ color: '#10b981', fontSize: '12px' }}
                  labelStyle={{ color: '#f1f5f9', fontSize: '10px', fontWeight: 'bold' }}
                  formatter={(value: any) => [value.toFixed(2) + '%', 'Growth']}
                />
                <Bar dataKey="percent" radius={[4, 4, 0, 0]}>
                  {data?.highFlyers.slice(0, 10).map((_, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </Card>

        <Card className="flex flex-col h-[500px]">
          <h2 className="text-sm font-bold text-slate-300 uppercase tracking-widest mb-6">Growth Leaderboard</h2>
          <div className="flex-1 overflow-auto custom-scrollbar pr-2">
            <div className="space-y-4">
              {data?.highFlyers.map((flyer, i) => (
                <div key={i} className="p-3 bg-slate-800/50 rounded-xl border border-slate-700/50 hover:border-emerald-500/30 transition-all group">
                  <div className="flex items-center justify-between mb-1">
                    <span className="text-[10px] font-mono text-slate-500">{flyer.contract}|{flyer.plan}</span>
                    <div className="flex items-center gap-1 text-emerald-400 font-bold text-xs">
                      {flyer.percent.toFixed(1)}% <ArrowUpRight className="w-3 h-3" />
                    </div>
                  </div>
                  <div className="text-xs font-bold text-white truncate group-hover:text-emerald-400 transition-colors">
                    {flyer.name}
                  </div>
                  <div className="mt-2 flex items-center justify-between">
                    <div className="text-[10px] text-slate-400 uppercase tracking-tighter">Current: {flyer.current.toLocaleString()}</div>
                    <div className="text-[10px] text-emerald-500/80 font-bold">+{flyer.change.toLocaleString()}</div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </Card>
      </div>
    </div>
  );
};
