import React, { useEffect, useState } from 'react';
import { useFilters } from '../context/FilterContext';
import { Card, PageHeader, StatCard } from '../components/ui/Primitives';
import { 
  BarChart, 
...

interface HighFlyer {
  name: string;
  contract: string;
  plan: string;
  current: number;
  change: number;
  percent: number;
  aepChange: number;
}

interface GrowthData {
  latestMonth: number;
  priorMonth: number;
  totalGrowth: number;
  aepGrowth: number;
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

  const exportToCSV = () => {
    if (!data) return;
    const headers = ['Plan Name', 'Contract', 'Plan ID', 'Current Enrollment', 'MoM Change', '% Change', 'AEP Change'];
    const csvContent = [
      headers.join(','),
      ...data.highFlyers.map(f => [
        `"${f.name}"`,
        f.contract,
        f.plan,
        f.current,
        f.change,
        f.percent.toFixed(2),
        f.aepChange
      ].join(','))
    ].join('\n');

    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    const url = URL.createObjectURL(blob);
    link.setAttribute('href', url);
    link.setAttribute('download', `growth_analysis_${new Date().toISOString().split('T')[0]}.csv`);
    link.style.visibility = 'hidden';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const COLORS = ['#10b981', '#34d399', '#6ee7b7', '#a7f3d0', '#d1fae5'];

  // Calculate AEP labels
  const analysisYear = filters.analysisMonth ? parseInt(filters.analysisMonth.split('-')[0]) : 2025;
  const aepStartLabel = `Dec ${analysisYear - 1}`;
  const aepEndLabel = `Feb ${analysisYear}`;

  return (
    <div className="space-y-8 max-w-[1600px] mx-auto pb-12">
      <PageHeader 
        title="Growth & AEP Performance" 
        subtitle="Identify high-velocity plans and analyze shifts during the Annual Enrollment Period."
      />

      <div className="bg-emerald-500/5 border border-emerald-500/20 rounded-2xl p-4 flex items-start gap-4">
        <div className="p-2 bg-emerald-500/10 rounded-lg shrink-0">
          <Info className="w-5 h-5 text-emerald-500" />
        </div>
        <div>
          <h3 className="text-sm font-bold text-white mb-1 uppercase tracking-wider">Growth Methodology</h3>
          <p className="text-xs text-slate-400 leading-relaxed">
            Market expansion is calculated relative to the prior month. <span className="text-emerald-400 font-bold">AEP Growth</span> represents the net shift between <span className="text-white font-medium">{aepStartLabel}</span> and <span className="text-white font-medium">{aepEndLabel}</span>, capturing the primary annual plan selection window. High Flyers are plans with &gt;500 members showing significant momentum.
          </p>
        </div>
      </div>

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
          label="AEP Selection Shift" 
          value={(data?.aepGrowth || 0).toLocaleString()} 
          change={`${aepEndLabel} vs ${aepStartLabel}`}
          changeType={data?.aepGrowth && data.aepGrowth > 0 ? 'positive' : 'negative'}
          icon={Sparkles} 
          loading={loading}
        />
        <StatCard 
          label="High Flyers Identified" 
          value={data?.highFlyers.length || 0} 
          change="Top momentum plans"
          icon={Zap} 
          loading={loading}
        />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <Card className="lg:col-span-2 flex flex-col min-h-[500px]">
          <div className="flex items-center justify-between mb-8">
            <div className="space-y-1">
              <h2 className="text-sm font-bold text-slate-300 uppercase tracking-widest">Momentum Leaders</h2>
              <p className="text-[10px] text-slate-500 font-medium uppercase tracking-tight">Top 10 plans by percentage growth (min. 500 members)</p>
            </div>
            {loading && <div className="animate-pulse text-[10px] font-black text-emerald-500 uppercase tracking-widest">Updating...</div>}
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
          <div className="flex items-center justify-between mb-6">
            <div className="space-y-1">
              <h2 className="text-sm font-bold text-slate-300 uppercase tracking-widest">Growth Rankings</h2>
              <p className="text-[10px] text-slate-500 font-medium uppercase tracking-tight">Ranked by momentum %</p>
            </div>
            <button 
              onClick={exportToCSV}
              className="p-2 text-slate-400 hover:text-white hover:bg-slate-800 rounded-lg transition-colors border border-slate-800"
              title="Export Growth Data"
            >
              <Download className="w-4 h-4" />
            </button>
          </div>
          <div className="flex-1 overflow-auto custom-scrollbar pr-2">
            <div className="space-y-4">
              {loading && !data ? (
                Array.from({length: 5}).map((_, i) => (
                  <div key={i} className="h-24 bg-slate-800/20 rounded-xl border border-slate-800 animate-pulse" />
                ))
              ) : data?.highFlyers.length === 0 ? (
                <div className="h-full flex items-center justify-center text-slate-600 text-xs italic">No high flyers detected.</div>
              ) : (
                data?.highFlyers.map((flyer, i) => (
                  <div key={i} className="p-3 bg-slate-800/30 rounded-xl border border-slate-800/50 hover:border-emerald-500/30 transition-all group">
                    <div className="flex items-center justify-between mb-1">
                      <span className="text-[9px] font-mono text-slate-500">{flyer.contract}|{flyer.plan}</span>
                      <div className="flex items-center gap-1 text-emerald-400 font-bold text-[10px]">
                        {flyer.percent.toFixed(1)}% <ArrowUpRight className="w-2.5 h-2.5" />
                      </div>
                    </div>
                    <div className="text-xs font-bold text-white truncate group-hover:text-emerald-400 transition-colors">
                      {flyer.name}
                    </div>
                    <div className="mt-3 grid grid-cols-2 gap-2">
                      <div className="flex flex-col">
                        <span className="text-[8px] text-slate-500 uppercase font-black">Enrollment</span>
                        <span className="text-[10px] text-white font-mono">{flyer.current.toLocaleString()}</span>
                      </div>
                      <div className="flex flex-col items-end">
                        <span className="text-[8px] text-slate-500 uppercase font-black">Net Change</span>
                        <span className="text-[10px] text-emerald-500 font-mono">+{flyer.change.toLocaleString()}</span>
                      </div>
                    </div>
                  </div>
                ))
              )}
            </div>
          </div>
        </Card>
      </div>
    </div>
  );
};
