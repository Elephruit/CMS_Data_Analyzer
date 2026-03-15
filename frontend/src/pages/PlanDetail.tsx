import React, { useEffect, useState } from 'react';
import { Card } from '../components/ui/Primitives';
import { 
  AreaChart, 
  Area, 
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip, 
  ResponsiveContainer 
} from 'recharts';
import { Search } from 'lucide-react';

interface PlanMetadata {
  name: string;
  contract_id: string;
  plan_id: string;
  org: string;
}

interface FootprintItem {
  state: string;
  county: string;
  enrollment: number;
}

interface TrendPoint {
  month: number;
  value: number;
}

interface PlanDetailsData {
  metadata: PlanMetadata;
  footprint: FootprintItem[];
  trend: TrendPoint[];
}

export const PlanDetail: React.FC = () => {
  const [searchId, setSearchId] = useState('H5425|087');
  const [data, setData] = useState<PlanDetailsData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchPlanDetails = async (id: string) => {
    const [contract_id, plan_id] = id.split('|');
    if (!contract_id || !plan_id) return;

    setLoading(true);
    setError(null);
    try {
      const response = await fetch('http://127.0.0.1:3000/api/query/plan-details', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ contract_id, plan_id }),
      });
      if (!response.ok) throw new Error('Plan not found');
      const result = await response.json();
      setData(result);
    } catch (err: any) {
      setError(err.message);
      setData(null);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchPlanDetails(searchId);
  }, []);

  const chartData = data?.trend.map(t => ({
    month: t.month.toString().replace(/(\d{4})(\d{2})/, '$1-$2'),
    enrollment: t.value
  })) || [];

  return (
    <div className="space-y-8 max-w-[1600px] mx-auto pb-12">
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-6">
        <div>
          <h1 className="text-2xl font-bold text-white tracking-tight">Plan Intelligence</h1>
          <p className="text-slate-400 text-sm mt-1">Drill into specific contract-plan performance and footprint.</p>
        </div>
        <div className="relative w-full max-w-sm">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500" />
          <input 
            type="text" 
            placeholder="Search ID (e.g. H5425|087)..."
            value={searchId}
            onChange={(e) => setSearchId(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && fetchPlanDetails(searchId)}
            className="w-full bg-slate-900 border border-slate-800 rounded-xl pl-10 pr-4 py-2.5 text-sm text-white focus:border-sky-500 outline-none transition-all shadow-lg"
          />
        </div>
      </div>

      {error && (
        <Card className="border-rose-500/20 bg-rose-500/5 text-rose-400 text-sm font-medium py-4 px-6">
          {error}. Please check the Contract|Plan ID and try again.
        </Card>
      )}

      {data && (
        <>
          <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
            <div className="lg:col-span-3 space-y-6">
              <Card className="bg-gradient-to-br from-slate-900 to-slate-800 border-sky-500/20">
                <div className="flex flex-col md:flex-row md:items-center justify-between gap-6">
                  <div className="space-y-2">
                    <div className="flex items-center gap-3">
                      <span className="px-2 py-0.5 bg-sky-500/10 text-sky-400 text-[10px] font-bold rounded border border-sky-500/20 tracking-widest">
                        {data.metadata.contract_id}|{data.metadata.plan_id}
                      </span>
                      <span className="px-2 py-0.5 bg-slate-800 text-slate-400 text-[10px] font-bold rounded border border-slate-700 tracking-widest uppercase">
                        {data.metadata.org}
                      </span>
                    </div>
                    <h2 className="text-3xl font-extrabold text-white">{data.metadata.name}</h2>
                  </div>
                  <div className="flex gap-4">
                    <div className="text-right">
                      <div className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">Total Enrollment</div>
                      <div className="text-2xl font-bold text-sky-400">
                        {data.footprint.reduce((sum, f) => sum + f.enrollment, 0).toLocaleString()}
                      </div>
                    </div>
                  </div>
                </div>
              </Card>

              <Card className="h-[400px] flex flex-col">
                <h3 className="text-sm font-bold text-slate-300 uppercase tracking-widest mb-8">Enrollment History</h3>
                <div className="flex-1 w-full">
                  <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={chartData}>
                      <defs>
                        <linearGradient id="colorPlan" x1="0" y1="0" x2="0" y2="1">
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
                      />
                      <YAxis 
                        axisLine={false} 
                        tickLine={false} 
                        tick={{fill: '#64748b', fontSize: 10}}
                      />
                      <Tooltip 
                        contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #334155', borderRadius: '12px' }}
                        itemStyle={{ color: '#f1f5f9' }}
                      />
                      <Area 
                        type="monotone" 
                        dataKey="enrollment" 
                        stroke="#0ea5e9" 
                        strokeWidth={3}
                        fillOpacity={1} 
                        fill="url(#colorPlan)" 
                      />
                    </AreaChart>
                  </ResponsiveContainer>
                </div>
              </Card>
            </div>

            <Card className="flex flex-col h-full lg:min-h-[600px]">
              <h3 className="text-sm font-bold text-slate-300 uppercase tracking-widest mb-6">County Footprint</h3>
              <div className="flex-1 overflow-auto custom-scrollbar pr-2">
                <div className="space-y-3">
                  {data.footprint.map((f, i) => (
                    <div key={i} className="flex items-center justify-between p-3 bg-slate-800/30 rounded-xl border border-slate-800 hover:border-sky-500/30 transition-all">
                      <div>
                        <div className="text-xs font-bold text-white">{f.county}</div>
                        <div className="text-[10px] text-slate-500 font-bold uppercase">{f.state}</div>
                      </div>
                      <div className="text-xs font-mono text-sky-400 font-bold">{f.enrollment.toLocaleString()}</div>
                    </div>
                  ))}
                </div>
              </div>
            </Card>
          </div>
        </>
      )}

      {loading && (
        <div className="flex items-center justify-center py-24">
          <div className="w-12 h-12 border-4 border-sky-500 border-t-transparent rounded-full animate-spin"></div>
        </div>
      )}
    </div>
  );
};
