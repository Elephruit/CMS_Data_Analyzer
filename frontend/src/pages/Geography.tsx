import React, { useEffect, useState, useMemo } from 'react';
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
import { Map as MapIcon, Globe, Navigation, ArrowUpDown } from 'lucide-react';

interface StateData {
  name: string;
  enrollment: number;
}

interface CountyData {
  state: string;
  name: string;
  enrollment: number;
}

interface GeoAnalysisData {
  latestMonth: number;
  states: StateData[];
  counties: CountyData[];
}

export const Geography: React.FC = () => {
  const { filters } = useFilters();
  const [data, setData] = useState<GeoAnalysisData | null>(null);
  const [loading, setLoading] = useState(true);
  const [sortConfig, setSortSortConfig] = useState<{ key: 'name' | 'enrollment'; direction: 'asc' | 'desc' }>({
    key: 'enrollment',
    direction: 'desc'
  });

  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      try {
        const response = await fetch('http://127.0.0.1:3000/api/query/geo-analysis', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(filters),
        });
        const result = await response.json();
        setData(result);
      } catch (error) {
        console.error('Failed to fetch geo analysis:', error);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [filters]);

  const sortedCounties = useMemo(() => {
    if (!data) return [];
    let counties = [...data.counties];
    counties.sort((a, b) => {
      if (sortConfig.key === 'enrollment') {
        return sortConfig.direction === 'asc' ? a.enrollment - b.enrollment : b.enrollment - a.enrollment;
      } else {
        return sortConfig.direction === 'asc' ? a.name.localeCompare(b.name) : b.name.localeCompare(a.name);
      }
    });
    return counties;
  }, [data, sortConfig]);

  const topStates = useMemo(() => data?.states.slice(0, 10) || [], [data]);
  const COLORS = ['#0ea5e9', '#38bdf8', '#7dd3fc', '#bae6fd', '#e0f2fe'];

  if (loading && !data) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-sky-500"></div>
      </div>
    );
  }

  return (
    <div className="space-y-8 max-w-[1600px] mx-auto pb-12">
      <PageHeader 
        title="Geography & Market Penetration" 
        subtitle="Analyze enrollment distribution across states and counties."
      />

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <StatCard 
          label="Active States" 
          value={data?.states.length || 0} 
          icon={Globe} 
          loading={loading}
        />
        <StatCard 
          label="Counties Covered" 
          value={data?.counties.length || 0} 
          icon={MapIcon} 
          loading={loading}
        />
        <StatCard 
          label="Top State" 
          value={data?.states[0]?.name || 'N/A'} 
          change={data?.states[0]?.enrollment.toLocaleString()}
          changeType="neutral"
          icon={Navigation} 
          loading={loading}
        />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card className="h-[450px] flex flex-col">
          <h2 className="text-sm font-bold text-slate-300 uppercase tracking-widest mb-8 text-center">Top 10 States by Enrollment</h2>
          <div className="flex-1 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={topStates} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
                <XAxis dataKey="name" axisLine={false} tickLine={false} tick={{fill: '#94a3b8', fontSize: 10}} />
                <YAxis axisLine={false} tickLine={false} tick={{fill: '#94a3b8', fontSize: 10}} tickFormatter={(val) => (val / 1000).toFixed(0) + 'k'} />
                <Tooltip 
                  cursor={{fill: '#1e293b'}}
                  contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #334155', borderRadius: '12px' }}
                  itemStyle={{ color: '#f1f5f9', fontSize: '12px' }}
                  labelStyle={{ color: '#0ea5e9', fontSize: '10px', fontWeight: 'bold' }}
                />
                <Bar dataKey="enrollment" radius={[4, 4, 0, 0]}>
                  {topStates.map((_, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </Card>

        <Card className="flex flex-col">
          <div className="flex items-center justify-between mb-6">
            <h2 className="text-sm font-bold text-slate-300 uppercase tracking-widest">County-Level Breakdown</h2>
            <div className="text-[10px] font-mono text-slate-500 uppercase tracking-wider">Top 50 Counties</div>
          </div>
          <div className="flex-1 overflow-auto max-h-[340px] pr-2 custom-scrollbar">
            <table className="w-full text-left border-collapse">
              <thead className="sticky top-0 bg-slate-900 z-10">
                <tr className="border-b border-slate-800">
                  <th 
                    className="pb-3 text-[10px] font-bold text-slate-500 uppercase tracking-widest cursor-pointer hover:text-sky-400"
                    onClick={() => setSortSortConfig({ key: 'name', direction: sortConfig.direction === 'asc' ? 'desc' : 'asc' })}
                  >
                    <div className="flex items-center gap-1">County <ArrowUpDown className="w-3 h-3" /></div>
                  </th>
                  <th className="pb-3 text-[10px] font-bold text-slate-500 uppercase tracking-widest text-center">State</th>
                  <th 
                    className="pb-3 text-[10px] font-bold text-slate-500 uppercase tracking-widest text-right cursor-pointer hover:text-sky-400"
                    onClick={() => setSortSortConfig({ key: 'enrollment', direction: sortConfig.direction === 'asc' ? 'desc' : 'asc' })}
                  >
                    <div className="flex items-center justify-end gap-1">Enrollment <ArrowUpDown className="w-3 h-3" /></div>
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/50">
                {sortedCounties.map((county, i) => (
                  <tr key={`${county.state}-${county.name}`} className="hover:bg-slate-800/30 transition-colors">
                    <td className="py-3 text-sm font-bold text-white">{county.name}</td>
                    <td className="py-3 text-sm text-slate-400 text-center">{county.state}</td>
                    <td className="py-3 text-sm font-mono text-sky-400 text-right">{county.enrollment.toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      </div>
    </div>
  );
};
