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
  PieChart,
  Pie
} from 'recharts';
import { 
  ArrowUpRight, 
  ArrowDownRight, 
  Download, 
  Search, 
  ArrowUpDown,
  Building2,
  FileText,
  Map as MapIcon,
  Users
} from 'lucide-react';

type Grain = 'parentOrg' | 'contract' | 'plan' | 'county';

interface ExplorerRow {
  name: string;
  current: number;
  prior: number;
  change: number;
  percentChange: number;
}

interface ExplorerData {
  grain: Grain;
  latestMonth: number;
  priorMonth: number;
  rows: ExplorerRow[];
}

export const EnrollmentExplorer: React.FC = () => {
  const { filters } = useFilters();
  const { getDisplayName, getColor } = useOrgDisplay();
  const [grain, setGrain] = useState<Grain>('parentOrg');
  const [data, setData] = useState<ExplorerData | null>(null);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState('');
  const [sortConfig, setSortSortConfig] = useState<{ key: keyof ExplorerRow; direction: 'asc' | 'desc' }>({
    key: 'current',
    direction: 'desc'
  });

  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      try {
        const response = await fetch('http://127.0.0.1:3000/api/query/explorer', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ grain, filters }),
        });
        const result = await response.json();
        setData(result);
      } catch (error) {
        console.error('Failed to fetch explorer data:', error);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [grain, filters]);

  const filteredAndSortedRows = useMemo(() => {
    if (!data) return [];
    
    let rows = data.rows.filter(row => 
      row.name.toLowerCase().includes(searchTerm.toLowerCase())
    );

    rows.sort((a, b) => {
      const aVal = a[sortConfig.key];
      const bVal = b[sortConfig.key];
      
      if (typeof aVal === 'number' && typeof bVal === 'number') {
        return sortConfig.direction === 'asc' ? aVal - bVal : bVal - aVal;
      }
      
      const aStr = String(aVal).toLowerCase();
      const bStr = String(bVal).toLowerCase();
      return sortConfig.direction === 'asc' 
        ? aStr.localeCompare(bStr) 
        : bStr.localeCompare(aStr);
    });

    return rows;
  }, [data, searchTerm, sortConfig]);

  const handleSort = (key: keyof ExplorerRow) => {
    setSortSortConfig(prev => ({
      key,
      direction: prev.key === key && prev.direction === 'desc' ? 'asc' : 'desc'
    }));
  };

  const exportToCSV = () => {
    if (!data) return;
    const headers = ['Name', 'Current Enrollment', 'Prior Enrollment', 'Change', '% Change'];
    const csvContent = [
      headers.join(','),
      ...filteredAndSortedRows.map(row => [
        `"${row.name}"`,
        row.current,
        row.prior,
        row.change,
        row.percentChange.toFixed(2)
      ].join(','))
    ].join('\n');

    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    const url = URL.createObjectURL(blob);
    link.setAttribute('href', url);
    link.setAttribute('download', `enrollment_explorer_${grain}_${new Date().toISOString().split('T')[0]}.csv`);
    link.style.visibility = 'hidden';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const chartData = useMemo(() => {
    return filteredAndSortedRows.slice(0, 10).map(row => {
      const displayName = grain === 'parentOrg' ? getDisplayName(row.name) : row.name;
      return {
        rawName: row.name,
        name: displayName.length > 20 ? displayName.substring(0, 20) + '...' : displayName,
        fullName: displayName,
        current: row.current,
        change: row.change
      };
    });
  }, [filteredAndSortedRows, grain, getDisplayName]);

  const COLORS = ['#0ea5e9', '#38bdf8', '#7dd3fc', '#bae6fd', '#e0f2fe'];

  // Resolve color for a chart entry — uses configured brand color when in parentOrg grain
  const getEntryColor = useCallback((rawName: string, fallbackIndex: number) => {
    if (grain === 'parentOrg') return getColor(rawName, COLORS[fallbackIndex % COLORS.length]);
    return COLORS[fallbackIndex % COLORS.length];
  }, [grain, getColor]);

  const grains: { value: Grain; label: string; icon: any }[] = [
    { value: 'parentOrg', label: 'Organization', icon: Building2 },
    { value: 'contract', label: 'Contract', icon: FileText },
    { value: 'plan', label: 'Plan', icon: Users },
    { value: 'county', label: 'County', icon: MapIcon },
  ];

  return (
    <div className="space-y-6 max-w-[1600px] mx-auto">
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div className="flex items-center gap-2 bg-slate-900 border border-slate-800 p-1 rounded-xl">
          {grains.map((g) => (
            <button
              key={g.value}
              onClick={() => setGrain(g.value)}
              className={`flex items-center gap-2 px-4 py-2 rounded-lg text-xs font-bold transition-all ${
                grain === g.value 
                  ? 'bg-sky-500 text-white shadow-lg shadow-sky-500/20' 
                  : 'text-slate-400 hover:text-white hover:bg-slate-800'
              }`}
            >
              <g.icon className="w-3.5 h-3.5" />
              {g.label.toUpperCase()}
            </button>
          ))}
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-2 bg-slate-900 border border-slate-800 rounded-2xl p-6 min-h-[400px] flex flex-col">
          <div className="flex items-center justify-between mb-8">
            <h2 className="text-sm font-bold text-slate-300 uppercase tracking-widest">Market Share (Top 10)</h2>
            <div className="text-[10px] font-mono text-slate-500">SORTED BY CURRENT ENROLLMENT</div>
          </div>
          <div className="flex-1 w-full min-h-[300px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={chartData} layout="vertical" margin={{ left: 40, right: 40 }}>
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
                  formatter={(value: any) => [value?.toLocaleString() ?? '0', 'Enrollment']}
                />
                <Bar dataKey="current" radius={[0, 4, 4, 0]} barSize={24}>
                  {chartData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={getEntryColor(entry.rawName, index)} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-6 flex flex-col">
          <h2 className="text-sm font-bold text-slate-300 uppercase tracking-widest mb-8">Growth Distribution</h2>
          <div className="flex-1 flex items-center justify-center min-h-[300px]">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie
                  data={chartData}
                  innerRadius={60}
                  outerRadius={100}
                  paddingAngle={5}
                  dataKey="current"
                >
                  {chartData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={getEntryColor(entry.rawName, index)} />
                  ))}
                </Pie>
                <Tooltip 
                  contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #334155', borderRadius: '12px' }}
                  itemStyle={{ color: '#f1f5f9', fontSize: '12px' }}
                />
              </PieChart>
            </ResponsiveContainer>
          </div>
          <div className="mt-4 grid grid-cols-2 gap-2">
            {chartData.slice(0, 4).map((entry, index) => (
              <div key={index} className="flex items-center gap-2">
                <div className="w-2 h-2 rounded-full" style={{ backgroundColor: getEntryColor(entry.rawName, index) }}></div>
                <span className="text-[10px] text-slate-400 font-medium truncate">{entry.name}</span>
              </div>
            ))}
          </div>
        </div>
      </div>

      <div className="bg-slate-900 border border-slate-800 rounded-2xl overflow-hidden shadow-xl">
        <div className="p-6 border-b border-slate-800 flex flex-col md:flex-row md:items-center justify-between gap-4">
          <div className="relative flex-1 max-w-md">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500" />
            <input 
              type="text" 
              placeholder={`Search ${grain}...`}
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="w-full bg-slate-800 border border-slate-700 rounded-xl pl-10 pr-4 py-2.5 text-sm text-white focus:border-sky-500 outline-none transition-all shadow-inner"
            />
          </div>
          <div className="flex items-center gap-3">
            <button 
              onClick={exportToCSV}
              className="flex items-center gap-2 px-4 py-2.5 bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs font-bold rounded-xl border border-slate-700 transition-all shadow-lg"
            >
              <Download className="w-4 h-4" />
              EXPORT CSV
            </button>
          </div>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-left border-collapse">
            <thead>
              <tr className="bg-slate-900/50 border-b border-slate-800">
                {[
                  { key: 'name', label: 'NAME' },
                  { key: 'current', label: 'CURRENT' },
                  { key: 'prior', label: 'PRIOR' },
                  { key: 'change', label: 'CHANGE' },
                  { key: 'percentChange', label: '% CHANGE' },
                ].map((col) => (
                  <th 
                    key={col.key}
                    onClick={() => handleSort(col.key as keyof ExplorerRow)}
                    className="px-6 py-4 text-[10px] font-bold text-slate-500 uppercase tracking-widest cursor-pointer hover:text-sky-400 transition-colors"
                  >
                    <div className="flex items-center gap-2">
                      {col.label}
                      <ArrowUpDown className={`w-3 h-3 ${sortConfig.key === col.key ? 'text-sky-500' : 'text-slate-700'}`} />
                    </div>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/50">
              {loading ? (
                <tr>
                  <td colSpan={5} className="px-6 py-24 text-center">
                    <div className="flex flex-col items-center gap-3">
                      <div className="w-8 h-8 border-4 border-sky-500 border-t-transparent rounded-full animate-spin"></div>
                      <span className="text-slate-500 text-sm font-medium">Crunching enrollment data...</span>
                    </div>
                  </td>
                </tr>
              ) : filteredAndSortedRows.length === 0 ? (
                <tr>
                  <td colSpan={5} className="px-6 py-24 text-center text-slate-500 font-medium">
                    No results found matching your filters.
                  </td>
                </tr>
              ) : (
                filteredAndSortedRows.map((row, i) => (
                  <tr key={i} className="hover:bg-slate-800/30 transition-colors group">
                    <td className="px-6 py-4">
                      <div className="text-sm font-bold text-white group-hover:text-sky-400 transition-colors">
                        {grain === 'parentOrg' ? getDisplayName(row.name) : row.name}
                      </div>
                    </td>
                    <td className="px-6 py-4 text-sm font-mono text-slate-300">
                      {row.current.toLocaleString()}
                    </td>
                    <td className="px-6 py-4 text-sm font-mono text-slate-500">
                      {row.prior.toLocaleString()}
                    </td>
                    <td className="px-6 py-4">
                      <div className={`flex items-center gap-1.5 text-sm font-mono font-bold ${
                        row.change > 0 ? 'text-emerald-400' : row.change < 0 ? 'text-rose-400' : 'text-slate-500'
                      }`}>
                        {row.change > 0 ? '+' : ''}{row.change.toLocaleString()}
                        {row.change > 0 && <ArrowUpRight className="w-3.5 h-3.5" />}
                        {row.change < 0 && <ArrowDownRight className="w-3.5 h-3.5" />}
                      </div>
                    </td>
                    <td className="px-6 py-4">
                      <div className={`text-xs font-bold px-2 py-1 rounded-md inline-block ${
                        row.percentChange > 0 ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20' : 
                        row.percentChange < 0 ? 'bg-rose-500/10 text-rose-400 border border-rose-500/20' : 
                        'bg-slate-800 text-slate-500'
                      }`}>
                        {row.percentChange > 0 ? '+' : ''}{row.percentChange.toFixed(2)}%
                      </div>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};
